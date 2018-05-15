/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.opencypher.memcypher.impl.planning

import java.util.concurrent.atomic.AtomicLong

import org.opencypher.memcypher.api.value.{MemNode, MemRelationship}
import org.opencypher.memcypher.api.{Embeddings, MemCypherGraph, MemCypherSession, MemRecords}
import org.opencypher.memcypher.impl.{MemPhysicalResult, MemRuntimeContext}
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types.{CTBoolean, CTInteger}
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.api.value.CypherValue.{CypherBoolean, CypherInteger, CypherMap, CypherValue}
import org.opencypher.okapi.impl.exception.NotImplementedException
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.ir.api.set.SetPropertyItem
import org.opencypher.okapi.logical.impl.{ConstructedEntity, ConstructedNode, LogicalPatternGraph}
import org.opencypher.okapi.relational.impl.physical.{InnerJoin, JoinType, LeftOuterJoin, RightOuterJoin}
import org.opencypher.okapi.relational.impl.table.RecordHeader

private[memcypher] abstract class BinaryOperator extends MemOperator {

  def left: MemOperator

  def right: MemOperator

  override def execute(implicit context: MemRuntimeContext): MemPhysicalResult =
    executeBinary(left.execute, right.execute)

  def executeBinary(left: MemPhysicalResult, right: MemPhysicalResult)(implicit context: MemRuntimeContext): MemPhysicalResult
}

final case class Join(
                       left: MemOperator,
                       right: MemOperator,
                       joinExprs: Seq[(Expr, Expr)],
                       header: RecordHeader,
                       joinType: JoinType) extends BinaryOperator {

  override def executeBinary(left: MemPhysicalResult, right: MemPhysicalResult)(implicit context: MemRuntimeContext): MemPhysicalResult = {
    if (joinExprs.length > 1) throw NotImplementedException("Multi-way join support")

    val (leftExpr, rightExpr) = joinExprs.head
    val leftId = leftExpr match {
      case v: Var => Id(v)(CTInteger)
      case other => other
    }
    val rightId = rightExpr match {
      case v: Var => Id(v)(CTInteger)
      case other => other
    }

    val newData = joinType match {
      case InnerJoin => left.records.data.innerJoin(right.records.data, leftId, rightId)(header, context)
      case RightOuterJoin => left.records.data.rightOuterJoin(right.records.data, leftId, rightId)(header, context)
      case LeftOuterJoin => right.records.data.rightOuterJoin(left.records.data, rightId, leftId)(header, context)
      case other => throw NotImplementedException(s"Unsupported JoinType: $other")
    }

    MemPhysicalResult(MemRecords(newData, header), left.workingGraph, left.workingGraphName)
  }

}

final case class CartesianProduct(left: MemOperator, right: MemOperator, header: RecordHeader) extends BinaryOperator {
  override def executeBinary(left: MemPhysicalResult, right: MemPhysicalResult)(implicit context: MemRuntimeContext): MemPhysicalResult = {
    val newData = left.records.data.cartesianProduct(right.records.data)
    MemPhysicalResult(MemRecords(newData, header), left.workingGraph, left.workingGraphName)
  }
}

final case class TabularUnionAll(left: MemOperator, right: MemOperator) extends BinaryOperator with InheritedHeader {
  override def executeBinary(left: MemPhysicalResult, right: MemPhysicalResult)(implicit context: MemRuntimeContext): MemPhysicalResult = {
    val newData = left.records.data.unionAll(right.records.data)
    MemPhysicalResult(MemRecords(newData, header), left.workingGraph, left.workingGraphName)
  }
}

final case class ConstructGraph(left: MemOperator, right: MemOperator, construct: LogicalPatternGraph) extends BinaryOperator {
  val current_max_id = new AtomicLong()

  //toString method from openCypher
  override def toString: String = {
    val entities = construct.clones.keySet ++ construct.newEntities.map(_.v)
    s"ConstructGraph(on=[${construct.onGraphs.mkString(", ")}], entities=[${entities.mkString(", ")}])"
  }

  override def header: RecordHeader = RecordHeader.empty

  override def executeBinary(left: MemPhysicalResult, right: MemPhysicalResult)(implicit context: MemRuntimeContext): MemPhysicalResult = {

    implicit val session: MemCypherSession = left.workingGraph.session
    val matchTable = left.records
    val LogicalPatternGraph(schema, clonedVarsToInputVars, newEntities, sets, _, name) = construct


    val baseTable = left.records; //todo: add allias (use .data.withColumnsRenamed )
    val entityTable = createEntities(newEntities,schema)
    val entityTableWithProperties = sets.foldLeft(entityTable) {
      case (table, SetPropertyItem(key, v, expr)) => constructProperty(v, key, expr, table)(table.header,context)
    }

    //Todo: MemRecords to MemGraph

    //straight-forward version (newEntities+their labels & properties)(without use of MemRecords)
    val nodes = newEntities.foldLeft(Seq.empty[MemNode]) {
      (list, b) =>
        b match {
          case x: ConstructedNode =>
            val labels = x.labels.map(_.name.toString)
            val properties = sets.filter(_.variable == x.v).foldLeft(CypherMap.empty) {
              (map, setItem) => map.updated(setItem.propertyKey, exprToCypherValue(setItem.setValue))
            }
            list :+ MemNode(generateID(), labels, properties)
          case _ => list //println("not implemented for" + z.getClass)
        }
    }


    val newGraph = MemCypherGraph.create(nodes, Seq[MemRelationship]())
    MemPhysicalResult(MemRecords.unit(), newGraph, session.qgnGenerator.generate)
  }

  // with grouping make with map (key = variablename|groupedbyvars(List(var,value))
  def generateID(): Long = current_max_id.incrementAndGet()

  def createEntities(newEntities: Set[ConstructedEntity],schema: Schema): MemRecords = {

    val nodes = newEntities.collect {
      case c@ConstructedNode(Var(name), _, _) => c //
    }
    var header = RecordHeader.empty
    val nodeData = nodes.foldLeft(Seq[CypherMap]()) {
      (z, node) => {
        header ++= RecordHeader.nodeFromSchema(node.v,schema)
        z :+ CypherMap(node.labels.map(label => node.v.name +":" + label.name -> CypherBoolean(true)).toMap).updated(node.v.name, generateID())
        //problem: both freshvars in Recordheader --> both vars must have an entry in one Cyphermap?
      }
    }
    MemRecords.create(Embeddings(nodeData), header)
  }

  def constructProperty(variable: Var, propertyKey: String, propertyValue: Expr, constructedTable: MemRecords)(implicit header: RecordHeader,context: MemRuntimeContext): MemRecords = {
    val dataToUpdate = constructedTable.data.filter(IsNotNull(variable)(CTInteger)) //todo: get filter to work (should return CypherMaps with variable as a key in it)
    val dataWithProperty = dataToUpdate.data.map(_.updated(variable.name+"."+propertyKey,exprToCypherValue(propertyValue)))

    MemRecords.create(dataWithProperty, constructedTable.header)
  }

  //helperMethod -- better Way?
  def exprToCypherValue(expr: Expr): CypherValue = expr match {
    case i: IntegerLit => i.v
    case s: StringLit => s.v
    case l: ListLit => CypherValue.apply(l.v.toList.map(exprToCypherValue(_)))
    case b: BoolLit => b.v
    case _ => "notImplementedType"
  }
}
