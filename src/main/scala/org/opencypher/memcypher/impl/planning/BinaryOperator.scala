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
import org.opencypher.memcypher.impl.value.CypherMapOps._
import org.opencypher.memcypher.impl.{MemPhysicalResult, MemRuntimeContext}
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types.{CTInteger, CTString, CTWildcard}
import org.opencypher.okapi.api.value.CypherValue.{CypherBoolean, CypherInteger, CypherList, CypherMap, CypherString, CypherValue}
import org.opencypher.okapi.impl.exception.{IllegalArgumentException, NotImplementedException}
import org.opencypher.okapi.ir.api.Label
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.ir.api.set.SetPropertyItem
import org.opencypher.okapi.logical.impl.{ConstructedEntity, ConstructedNode, ConstructedRelationship, LogicalPatternGraph}
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

sealed trait aggregationExtension {
  def groupBy: Set[Var]

  def aggregatedProperties: Option[Set[SetPropertyItem[Expr]]]
}

class ConstructNodeExtended(v: Var, labels: Set[Label], baseEntity: Option[Var],
                            val groupBy: Set[Var],
                            val aggregatedProperties: Option[Set[SetPropertyItem[Expr]]]) extends ConstructedNode(v, labels, baseEntity) with aggregationExtension

class ConstructedRelationshipExtended(v: Var, baseEntity: Option[Var], source: Var, target: Var, typ: Option[String],
                                      val groupBy: Set[Var],
                                      val aggregatedProperties: Option[Set[SetPropertyItem[Expr]]]) extends ConstructedRelationship(v, source, target, typ, baseEntity) with aggregationExtension


final case class ConstructGraph(left: MemOperator, right: MemOperator, construct: LogicalPatternGraph) extends BinaryOperator {
  val current_max_id = new AtomicLong()
  var storedIDs = Map[String, Long]()

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



    //todo: find expr for matchTable.data.project() (how to use generateID inside of Expr?) .. rename vars; put baseEntity also in groupby.. and than ask cause of project; group by with constant? (maybe if potentialgroupingList is no List)

    val extendedEntities = newEntities.map(entity => {
      val potentialGrouping = sets.filter(x => (x.variable == entity.v) && (x.propertyKey == "groupby"))
      var groupByVarSet = Set[Var]()
      if (potentialGrouping.nonEmpty) {
        val potentialListOfGroupings = RichCypherMap(CypherMap.empty).evaluate(potentialGrouping.head.setValue)(RecordHeader.empty, MemRuntimeContext.empty) //evaluate Expr
        potentialListOfGroupings match {
          case groupByStringList: CypherList => groupByVarSet = groupByStringList.value.map(x => Var(x.toString())()).toSet //todo: could also be a property & check if it exists in matchTable.columns
          case x => throw IllegalArgumentException("wrong value typ for groupBy: should be CypherList but found " + x.getClass.getSimpleName)
        }
      }
      entity match {
        case n: ConstructedNode => new ConstructNodeExtended(n.v, n.labels, n.baseEntity, groupByVarSet, None)
        case r: ConstructedRelationship => {
          groupByVarSet ++= Set(Var(r.source.name)(), Var(r.target.name)()) // relationships implicit grouped by source and target node
          new ConstructedRelationshipExtended(r.v, r.baseEntity, r.source, r.target, r.typ, groupByVarSet, None)
        }
      }
    }
    )

    //todo: let ExpressionSeq generate before and save in entity.groupby (alter .groupby to Set[Expr])
    val entityExpressionsSeq = extendedEntities.map(entity => StringLit(entity.v.name)() +: entity.groupBy.foldLeft(IndexedSeq[Expr]())((z, groupVar) => z :+ StringLit(groupVar.name)() :+ Id(groupVar)())) //generate Expr for every ConstructedEntityExtended
    //generate expr ... groupby parameter for generateID
    val matchTableProjectGroupBy = entityExpressionsSeq.map(seq => {
      val expr = ListLit(seq)()
      matchTable.data.project(expr, seq.head.withoutType)(matchTable.header, context).select(Set(seq.head.withoutType, "n"))(matchTable.header, context)
    })


    MemPhysicalResult(MemRecords.unit(), MemCypherGraph.empty, name)
  }

  //maybe groupby parameter only Seq[Long] (only one groupby per constructedEntity possible)
  def generateID(constructedEntityName: String, groupby: Seq[(String, Long)] = Seq()): Long = {
    var key = constructedEntityName

    if (groupby.nonEmpty) {
      val sorted_groupby = groupby.sortWith((x, y) => x._1 < y._1); // groupby a,b should be equal to groupby b,a
      key += sorted_groupby.foldLeft("")((x, tupel) => x + "|" + tupel._1 + tupel._2); //generate with groupby
      if (storedIDs.contains(key)) storedIDs.getOrElse(key, -1)
      else {
        storedIDs += key -> current_max_id.incrementAndGet()
        current_max_id.get()
      }
    }
    else {
      val newID = current_max_id.incrementAndGet()
      storedIDs += key + newID -> newID //everytime id fct for ungrouped var gets called --> new ID
      newID
    }
  }
}
