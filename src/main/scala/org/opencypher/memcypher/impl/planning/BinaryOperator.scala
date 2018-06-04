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
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.api.value.CypherValue.{CypherBoolean, CypherInteger, CypherList, CypherMap, CypherString}
import org.opencypher.okapi.impl.exception.{IllegalArgumentException, NotImplementedException}
import org.opencypher.okapi.ir.api.{Label, PropertyKey}
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
  def groupBy: Set[Expr]

  def aggregatedProperties: Option[Set[SetPropertyItem[Expr]]]
}

class ConstructedNodeExtended(v: Var, labels: Set[Label], baseEntity: Option[Var],
                              val groupBy: Set[Expr],
                              val aggregatedProperties: Option[Set[SetPropertyItem[Expr]]]) extends ConstructedNode(v, labels, baseEntity) with aggregationExtension

class ConstructedRelationshipExtended(v: Var, baseEntity: Option[Var], source: Var, target: Var, typ: Option[String],
                                      val groupBy: Set[Expr],
                                      val aggregatedProperties: Option[Set[SetPropertyItem[Expr]]]) extends ConstructedRelationship(v, source, target, typ, baseEntity) with aggregationExtension


final case class ConstructGraph(left: MemOperator, right: MemOperator, construct: LogicalPatternGraph) extends BinaryOperator {

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


    val extendedEntities = newEntities.map(x => createExtendedEntity(x, sets, matchTable))
    val extendedNodes = extendedEntities.filter(_ match { case _: ConstructedNodeExtended => true case _ => false })
    val extendedRelationships = extendedEntities.filter(_ match { case _: ConstructedRelationshipExtended => true case _ => false })
    var extendedMatchTable = matchTable
    extendedNodes.foreach(entity => extendedMatchTable = extendMatchTable(entity, extendedMatchTable, context))
    extendedRelationships.foreach(entity => extendMatchTable(entity, extendedMatchTable, context)) //only relationship id generated for now

    val compact_result = extendedMatchTable.data.select(Set("m", "m.gender:STRING", "n", "copiedNode", "ungroupedNode", "propertygroupedNode", "constantgroup"))(matchTable.header, context) //selected only important rows for example in demo

    val remaining_sets = sets.filter(_.propertyKey != "groupby") //groupby got evaluated already , todo: evaluate via project op
    //todo: from MemRecords to MemGraph
    MemPhysicalResult(MemRecords.unit(), MemCypherGraph.empty, name)
  }

  def extendMatchTable(entity: ConstructedEntity with aggregationExtension, matchTable: MemRecords, context: MemRuntimeContext): MemRecords = {
    val newData = entity.aggregatedProperties match {
      case None => entity match {
        case node: ConstructedNode => {
          matchTable.data.project(Id(ListLit(StringLit(entity.v.name)() +: entity.groupBy.toIndexedSeq)())(), entity.v.name)(matchTable.header, context)
        }
        //case relationship: ConstructedRelationship => matchTable.data //todo: for constructedRelationship also project of sourceNode & targetNode (groupby already implemented)
      }
      case Some(setAggProp) => ???
    }

    MemRecords(newData, header)
  }

  def createExtendedEntity(newEntity: ConstructedEntity, sets: List[SetPropertyItem[Expr]], matchTable: MemRecords): ConstructedEntity with aggregationExtension = {
    val potentialGrouping = sets.filter(x => (x.variable == newEntity.v) && (x.propertyKey == "groupby"))
    var groupByVarSet = newEntity.baseEntity match {
      case Some(v) => Set[Expr](v)
      case None => Set[Expr]()
    }
    if (potentialGrouping.nonEmpty) {
      val potentialListOfGroupings = RichCypherMap(CypherMap.empty).evaluate(potentialGrouping.head.setValue)(RecordHeader.empty, MemRuntimeContext.empty) //evaluate Expr , maybe sort this list?
      potentialListOfGroupings match {
        case groupByStringList: CypherList => groupByVarSet ++= createGroupByExprSet(groupByStringList, matchTable.columnType)
        case _: CypherString | _: CypherInteger | _: CypherBoolean => groupByVarSet += StringLit("constant")(CTString) //groupby constant --> only one entity created
        case x => throw IllegalArgumentException("wrong value typ for groupBy: should be CypherList but found " + x.getClass.getSimpleName) //todo: enable group by constant here
      }
    }
    newEntity match {
      case n: ConstructedNode => new ConstructedNodeExtended(n.v, n.labels, n.baseEntity, groupByVarSet, None)
      case r: ConstructedRelationship => {
        groupByVarSet ++= Set(Id(Var(r.source.name)())(), Id(Var(r.target.name)())()) // relationships implicit grouped by source and target node
        new ConstructedRelationshipExtended(r.v, r.baseEntity, r.source, r.target, r.typ, groupByVarSet, None)
      }
    }
  }

  def createGroupByExprSet(list: CypherList, validColumnns: Map[String, CypherType]): Set[Expr] = {
    list.value.map(x => {
      val stringValue = x.toString()
      if (stringValue.contains('.')) {
        val tmp = stringValue.split('.')
        val neededKey = validColumnns.keySet.filter(_.matches(stringValue + ":.++"))
        if (neededKey.isEmpty) throw new IllegalArgumentException("valid property for groupby", "invalid groupby property " + stringValue)
        val propertyCypherType = validColumnns.getOrElse(neededKey.head, CTWildcard)
        Property(Var(tmp(0))(), PropertyKey(tmp(1)))(propertyCypherType)
      }
      else if (validColumnns.keySet.contains(x.toString()))
        Id(Var(stringValue)())()
      else throw IllegalArgumentException("valid variable for groupby", "invalid groupby variable " + stringValue)
    }).toSet
  }
}

object generateID {
  private val current_max_id = new AtomicLong()
  private var storedIDs = Map[String, Long]()

  def generateID(constructedEntityName: String, groupby: List[String] = List()): Long = {
    var key = constructedEntityName

    if (groupby.nonEmpty) {
      key += groupby.foldLeft("")((x, based_value) => x + "|" + based_value); //generate with groupby
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

  def clearIdMap() = storedIDs.empty //needed, so that next query works on empty storedIDs Map?
}
