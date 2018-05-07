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
import org.opencypher.memcypher.api.{MemCypherGraph, MemCypherSession, MemRecords}
import org.opencypher.memcypher.impl.{MemPhysicalResult, MemRuntimeContext}
import org.opencypher.okapi.api.graph.{GraphName, QualifiedGraphName}
import org.opencypher.okapi.api.types.CTInteger
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.impl.exception.NotImplementedException
import org.opencypher.okapi.ir.api.Label
import org.opencypher.okapi.ir.api.expr.{Expr, Id, Var}
import org.opencypher.okapi.logical.impl.LogicalPatternGraph
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

sealed trait ConstructedEntity {
  def v: Var
  def baseEntity: Option[Var]
}
case class ConstructedNode(
  v: Var,
  labels: Set[Label],
  baseEntity: Option[Var]) extends ConstructedEntity

case class ConstructedRelationship(
  v: Var,
  source: Var,
  target: Var,
  typ: Option[String],
  baseEntity: Option[Var]) extends ConstructedEntity {
  require(typ.isDefined || baseEntity.isDefined, s"$this: Need to define either the rel type or an equivalence model to construct a relationship")
}

final case class ConstructGraph(left: MemOperator, right: MemOperator, construct: LogicalPatternGraph) extends BinaryOperator {
  val current_max_id = new AtomicLong();
  //toString method from openCypher
  override def toString: String = {
    val entities = construct.clones.keySet ++ construct.newEntities.map(_.v)
    s"ConstructGraph(on=[${construct.onGraphs.mkString(", ")}], entities=[${entities.mkString(", ")}])"
  }

  override def header: RecordHeader = RecordHeader.empty

  override def executeBinary(left: MemPhysicalResult, right: MemPhysicalResult)(implicit context: MemRuntimeContext): MemPhysicalResult = {
    //TODO: plan machen! ; wie komme ich an tabelle ran? (left.records = tabelle (über einzelne embeddings iterieren?))
    implicit val session : MemCypherSession = left.workingGraph.session

    val LogicalPatternGraph(schema, clonedVarsToInputVars, newEntities, sets, _, name) = construct

    if(newEntities.nonEmpty){

    }

    val nodes:Seq[MemNode] = Seq(MemNode(generateID(),Set.empty,CypherMap.empty))
    val rels:Seq[MemRelationship] = Seq()
    val newGraph = MemCypherGraph.create(nodes,rels)
    MemPhysicalResult(MemRecords.unit(), newGraph, session.qgnGenerator.generate)
  }

  //TODO: with grouping make with map (key = variablename++groupedbyvars(List(var,value))
  def generateID(): Long = current_max_id.incrementAndGet();
}
