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

import org.opencypher.memcypher.api.{MemCypherGraph, MemCypherSession, MemRecords}
import org.opencypher.memcypher.impl.MemRuntimeContext
import org.opencypher.okapi.api.graph.QualifiedGraphName
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.ir.api.block.SortItem
import org.opencypher.okapi.ir.api.expr.{Aggregator, Expr, Var}
import org.opencypher.okapi.ir.impl.QueryCatalog
import org.opencypher.okapi.logical.impl.{Direction, LogicalCatalogGraph, LogicalGraph, LogicalPatternGraph}
import org.opencypher.okapi.relational.api.physical.{PhysicalOperatorProducer, PhysicalPlannerContext}
import org.opencypher.okapi.relational.impl.physical.JoinType
import org.opencypher.okapi.relational.impl.table.{ProjectedExpr, ProjectedField, RecordHeader}

case class MemPhysicalPlannerContext(
  session: MemCypherSession,
  catalog: QueryCatalog,
  inputRecords: MemRecords,
  parameters: CypherMap,
  constructedGraphPlans: collection.mutable.Map[QualifiedGraphName, MemOperator]) extends PhysicalPlannerContext[MemOperator, MemRecords]

object MemPhysicalPlannerContext {
  def from(
    catalog: QueryCatalog,
    inputRecords: MemRecords,
    parameters: CypherMap)(implicit session: MemCypherSession): PhysicalPlannerContext[MemOperator, MemRecords] = {
    MemPhysicalPlannerContext(session, catalog, inputRecords, parameters, collection.mutable.Map.empty)
  }
}

class MemOperatorProducer(implicit memCypher: MemCypherSession)
  extends PhysicalOperatorProducer[MemOperator, MemRecords, MemCypherGraph, MemRuntimeContext] {

  override def planStart(
    qgnOpt: Option[QualifiedGraphName] = None,
    in: Option[MemRecords] = None): MemOperator = Start(qgnOpt.getOrElse(memCypher.emptyGraphQgn), in)

  override def planNodeScan(
    in: MemOperator,
    inGraph: LogicalGraph,
    v: Var,
    header: RecordHeader): MemOperator = Scan(in, v, header)

  override def planRelationshipScan(
    in: MemOperator,
    inGraph: LogicalGraph,
    v: Var,
    header: RecordHeader): MemOperator = Scan(in, v, header)

  override def planSelect(
    in: MemOperator,
    expressions: List[Expr],
    header: RecordHeader): MemOperator = Select(in, expressions, header)

  override def planAlias(
    in: MemOperator,
    tuples: Seq[(Expr, Var)],
    header: RecordHeader): MemOperator = Alias(in, tuples, header)

  override def planFilter(
    in: MemOperator,
    expr: Expr,
    header: RecordHeader): MemOperator = Filter(in, expr, header)

  override def planProject(
    in: MemOperator,
    expr: Expr,
    header: RecordHeader): MemOperator = Project(in, expr, header)

  override def planJoin(
    lhs: MemOperator,
    rhs: MemOperator,
    joinColumns: Seq[(Expr, Expr)],
    header: RecordHeader,
    joinType: JoinType): MemOperator = Join(lhs, rhs, joinColumns, header, joinType)

  override def planDistinct(
    in: MemOperator,
    fields: Set[Var]): MemOperator = Distinct(in,fields)

  override def planAggregate(
    in: MemOperator,
    group: Set[Var],
    aggregations: Set[(Var, Aggregator)],
    header: RecordHeader): MemOperator = Aggregate(in, group, aggregations, header)

  override def planDrop(
    in: MemOperator,
    dropFields: Seq[Expr],
    header: RecordHeader): MemOperator = Drop(in, dropFields, header)

  override def planOrderBy(
    in: MemOperator,
    sortItems: Seq[SortItem[Expr]],
    header: RecordHeader): MemOperator = OrderBy(in, sortItems)

  override def planRemoveAliases(
    in: MemOperator,
    aliases: Set[(ProjectedField, ProjectedExpr)],
    header: RecordHeader): MemOperator = RemoveAliases(in, aliases, header)

  override def planCartesianProduct(
    lhs: MemOperator,
    rhs: MemOperator,
    header: RecordHeader): MemOperator = CartesianProduct(lhs, rhs, header)

  override def planTabularUnionAll(
    lhs: MemOperator,
    rhs: MemOperator): MemOperator = TabularUnionAll(lhs, rhs)

  override def planEmptyRecords(in: MemOperator, header: RecordHeader): MemOperator = ???

  override def planInitVarExpand(in: MemOperator, source: Var, edgeList: Var, target: Var, header: RecordHeader): MemOperator = ???

  override def planSkip(in: MemOperator, expr: Expr, header: RecordHeader): MemOperator = ???

  override def planLimit(in: MemOperator, expr: Expr, header: RecordHeader): MemOperator = ???

  override def planExistsSubQuery(lhs: MemOperator, rhs: MemOperator, targetField: Var, header: RecordHeader): MemOperator = ???

  override def planBoundedVarExpand(first: MemOperator, second: MemOperator, third: MemOperator, rel: Var, edgeList: Var, target: Var, initialEndNode: Var, lower: Int, upper: Int, direction: Direction, header: RecordHeader, isExpandInto: Boolean): MemOperator = ???

  override def planReturnGraph(in: MemOperator): MemOperator = ???

  override def planFromGraph(in: MemOperator, graph: LogicalCatalogGraph): MemOperator = ???

  override def planConstructGraph(table: MemOperator, onGraph: MemOperator, construct: LogicalPatternGraph): MemOperator = ???

  override def planGraphUnionAll(graphs: List[MemOperator], qgn: QualifiedGraphName): MemOperator = ???

}
