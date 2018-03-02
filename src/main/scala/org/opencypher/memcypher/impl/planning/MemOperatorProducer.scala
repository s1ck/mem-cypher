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

import org.opencypher.okapi.api.graph.{PropertyGraph, QualifiedGraphName}
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.ir.api.block.SortItem
import org.opencypher.okapi.ir.api.expr
import org.opencypher.okapi.ir.api.expr.{Aggregator, Expr, Var}
import org.opencypher.okapi.logical.impl.{ConstructedEntity, Direction, LogicalExternalGraph, LogicalGraph}
import org.opencypher.okapi.relational.api.physical.{PhysicalOperatorProducer, PhysicalPlannerContext}
import org.opencypher.okapi.relational.impl.table.{ProjectedExpr, ProjectedField, RecordHeader}
import org.opencypher.memcypher.api.{MemCypherGraph, MemCypherSession, MemRecords}
import org.opencypher.memcypher.impl.MemRuntimeContext

case class MemPhysicalPlannerContext(
  session: MemCypherSession,
  resolver: QualifiedGraphName => PropertyGraph,
  inputRecords: MemRecords,
  parameters: CypherMap) extends PhysicalPlannerContext[MemRecords]

object MemPhysicalPlannerContext {
  def from(
    resolver: QualifiedGraphName => PropertyGraph,
    inputRecords: MemRecords,
    parameters: CypherMap)(implicit session: MemCypherSession): PhysicalPlannerContext[MemRecords] = {
    MemPhysicalPlannerContext(session, resolver, inputRecords, parameters)
  }
}

class MemOperatorProducer(implicit caps: MemCypherSession)
  extends PhysicalOperatorProducer[MemOperator, MemRecords, MemCypherGraph, MemRuntimeContext] {

  /**
    * Starts the query execution based on the given records and an external graph.
    *
    * @param in backend-specific records
    * @param g  external (URI) reference to the input graph (e.g. the session graph)
    * @return start operator
    */
  override def planStart(in: MemRecords, g: LogicalExternalGraph): MemOperator = Start(in, g)

  /**
    * Starts the query execution based on empty records and an external graph.
    *
    * @param graph external (URI) reference to the input graph (e.g. the session graph)
    * @return start from unit operator
    */
  override def planStartFromUnit(graph: LogicalExternalGraph): MemOperator = ???

  /**
    * Sets the source graph for the next query operation.
    *
    * @param in previous operator
    * @param g  external (URI) reference to a graph on which the query is continued
    * @return set source graph operator
    */
  override def planSetSourceGraph(in: MemOperator, g: LogicalExternalGraph): MemOperator =
    SetSourceGraph(in, g)

  /**
    * Scans the node set of the input graph and returns all nodes that match the given CTNode type.
    *
    * @param in      previous operator
    * @param inGraph graph to scan nodes from
    * @param v       node variable carrying the node type to scan for
    * @param header  resulting record header
    * @return node scan operator
    */
  override def planNodeScan(in: MemOperator, inGraph: LogicalGraph, v: Var, header: RecordHeader): MemOperator =
    Scan(in, inGraph, v, header)

  /**
    * Scans the relationship set of the input graph and returns all relationships that match the given CTRelationship
    * type.
    *
    * @param in      previous operator
    * @param inGraph graph to scan relationships from
    * @param v       node variable carrying the relationship type to scan for
    * @param header  resulting record header
    * @return relationship scan operator
    */
  override def planRelationshipScan(in: MemOperator, inGraph: LogicalGraph, v: Var, header: RecordHeader): MemOperator =
    Scan(in, inGraph, v, header)

  /**
    * Creates an empty record set thereby disregarding the input. The records are described by the given record header.
    *
    * @param in     previous operator
    * @param header record header describing the created records
    * @return empty records operator
    */
  override def planEmptyRecords(in: MemOperator, header: RecordHeader): MemOperator = ???

  /**
    * Renames the column identified by the given expression to the specified alias.
    *
    * @param in     previous operator
    * @param expr   expression to be aliased
    * @param alias  alias
    * @param header resulting record header
    * @return empty records operator
    */
  override def planAlias(in: MemOperator, expr: Expr, alias: Var, header: RecordHeader): MemOperator = ???

  /**
    * The operator takes a set of (field, expression) aliases and renames the columns identified by a field to the
    * corresponding expression.
    *
    * @param in      previous operator
    * @param aliases set of aliases
    * @param header  resulting record header
    * @return remove aliases operator
    */
  override def planRemoveAliases(in: MemOperator, aliases: Set[(ProjectedField, ProjectedExpr)], header: RecordHeader): MemOperator = ???

  /**
    * Filters the incoming rows according to the specified expression.
    *
    * @param in     previous operator
    * @param expr   expression to be evaluated
    * @param header resulting record header
    * @return filter operator
    */
  override def planFilter(in: MemOperator, expr: Expr, header: RecordHeader): MemOperator =
    Filter(in, expr, header)

  /**
    * Selects the specified fields from the given records.
    *
    * @param in     previous operator
    * @param fields fields to select from the records (i.e., as specified in the RETURN clause)
    * @param header resulting record header
    * @return select fields operator
    */
  override def planSelectFields(in: MemOperator, fields: IndexedSeq[Var], header: RecordHeader): MemOperator =
    Select(in, fields, Set.empty, header)

  /**
    * Selects the specified graph from the input operator.
    *
    * @param in     previous operator
    * @param graphs graphs to select from the previous operator (i.e., as specified in the RETURN clause)
    * @return select graphs operator
    */
  override def planSelectGraphs(in: MemOperator, graphs: Set[String]): MemOperator =
    Select(in, Seq.empty, graphs, in.header)

  /**
    * Evaluates the given expression and projects it to a new column in the input records.
    *
    * @param in     previous operator
    * @param expr   expression to evaluate
    * @param header resulting record header
    * @return project operator
    */
  override def planProject(in: MemOperator, expr: Expr, header: RecordHeader): MemOperator =
    Project(in, expr, header)

  /**
    * Stores the graph identified by the given URI by the given name.
    *
    * @param in                 previous operator
    * @param name               name to project graph to
    * @param qualifiedGraphName reference to a graph (e.g. an external graph)
    * @return project external graph operator
    */
  override def planProjectExternalGraph(in: MemOperator, name: String, qualifiedGraphName: QualifiedGraphName): MemOperator = ???

  /**
    * Creates a new record containing the specified entities (i.e. as defined in a construction pattern).
    *
    * @param in       previous operator
    * @param toCreate entities to create
    * @param name     name of the resulting graph
    * @param schema   schema of the resulting graph
    * @param header   resulting record header
    * @return project pattern graph operator
    */
  override def planProjectPatternGraph(in: MemOperator, toCreate: Set[ConstructedEntity], name: String, schema: Schema, header: RecordHeader): MemOperator = ???

  /**
    * Groups the underlying records by the specified expressions and evaluates the given aggregate functions.
    *
    * @param in           previous operator
    * @param group        vars to group records by
    * @param aggregations aggregate functions
    * @param header       resulting record header
    * @return aggregate operator
    */
  override def planAggregate(in: MemOperator, group: Set[Var], aggregations: Set[(Var, Aggregator)], header: RecordHeader): MemOperator = ???

  /**
    * Performs a distinct operation on the specified fields.
    *
    * @param in     previous operator
    * @param fields fields to compute distinct on
    * @return distinct operator
    */
  override def planDistinct(in: MemOperator, fields: Set[Var]): MemOperator = ???

  /**
    * Orders the underlying records by the given expressions.
    *
    * @param in        previous operator
    * @param sortItems fields to order records by
    * @param header    resulting record header
    * @return order by operator
    */
  override def planOrderBy(in: MemOperator, sortItems: Seq[SortItem[Expr]], header: RecordHeader): MemOperator = ???

  /**
    * Unwinds the given list of items into the specified var for each row in the input records.
    *
    * @param in     previous operator
    * @param list   list of items to unwind
    * @param item   var to project item to
    * @param header resulting record header
    * @return unwind operator
    */
  override def planUnwind(in: MemOperator, list: Expr, item: Var, header: RecordHeader): MemOperator = ???

  /**
    * Initializes the underlying records for a variable expand computation (e.g., (a)-[:A*1..3]->(b)).
    *
    * @param in       previous operator
    * @param source   variable to expand from (e.g. (a))
    * @param edgeList variable to identify column which later stores relationship identifiers of the computed paths
    * @param target   variable to expand into (e.g. (b))
    * @param header   resulting record header
    * @return init var expand operator
    */
  override def planInitVarExpand(in: MemOperator, source: Var, edgeList: Var, target: Var, header: RecordHeader): MemOperator = ???

  /**
    * Skips the given amount of rows in the input records. The number of rows is specified by an expression which can be
    * a literal or a query parameter.
    *
    * @param in     previous operator
    * @param expr   expression which contains or refers to the number of rows to skip
    * @param header resulting record header
    * @return skip operator
    */
  override def planSkip(in: MemOperator, expr: Expr, header: RecordHeader): MemOperator = ???

  /**
    * Limits the number of input records to the specified amount. The number of rows is specified by an expression which
    * can be a literal or a query parameter.
    *
    * @param in     previous operator
    * @param expr   expression which contains or refers to the maximum number of rows to return
    * @param header resulting record header
    * @return limit operator
    */
  override def planLimit(in: MemOperator, expr: Expr, header: RecordHeader): MemOperator = ???

  /**
    * Computes a cartesian product between the two input records.
    *
    * @param lhs    first previous operator
    * @param rhs    second previous operator
    * @param header resulting record header
    * @return cross operator
    */
  override def planCartesianProduct(lhs: MemOperator, rhs: MemOperator, header: RecordHeader): MemOperator = ???

  /**
    * Joins the two input records on node attribute values.
    *
    * @param lhs        first previous operator
    * @param rhs        second previous operator
    * @param predicates join predicates
    * @param header     resulting record header
    * @return value join operator
    */
  override def planValueJoin(lhs: MemOperator, rhs: MemOperator, predicates: Set[expr.Equals], header: RecordHeader): MemOperator = ???

  /**
    * Unions the input records.
    *
    * @param lhs first previous operator
    * @param rhs second previous operator
    * @return union operator
    */
  override def planUnion(lhs: MemOperator, rhs: MemOperator): MemOperator = ???

  /**
    * Joins the two input records on two columns, where `source` is solved in the first operator and `target` is solved
    * in the second operator.
    *
    * @param lhs    first previous operator
    * @param rhs    second previous operator
    * @param source variable solved by the first operator
    * @param rel    relationship variable
    * @param target variable solved by the second operator
    * @param header resulting record header
    * @return expand into operator
    */
  override def planExpandInto(lhs: MemOperator, rhs: MemOperator, source: Var, rel: Var, target: Var, header: RecordHeader): MemOperator = ???

  /**
    * Computes the result of an OPTIONAL MATCH where the first input is the non-optional part and the second input the
    * optional one.
    *
    * @param lhs    first previous operator
    * @param rhs    second previous operator
    * @param header resulting record header
    * @return optional operator
    */
  override def planOptional(lhs: MemOperator, rhs: MemOperator, header: RecordHeader): MemOperator = ???

  /**
    * Filters the rows of the first input by checking if there exists a corresponding row in the second input.
    *
    * @param lhs         first previous operator
    * @param rhs         second previous operator
    * @param targetField field that stores the (boolean) result of the evaluation
    * @param header      resulting record header
    * @return exists subquery operator
    */
  override def planExistsSubQuery(lhs: MemOperator, rhs: MemOperator, targetField: Var, header: RecordHeader): MemOperator = ???

  /**
    * Expands the records in the first input (nodes) via the records in the second input (relationships) into the
    * records in the third input (nodes).
    *
    * @param first                   first previous operator
    * @param second                  second previous operator
    * @param third                   third previous operator
    * @param source                  node variable in the first input
    * @param rel                     relationship variable in the second input
    * @param target                  node variable in the third input
    * @param header                  resulting record header
    * @param removeSelfRelationships set true, iff loops shall be removed from the ouput
    * @return expand source operator
    */
  override def planExpandSource(first: MemOperator, second: MemOperator, third: MemOperator, source: Var, rel: Var, target: Var, header: RecordHeader, removeSelfRelationships: Boolean): MemOperator = ???

  /**
    * Performs a bounded variable length path expression.
    *
    * @param first          first previous operator
    * @param second         second previous operator
    * @param third          third previous operator
    * @param rel            relationship variable to expand from
    * @param edgeList       refers to the column in which the path is stored
    * @param target         node variable in the third input
    * @param initialEndNode initial end node
    * @param lower          lower bound
    * @param upper          upper bound
    * @param direction      path direction
    * @param header         resulting record header
    * @param isExpandInto   true, iff the target variable is solved and can be replaced by a filter
    * @return bounded var expand operator
    */
  override def planBoundedVarExpand(first: MemOperator, second: MemOperator, third: MemOperator, rel: Var, edgeList: Var, target: Var, initialEndNode: Var, lower: Int, upper: Int, direction: Direction, header: RecordHeader, isExpandInto: Boolean): MemOperator = ???
}
