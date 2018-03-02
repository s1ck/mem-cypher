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
package org.opencypher.memcypher.api

import java.util.UUID

import org.opencypher.okapi.api.graph._
import org.opencypher.okapi.api.table.CypherRecords
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.impl.io.SessionPropertyGraphDataSource
import org.opencypher.okapi.impl.util.Measurement.time
import org.opencypher.okapi.ir.api.IRExternalGraph
import org.opencypher.okapi.ir.impl.parse.CypherParser
import org.opencypher.okapi.ir.impl.{IRBuilder, IRBuilderContext}
import org.opencypher.okapi.logical.api.configuration.LogicalConfiguration.PrintLogicalPlan
import org.opencypher.okapi.logical.impl.{LogicalOperatorProducer, LogicalOptimizer, LogicalPlanner, LogicalPlannerContext}
import org.opencypher.okapi.relational.api.configuration.CoraConfiguration.PrintFlatPlan
import org.opencypher.okapi.relational.impl.flat.{FlatPlanner, FlatPlannerContext}
import org.opencypher.okapi.relational.impl.physical.PhysicalPlanner
import org.opencypher.memcypher.api.MemCypherConverters._
import org.opencypher.memcypher.impl.planning.{MemOperatorProducer, MemPhysicalPlannerContext}
import org.opencypher.memcypher.impl.MemRuntimeContext

object MemCypherSession {
  def create: MemCypherSession = new MemCypherSession(SessionPropertyGraphDataSource.Namespace)
}

class MemCypherSession(override val sessionNamespace: Namespace) extends CypherSession {

  self =>

  private val producer = new LogicalOperatorProducer
  private val logicalPlanner = new LogicalPlanner(producer)
  private val logicalOptimizer = LogicalOptimizer
  private val flatPlanner = new FlatPlanner()
  private val physicalPlanner = new PhysicalPlanner(new MemOperatorProducer()(this))
  private val parser = CypherParser

  /**
    * Executes a Cypher query in this session on the current ambient graph.
    *
    * @param query      Cypher query to execute
    * @param parameters parameters used by the Cypher query
    * @return result of the query
    */
  override def cypher(query: String, parameters: CypherMap = CypherMap.empty, drivingTable: Option[CypherRecords] = None): CypherResult =
    cypherOnGraph(MemCypherGraph.empty(this), query, parameters, drivingTable)

  override def cypherOnGraph(graph: PropertyGraph, query: String, parameters: CypherMap, drivingTable: Option[CypherRecords]): CypherResult = {
    val ambientGraph = mountAmbientGraph(graph)

    val (stmt, extractedLiterals, semState) = time("AST construction")(parser.process(query)(CypherParser.defaultContext))

    val extractedParameters = extractedLiterals.mapValues(v => CypherValue(v))
    val allParameters = parameters ++ extractedParameters

    val ir = time("IR translation")(IRBuilder(stmt)(IRBuilderContext.initial(query, allParameters, semState, ambientGraph, dataSource)))

    val logicalPlannerContext = LogicalPlannerContext(graph.schema, Set.empty, ir.model.graphs.mapValues(_.namespace).andThen(dataSource), ambientGraph)
    val logicalPlan = time("Logical planning")(logicalPlanner(ir)(logicalPlannerContext))
    val optimizedLogicalPlan = time("Logical optimization")(logicalOptimizer(logicalPlan)(logicalPlannerContext))
    if (PrintLogicalPlan.isSet) {
      println(logicalPlan.pretty)
      println(optimizedLogicalPlan.pretty)
    }

    val flatPlan = time("Flat planning")(flatPlanner(optimizedLogicalPlan)(FlatPlannerContext(parameters)))
    if (PrintFlatPlan.isSet) println(flatPlan.pretty)

    val memPlannerContext = MemPhysicalPlannerContext(this, super.graph, MemRecords.unit()(self), allParameters)
    val memPlan = time("Physical planning")(physicalPlanner.process(flatPlan)(memPlannerContext))

    time("Query execution")(MemCypherResultBuilder.from(logicalPlan, flatPlan, memPlan)(MemRuntimeContext(memPlannerContext.parameters, graphAt)))
  }

  private def graphAt(qualifiedGraphName: QualifiedGraphName): Option[MemCypherGraph] =
    Some(dataSource(qualifiedGraphName.namespace).graph(qualifiedGraphName.graphName).asMemCypher)

  private def mountAmbientGraph(ambient: PropertyGraph): IRExternalGraph = {
    val graphName = GraphName(UUID.randomUUID().toString)
    val qualifiedGraphName = store(graphName, ambient)
    IRExternalGraph(graphName.value, ambient.schema, qualifiedGraphName)
  }
}
