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

import java.util.concurrent.atomic.AtomicLong

import org.opencypher.memcypher.api.MemCypherConverters._
import org.opencypher.memcypher.impl.MemRuntimeContext
import org.opencypher.memcypher.impl.planning.{MemOperatorProducer, MemPhysicalPlannerContext}
import org.opencypher.okapi.api.graph._
import org.opencypher.okapi.api.io.PropertyGraphDataSource
import org.opencypher.okapi.api.table.CypherRecords
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.impl.exception.NotImplementedException
import org.opencypher.okapi.impl.io.SessionGraphDataSource
import org.opencypher.okapi.impl.util.Measurement.time
import org.opencypher.okapi.ir.api.configuration.IrConfiguration.PrintIr
import org.opencypher.okapi.ir.api.expr.Expr
import org.opencypher.okapi.ir.api.{CypherQuery, IRCatalogGraph}
import org.opencypher.okapi.ir.impl.parse.CypherParser
import org.opencypher.okapi.ir.impl.{IRBuilder, IRBuilderContext, QGNGenerator, QueryCatalog}
import org.opencypher.okapi.logical.api.configuration.LogicalConfiguration.PrintLogicalPlan
import org.opencypher.okapi.logical.impl.{LogicalOperatorProducer, LogicalOptimizer, LogicalPlanner, LogicalPlannerContext}
import org.opencypher.okapi.relational.api.configuration.CoraConfiguration.{PrintFlatPlan, PrintPhysicalPlan}
import org.opencypher.okapi.relational.impl.flat.{FlatPlanner, FlatPlannerContext}
import org.opencypher.okapi.relational.impl.physical.PhysicalPlanner

object MemCypherSession {
  def create: MemCypherSession = new MemCypherSession(SessionGraphDataSource.Namespace)
}

class MemCypherSession(override val sessionNamespace: Namespace) extends CypherSession {

  self =>

  private val producer = new LogicalOperatorProducer
  private val logicalPlanner = new LogicalPlanner(producer)
  private val logicalOptimizer = LogicalOptimizer
  private val flatPlanner = new FlatPlanner()
  private val physicalPlanner = new PhysicalPlanner(new MemOperatorProducer()(this))
  private val parser = CypherParser

  private val maxSessionGraphId: AtomicLong = new AtomicLong(0)

  private[opencypher] val emptyGraphQgn = QualifiedGraphName(sessionNamespace, GraphName("emptyGraph"))

  // Store empty graph in catalog, so operators that start with an empty graph can refer to its QGN
  // TODO: this is duplicated from CAPSSession and can be generalized
  store(emptyGraphQgn, MemCypherGraph.empty(this))

  def catalog(qualifiedGraphName: QualifiedGraphName): PropertyGraphDataSource = {
    dataSourceMapping(qualifiedGraphName.namespace)
  }

  /**
    * Executes a Cypher query in this session on the current ambient graph.
    *
    * @param query      Cypher query to execute
    * @param parameters parameters used by the Cypher query
    * @return result of the query
    */
  override def cypher(query: String, parameters: CypherMap = CypherMap.empty, drivingTable: Option[CypherRecords] = None): CypherResult =
    cypherOnGraph(MemCypherGraph.empty(this), query, parameters, drivingTable)

  override def cypherOnGraph(graph: PropertyGraph, query: String, parameters: CypherMap, maybeDrivingTable: Option[CypherRecords]): CypherResult = {
    val ambientGraph = mountAmbientGraph(graph)

    val (stmt, extractedLiterals, semState) = time("AST construction")(parser.process(query)(CypherParser.defaultContext))

    val extractedParameters = extractedLiterals.mapValues(v => CypherValue(v))
    val allParameters: CypherMap = parameters ++ extractedParameters

    val ir = time("IR translation")(IRBuilder(stmt)(IRBuilderContext.initial(query, allParameters, semState, ambientGraph, qgnGenerator, dataSourceMapping)))

    if (PrintIr.isSet) {
      println(ir.pretty)
    }

    val cypherQuery = ir match {
      case cq: CypherQuery[Expr] => cq
      case other => throw NotImplementedException(s"No support for IR of type: ${other.getClass}")
    }

    val logicalPlannerContext = LogicalPlannerContext(graph.schema, Set.empty, catalog)
    val logicalPlan = time("Logical planning")(logicalPlanner(cypherQuery)(logicalPlannerContext))
    val optimizedLogicalPlan = time("Logical optimization")(logicalOptimizer(logicalPlan)(logicalPlannerContext))

    if (PrintLogicalPlan.isSet) {
      println(logicalPlan.pretty)
      println(optimizedLogicalPlan.pretty)
    }

    val flatPlan = time("Flat planning")(flatPlanner(optimizedLogicalPlan)(FlatPlannerContext(allParameters)))
    if (PrintFlatPlan.isSet) println(flatPlan.pretty)

    val memPlannerContext = MemPhysicalPlannerContext.from(QueryCatalog(dataSourceMapping), MemRecords.unit()(this), allParameters)(this)
    val memPlan = time("Physical planning")(physicalPlanner.process(flatPlan)(memPlannerContext))

    if (PrintPhysicalPlan.isSet) {
      println(memPlan.pretty)
    }

    time("Query execution")(MemCypherResultBuilder.from(logicalPlan, flatPlan, memPlan)(MemRuntimeContext(memPlannerContext.parameters, graphAt)))
  }

  private def graphAt(qualifiedGraphName: QualifiedGraphName): Option[MemCypherGraph] =
    Some(dataSource(qualifiedGraphName.namespace).graph(qualifiedGraphName.graphName).asMemCypher)

  private[opencypher] val qgnGenerator = new QGNGenerator {
    override def generate: QualifiedGraphName = {
      QualifiedGraphName(SessionGraphDataSource.Namespace, GraphName(s"tmp#${maxSessionGraphId.incrementAndGet}"))
    }
  }

  private def mountAmbientGraph(ambient: PropertyGraph): IRCatalogGraph = {
    val qgn = qgnGenerator.generate
    store(qgn, ambient)
    IRCatalogGraph(qgn, ambient.schema)
  }
}
