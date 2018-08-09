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

package org.opencypher.memcypher.apps

import com.typesafe.scalalogging.Logger
import jdk.nashorn.internal.ir.debug.ObjectSizeCalculator
import org.opencypher.memcypher.api.value.MemNode
import org.opencypher.memcypher.api.{MemCypherGraph, MemCypherSession}
import org.opencypher.memcypher.apps.ConstructEval.logger
import org.opencypher.okapi.api.configuration.Configuration.PrintTimings
import org.opencypher.okapi.api.value.CypherValue.CypherMap

object ConstructEval extends App {
  //function to get a specific number of nodes (size of the dataset)
  def getData(nodeNumber: Int) = {
    implicit val memCypher: MemCypherSession = MemCypherSession.create
    var nodes: Seq[MemNode] = Seq.empty
    val r = scala.util.Random //random number oder über x mod n deterministisch die values bestimmen?
    for (x <- 1 to nodeNumber) {
      nodes :+= MemNode(x, Set.empty, CypherMap("two" -> r.nextInt(2), "five" -> r.nextInt(5), "ten" -> r.nextInt(10),
        "twenty" -> r.nextInt(20), "fifty" -> r.nextInt(50), "hundred" -> r.nextInt(100)))
    }
    MemCypherGraph.create(nodes, Seq.empty)
  }

  def regardingInputCardinality(stepsize: Int, steps: Int) = {
    implicit val memCypher: MemCypherSession = MemCypherSession.create
    val query =
      s"""|MATCH (b)
          |Construct New ({groupby:1})
          |RETURN Graph""".stripMargin
    var nodes: Seq[MemNode] = Seq.empty
    for (x <- 1 to steps) {
      var dataSize = x * stepsize
      logger.info(s"Executing query on $dataSize matches")
      //redundant code but reduces unnecessary node creations?
      val r = scala.util.Random //random number oder über x mod n deterministisch die values bestimmen?
      for (y <- 1 to stepsize) {
        nodes :+= MemNode(y + dataSize, Set.empty, CypherMap("two" -> r.nextInt(2), "five" -> r.nextInt(5), "ten" -> r.nextInt(10),
          "twenty" -> r.nextInt(20), "fifty" -> r.nextInt(50), "hundred" -> r.nextInt(100)))
      }
      val graph = MemCypherGraph.create(nodes, Seq.empty)
      val result = graph.cypher(query).getRecords
      logger.info("size " + ObjectSizeCalculator.getObjectSize(result.collect) + " bytes")
    }
  }

  def regardingOutputCardinality(outputCardinality: List[Any],aggregatedProperty:Boolean, graph: MemCypherGraph) = {
    val matchPattern = "MATCH (b) "
    val returnPattern = "Return Graph"
    val graphsize = graph.nodes.length
    outputCardinality.foreach(
      grouping => {
        val string = grouping match {
          case s: String => "\"" + s + "\""
          case l: List[Any] => "[" + l.map(s => "\"" + s + "\"").mkString(",").concat("]")
          case x => x.toString
        }
        val constructPattern = if(aggregatedProperty) s"""Construct New ({groupby:$string,what:"max(b)"})""" else s"Construct New ({groupby:$string})"
        val query = matchPattern + constructPattern + returnPattern
        logger.info(query + s" on $graphsize matches")
        val result = graph.cypher(query)
        //logger.info("record size " + ObjectSizeCalculator.getObjectSize(result.getRecords.collect) + " bytes")
        //logger.info("graph size with " + result.getGraph.nodes("n").size + " nodes " + ObjectSizeCalculator.getObjectSize(result.getGraph) + " bytes")
      }
    )
  }

  def regardingProperties(expr: String, maxNumber: Int, graph: MemCypherGraph) = {
    val matchPattern = "MATCH (b) "
    val returnPattern = "Return Graph"
    var properties = ""
    val graphsize = graph.nodes.length
    for (x <- 1 to maxNumber) {
      properties += s"""aggr$x :"$expr","""
      val constructPattern = s"Construct New ({groupby:1,${properties.substring(0, properties.length - 1)}})"
      val query = matchPattern + constructPattern + returnPattern
      logger.info(s"$x properties with expr: $expr on $graphsize matches")
      val result = graph.cypher(query)
      //logger.info("record size " + ObjectSizeCalculator.getObjectSize(result.getRecords.collect) + " bytes")
      //logger.info("graph size with "+result.getGraph.nodes("n").size+" nodes " + ObjectSizeCalculator.getObjectSize(result.getGraph) + " bytes")
    }
  }

  def regardingNodeNumber(maxNumber: Int, graph: MemCypherGraph,aggregated:Boolean) = {
    val matchPattern = "MATCH (x) "
    val returnPattern = "Return Graph"
    var nodes = ""
    val node:String = {if(aggregated) s"""({groupby:"x.ten",what:"max(x)"}),""" else s"""(),"""}
    val graphsize = graph.nodes.length
    for (x: Int <- 1 to maxNumber) {
      nodes += node
      val constructPattern = s"Construct New ${nodes.substring(0, nodes.length - 1)}"
      val query = matchPattern + constructPattern + returnPattern
      logger.info(s"$x nodes ($node) on $graphsize matches")
      val result = graph.cypher(query)
      //logger.info("record size " + ObjectSizeCalculator.getObjectSize(result.getRecords.collect) + " bytes")
      //logger.info("graph size with " + result.getGraph.nodes("n").size + " nodes " + ObjectSizeCalculator.getObjectSize(result.getGraph) + " bytes")

    }
  }

  def regardingEdgeNumber(maxNumber: Int, graph: MemCypherGraph,aggregated:Boolean) = {
    val matchPattern = "MATCH (x) "
    val returnPattern = "Return Graph"
    var edges = ""
    val edge = {if(aggregated) s"""(a)-[:filler{groupby:"b.ten",what:"max(x)"]]->(b),""" else s"""(a)-[:filler]->(b),"""}
    val graphsize = graph.nodes.length
    for (x <- 1 to maxNumber) {
      edges += edge
      val constructPattern = s"Construct New (a),(b),${edges.substring(0, edges.length - 1)}"
      val query = matchPattern + constructPattern + returnPattern
      logger.info(s"$x edges and 2 nodes on $graphsize matches")
      val result = graph.cypher(query)
      //logger.info("record size " + ObjectSizeCalculator.getObjectSize(result.getRecords.collect) + " bytes")
      //logger.info("graph size " + ObjectSizeCalculator.getObjectSize(result.getGraph) + " bytes")
    }
  }

  def motivetheory(graph: MemCypherGraph): Unit = {
    val pattern_motive = "Match (x) Construct New (a)-[:e]->(b)-[:e]->(c)-[:e]->(d)-[:e]->(e) Return Graph"
    val simple_pattern = "Match (x) Construct New (a)-[:e]->(b),(a)-[:e]->(b),(a)-[:e]->(b),(a)-[:e]->(b),(c),(d),(e) Return Graph"

    logger.info("motive_pattern")
    val result_motive = graph.cypher(pattern_motive).getRecords
    logger.info("simple_pattern")
    val result_simple = graph.cypher(simple_pattern).getRecords
    logger.info("simple: " + ObjectSizeCalculator.getObjectSize(result_simple) + " pattern: " + ObjectSizeCalculator.getObjectSize(result_motive))
  }

  //todo: when testing graph based approach ... also look at memory of graph (alter tests therefore)
  //todo: when testing size of graph ... without loop ? (memcypherSession wird größer)
  val logger = Logger("Construct Evaluation")
  //PrintTimings.set()


  for (x <- 1 to 10) {
    //PrintTimings.set()
    implicit val memCypher: MemCypherSession = MemCypherSession.create
    val warmupGraph = getData(100)
    val realGraph = getData(100000)
    //regardingInputCardinality(100,20) //warmup
    //regardingInputCardinality(50000, 10)
    //regardingOutputCardinality(List(1, "b.two", "b.five", "b.ten", "b.twenty", "b.fifty", "b.hundred", "b"),true, warmupGraph) //warmup
    //regardingOutputCardinality(List(List("b.two", "b.five"), List("b.two", "b.ten"), List("b.two", "b", "b.five", "b.ten")),true, warmupGraph) //warmup
    //regardingOutputCardinality(List(1, "b.two", "b.five", "b.ten", "b.twenty", "b.fifty", "b.hundred"),true, realGraph)
    //regardingOutputCardinality(List(List("b.two", "b.hundred"), List("b.five", "b.hundred"), List("b.ten", "b.hundred"), List("b.twenty", "b.hundred"), List("b.fifty", "b.hundred"), List("b.ten", "b.twenty", "b.hundred"), List("b.ten", "b.fifty", "b.hundred"), List("b"), List()),true, realGraph) //todo more Lists
  //todo test copy
    // todo redo edges and nodes with aggregaed properties
    //regardingProperties("max(b)",10,warmupGraph) //warmup
  //regardingProperties("max(b)",20,realGraph)
  regardingNodeNumber(10,warmupGraph,true)
  regardingNodeNumber(10,realGraph,true)//}
  //regardingEdgeNumber(10,warmupGraph,true)
  //regardingEdgeNumber(10,realGraph,true)
  //motivetheory(warmupGraph)
  //motivetheory(realGraph)
}
}



