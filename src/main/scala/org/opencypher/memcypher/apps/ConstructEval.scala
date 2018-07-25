package org.opencypher.memcypher.apps

import com.typesafe.scalalogging.Logger
import jdk.nashorn.internal.ir.debug.ObjectSizeCalculator
import org.opencypher.memcypher.api.value.MemNode
import org.opencypher.memcypher.api.{MemCypherGraph, MemCypherSession}
import org.opencypher.okapi.api.configuration.Configuration.PrintTimings
import org.opencypher.okapi.api.value.CypherValue.CypherMap

object ConstructEval extends App {
  //function to get a specific number of nodes (size of the dataset)
  def getData(nodeNumber: Int) = {
    var nodes: Seq[MemNode] = Seq.empty
    val r = scala.util.Random //random number oder über x mod n deterministisch die values bestimmen?
    for (x <- 1 to nodeNumber) {
      nodes :+= MemNode(x, Set.empty, CypherMap("two" -> r.nextInt(2), "five" -> r.nextInt(5), "ten" -> r.nextInt(10),
        "twenty" -> r.nextInt(20), "fifty" -> r.nextInt(50), "hundred" -> r.nextInt(100)))
    }
    MemCypherGraph.create(nodes, Seq.empty)
  }

  def regardingInputCardinality(stepsize: Int, steps: Int) = {
    val query =
      s"""|MATCH (b)
          |Construct New ({groupby:1})
          |RETURN Graph""".stripMargin
    var nodes: Seq[MemNode] = Seq.empty
    for (x <- 0 to steps) {
      var dataSize = x * stepsize
      if(x==0) dataSize = 1
      logger.info(s"Executing query on $dataSize matches")
      //redundant code but reduces unnecessary node creations?
      val r = scala.util.Random //random number oder über x mod n deterministisch die values bestimmen?
      for (y <- 1 to stepsize) {
        nodes :+= MemNode(y+dataSize, Set.empty, CypherMap("two" -> r.nextInt(2), "five" -> r.nextInt(5), "ten" -> r.nextInt(10),
          "twenty" -> r.nextInt(20), "fifty" -> r.nextInt(50), "hundred" -> r.nextInt(100)))
      }
      val graph = MemCypherGraph.create(nodes, Seq.empty)
      val result = graph.cypher(query).getRecords
      logger.info("size " + ObjectSizeCalculator.getObjectSize(result.collect) + " bytes")
    }
  }

  def regardingOutputCardinality(outputCardinality: List[Any],graph: MemCypherGraph) = {
    val matchPattern = "MATCH (b) "
    val returnPattern = "Return Graph"

    outputCardinality.foreach(
      grouping => {
        val string = grouping match {
          case s: String => "\"" + s + "\""
          case l: List[Any] => "[" + l.map(s => "\"" + s + "\"").mkString(",").concat("]")
          case x => x.toString
        }
        val constructPattern = s"Construct New ({groupby:$string})"
        val query = matchPattern + constructPattern + returnPattern
        logger.info(query + s" on ${graph.nodes.length} matches")
        val result = graph.cypher(query).getRecords
        logger.info("size " + ObjectSizeCalculator.getObjectSize(result.collect) + " bytes")
      }
    )
  }

  def regardingAggregatedProperties(aggregator: String, maxNumber: Int, graph: MemCypherGraph) = {
    val matchPattern = "MATCH (b) "
    val returnPattern = "Return Graph"
    var properties = ""
    for (x <- 1 to maxNumber) {
      properties += s"""aggr$x :"$aggregator","""
      val constructPattern = s"Construct New ({groupby:1,${properties.substring(0, properties.length - 1)}})"
      val query = matchPattern + constructPattern + returnPattern
      logger.info(s"$x aggregated properties on ${graph.nodes.length} matches")
      graph.cypher(query)
    }
  }

  def regardingNodeNumber(maxNumber: Int, graph: MemCypherGraph) = {
    val matchPattern = "MATCH (x) "
    val returnPattern = "Return Graph"
    var nodes = ""
    for (x: Int <- 1 to maxNumber) {
      nodes += s"""(),"""
      val constructPattern = s"Construct New ${nodes.substring(0, nodes.length - 1)}"
      val query = matchPattern + constructPattern + returnPattern
      logger.info(s"$x nodes on ${graph.nodes.length} matches")
      val result = graph.cypher(query).getRecords
      logger.info("size " + ObjectSizeCalculator.getObjectSize(result.collect) + " bytes") //linearer Zusammenhang, bis auf sprung von 1980 zu 52688 ?

    }
  }

  def regardingEdgeNumber(maxNumber: Int, graph: MemCypherGraph) = {
    val matchPattern = "MATCH (x) "
    val returnPattern = "Return Graph"
    var edges = ""
    for (x <- 1 to maxNumber) {
      edges += s"""(a)-[:filler]->(b),"""
      val constructPattern = s"Construct New (a),(b),${edges.substring(0, edges.length - 1)}"
      val query = matchPattern + constructPattern + returnPattern
      logger.info(s"$x edges and 2 nodes on ${graph.nodes.length} matches")
      graph.cypher(query)
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

  val logger = Logger("Construct Evaluation")
  //PrintTimings.set()
  implicit val memCypher: MemCypherSession = MemCypherSession.create
  val warmupGraph = getData(1000)
  val realGraph = getData(100000)
  regardingInputCardinality(100,20) //warmup
  regardingInputCardinality(50000, 10)
  regardingOutputCardinality(List(1,"b.two","b.five","b.ten","b.twenty","b.fifty","b.hundred","b"),warmupGraph) //warmup
  regardingOutputCardinality(List(1,"b.two","b.five","b.ten","b.twenty","b.fifty","b.hundred","b"),realGraph)
  regardingOutputCardinality(List(List("b.two","b.five"),List("b.two","b.ten"),List("b.two","b","b.five","b.ten")),warmupGraph) //warmup
  regardingOutputCardinality(List(List("b.two","b.five"),List("b.two","b.ten"),List("b.two","b.five","b.ten"),List("b.two","b.hundred"),List("b.five","b.hundred"),List("b.ten","b.hundred"),List("b.twenty","b.hundred"),List("b.fifty","b.hundred"),List("b.ten","b.twenty","b.hundred"),List("b.ten","b.fifty","b.hundred"),List("b"),List(),List("b","b.ten"),List("b","b.ten","b.fifty","b.hundred")),realGraph) //todo more Lists
  regardingAggregatedProperties("collect(b)",5,warmupGraph) //warmup
  regardingAggregatedProperties("collect(b)",20,realGraph)
  regardingNodeNumber(10,warmupGraph)
  regardingNodeNumber(10,realGraph)
  regardingEdgeNumber(10,warmupGraph)
  regardingEdgeNumber(10,realGraph)
  motivetheory(warmupGraph)
  motivetheory(realGraph)
}



