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
    for (x <- 0 to steps) {
      var dataSize = x * stepsize
      if(x==0) dataSize = 1
      logger.info(s"Executing query with $dataSize matches")
      //System.gc()
      val graph = getData(dataSize)
      val result = graph.cypher(query).getRecords
      //logger.info("size " + ObjectSizeCalculator.getObjectSize(result.collect) + " bytes")
    }
  }

  def regardingOutputCardinality(outputCardinality: List[Any], inputCardinality: Int) = {
    val matchPattern = "MATCH (b) "
    val returnPattern = "Return Graph"
    val graph = getData(inputCardinality)

    outputCardinality.foreach(
      grouping => {
        val string = grouping match {
          case s: String => "\"" + s + "\""
          case l: List[Any] => "[" + l.map(s => "\"" + s + "\"").mkString(",").concat("]")
          case x => x.toString
        }
        val constructPattern = s"Construct New ({groupby:$string})"
        val query = matchPattern + constructPattern + returnPattern
        logger.info(query + s" on $inputCardinality matches")
        val result = graph.cypher(query).getRecords
        logger.info("size " + ObjectSizeCalculator.getObjectSize(result.collect) + " bytes")
      }
    )
  }

  def regardingAggregatedProperties(aggregator: String, maxNumber: Int, inputCardinality: Int) = {
    val matchPattern = "MATCH (b) "
    val returnPattern = "Return Graph"
    val graph = getData(inputCardinality)
    var properties = ""
    for (x <- 1 to maxNumber) {
      properties += s"""aggr$x :"$aggregator","""
      val constructPattern = s"Construct New ({groupby:1,${properties.substring(0, properties.length - 1)}})"
      val query = matchPattern + constructPattern + returnPattern
      logger.info(s"$x aggregated properties on $inputCardinality matches")
      graph.cypher(query)
    }
  }

  def regardingNodeNumber(maxNumber: Int, inputCardinality: Int) = {
    val matchPattern = "MATCH (x) "
    val returnPattern = "Return Graph"
    val graph = getData(inputCardinality)
    var nodes = ""
    for (x: Int <- 1 to maxNumber) {
      nodes += s"""(),"""
      val constructPattern = s"Construct New ${nodes.substring(0, nodes.length - 1)}"
      val query = matchPattern + constructPattern + returnPattern
      logger.info(s"$x nodes on $inputCardinality matches")
      val result = graph.cypher(query).getRecords
      logger.info("size " + ObjectSizeCalculator.getObjectSize(result.collect) + " bytes") //linearer Zusammenhang, bis auf sprung von 1980 zu 52688 ?

    }
  }

  def regardingEdgeNumber(maxNumber: Int, inputCardinality: Int) = {
    val matchPattern = "MATCH (x) "
    val returnPattern = "Return Graph"
    val graph = getData(inputCardinality)
    var edges = ""
    for (x <- 1 to maxNumber) {
      edges += s"""(a)-[:filler]->(b),"""
      val constructPattern = s"Construct New (a),(b),${edges.substring(0, edges.length - 1)}"
      val query = matchPattern + constructPattern + returnPattern
      logger.info(s"$x edges and 2 nodes on $inputCardinality matches")
      graph.cypher(query)
    }
  }

  def motivetheory(): Unit = {
    val pattern_motive = "Match (x) Construct New (a)-[:e]->(b)-[:e]->(c)-[:e]->(d)-[:e]->(e) Return Graph"
    val simple_pattern = "Match (x) Construct New (a)-[:e]->(b),(a)-[:e]->(b),(a)-[:e]->(b),(a)-[:e]->(b),(c),(d),(e) Return Graph"

    val graph = getData(1000)
    logger.info("motive_pattern")
    val result_motive = graph.cypher(pattern_motive).getRecords
    logger.info("simple_pattern")
    val result_simple = graph.cypher(simple_pattern).getRecords
    logger.info("simple: " + ObjectSizeCalculator.getObjectSize(result_simple) + " pattern: " + ObjectSizeCalculator.getObjectSize(result_motive))
  }

  val logger = Logger("Construct Evaluation")
  //PrintTimings.set()
  implicit val memCypher: MemCypherSession = MemCypherSession.create
  regardingInputCardinality(100,20) //warmup
  regardingInputCardinality(50000, 20)
  //regardingOutputCardinality(List(1,"b.two","b.five","b.ten","b.twenty","b.fifty","b.hundred","b"),1000)
  //regardingOutputCardinality(List(List("b.two","b.five"),List("b.two","b.ten"),List("b.two","b","b.five","b.ten")),1000) //todo more Lists
  //regardingAggregatedProperties("collect(b)",5,1000)
  //regardingNodeNumber(10,1000)
  //regardingEdgeNumber(10,1000)
  //motivetheory()
}



