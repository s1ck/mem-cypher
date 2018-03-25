package org.opencypher.memcypher.apps

import com.github.tototoshi.csv._
import org.opencypher.memcypher.api.value.{MemNode, MemRelationship}
import org.opencypher.memcypher.api.{MemCypherGraph, MemCypherSession}

object Benchmark extends App {

  implicit val memCypher: MemCypherSession = MemCypherSession.create

  val data =  new WebGoogle(args(0), Some(1000))
  val graph = MemCypherGraph.create(data.nodes, data.rels)

  val query = "MATCH (n)-->(m) RETURN n"
  benchmark({
    graph.cypher(query).getRecords.size
  })

  def benchmark(f: => Unit): Unit = {
    def run(runs: Int) = {
      (1 to runs).foldLeft(List.empty[Long]) {
        case (currentDurations, _) =>
          currentDurations :+ measure(f)
      }
    }
    val warmupRuns = run(10)
    val actualRuns = run(100)

    println("warm-ups")
    print(warmupRuns, math.pow(10, 6).toLong, "ms")
    println("actual runs")
    print(actualRuns, math.pow(10, 6).toLong, "ms")
  }

  def measure(f: => Unit): Long = {
    val start = System.nanoTime()
    f
    System.nanoTime() - start
  }

  def print(durations: List[Long], denominator: Long = 1, unit: String = "ns"): Unit = {
    val updatedDurations = durations.map(_ / denominator)

    println(s"Cnt: ${updatedDurations.size}")
    println(s"Min: ${updatedDurations.min} $unit")
    println(s"Max: ${updatedDurations.max} $unit")
    println(s"Avg: ${updatedDurations.sum / updatedDurations.size} $unit")
  }
}

class WebGoogle(inputPath: String, limit: Option[Int] = None) {
  private val nodeLabels = Set("Page")
  private val relType = "LINKS"

  // custom format
  implicit object TabDelimiter extends DefaultCSVFormat {
    override val delimiter = '\t'
  }

  private val relReader: CSVReader = CSVReader.open(inputPath)

  lazy val relIds: Stream[(Long, Long)] = {
    val lines = limit match {
      case Some(l) => relReader.toStream.take(l)
      case None => relReader.toStream
    }
    lines.map(list => list.head.toLong -> list(1).toLong)
  }

  def nodes: Seq[MemNode] = relIds
    .flatMap(pair => Seq(pair._1, pair._2))
    .distinct
    .map(MemNode(_, nodeLabels))

  def rels: Seq[MemRelationship] = relIds
    .zipWithIndex
    .map { case ((sourceId, targetId), rId) => MemRelationship(rId, sourceId, targetId, relType) }
}