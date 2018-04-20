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
import org.opencypher.memcypher.api.value.{MemNode, MemRelationship}
import org.opencypher.memcypher.api.{MemCypherGraph, MemCypherSession}
import org.opencypher.okapi.api.configuration.Configuration.PrintTimings
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.relational.api.configuration.CoraConfiguration.{PrintFlatPlan, PrintPhysicalPlan}

object Demo extends App {

  val logger = Logger("Demo")

  PrintTimings.set()
  PrintFlatPlan.set()
  PrintPhysicalPlan.set()

  val query =
    s"""|MATCH (n:Person)
        |RETURN n.city, n.age
        |ORDER BY n.city ASC, n.age DESC""".stripMargin

  logger.info(s"Executing query: $query")

  implicit val memCypher: MemCypherSession = MemCypherSession.create

  val graph = MemCypherGraph.create(DemoData.nodes, DemoData.rels)

  graph.cypher(query).show
}

object DemoData {

  def nodes = Seq(n0, n1, n2, n3, n4, n5, n6, n7, n8, n9, n10)

  def rels = Seq(
    e0, e1, e2, e3, e4, e5, e6, e7, e8, e9,
    e10, e11, e12, e13, e14, e15, e16, e17, e18, e19,
    e20, e21, e22, e23)


  val n0 = MemNode(0L, Set("Person"), CypherMap(
    "name" -> "Alice",
    "gender" -> "f",
    "city" -> "Leipzig",
    "age" -> 20
  ))

  val n1 = MemNode(1L, Set("Person"), CypherMap(
    "name" -> "Bob",
    "gender" -> "m",
    "city" -> "Leipzig",
    "age" -> 30
  ))

  val n2 = MemNode(2L, Set("Person"), CypherMap(
    "name" -> "Carol",
    "gender" -> "f",
    "city" -> "Dresden",
    "age" -> 30
  ))

  val n3 = MemNode(3L, Set("Person"), CypherMap(
    "name" -> "Dave",
    "gender" -> "m",
    "city" -> "Dresden",
    "age" -> 40
  ))

  val n4 = MemNode(4L, Set("Person"), CypherMap(
    "name" -> "Eve",
    "gender" -> "f",
    "city" -> "Dresden",
    "speaks" -> "English",
    "age" -> 35
  ))

  val n5 = MemNode(5L, Set("Person"), CypherMap(
    "name" -> "Frank",
    "gender" -> "m",
    "city" -> "Berlin",
    "LocIP" -> "127.0.0.1",
    "age" -> 42
  ))

  val n6 = MemNode(6L, Set("Tag"), CypherMap(
    "name" -> "Databases"
  ))

  val n7 = MemNode(7L, Set("Tag"), CypherMap(
    "name" -> "Graphs"
  ))

  val n8 = MemNode(8L, Set("Tag"), CypherMap(
    "name" -> "Hadoop"
  ))

  val n9 = MemNode(9L, Set("Forum"), CypherMap(
    "title" -> "Graph Databases"
  ))

  val n10 = MemNode(10L, Set("Forum"), CypherMap(
    "title" -> "Graph Processing"
  ))

  val e0 = MemRelationship(0L, n0.id, n1.id, "KNOWS", CypherMap("since" -> 2014))
  val e1 = MemRelationship(1L, n1.id, n0.id, "KNOWS", CypherMap("since" -> 2014))
  val e2 = MemRelationship(2L, n1.id, n2.id, "KNOWS", CypherMap("since" -> 2013))
  val e3 = MemRelationship(3L, n2.id, n1.id, "KNOWS", CypherMap("since" -> 2013))
  val e4 = MemRelationship(4L, n2.id, n3.id, "KNOWS", CypherMap("since" -> 2014))
  val e5 = MemRelationship(5L, n3.id, n2.id, "KNOWS", CypherMap("since" -> 2014))
  val e6 = MemRelationship(6L, n4.id, n0.id, "KNOWS", CypherMap("since" -> 2013))
  val e7 = MemRelationship(7L, n4.id, n1.id, "KNOWS", CypherMap("since" -> 2015))
  val e8 = MemRelationship(8L, n5.id, n2.id, "KNOWS", CypherMap("since" -> 2015))
  val e9 = MemRelationship(9L, n5.id, n3.id, "KNOWS", CypherMap("since" -> 2015))

  val e10 = MemRelationship(10L, n4.id, n6.id, "HAS_INTEREST")
  val e11 = MemRelationship(11L, n0.id, n6.id, "HAS_INTEREST")
  val e12 = MemRelationship(12L, n3.id, n8.id, "HAS_INTEREST")
  val e13 = MemRelationship(13L, n5.id, n8.id, "HAS_INTEREST")

  val e14 = MemRelationship(14L, n9.id, n6.id, "HAS_TAG")
  val e15 = MemRelationship(15L, n9.id, n7.id, "HAS_TAG")
  val e16 = MemRelationship(16L, n10.id, n7.id, "HAS_TAG")
  val e17 = MemRelationship(17L, n10.id, n8.id, "HAS_TAG")

  val e18 = MemRelationship(18L, n9.id, n0.id, "HAS_MODERATOR")
  val e19 = MemRelationship(19L, n10.id, n3.id, "HAS_MODERATOR")

  val e20 = MemRelationship(20L, n9.id, n0.id, "HAS_MEMBER")
  val e21 = MemRelationship(21L, n9.id, n1.id, "HAS_MEMBER")
  val e22 = MemRelationship(22L, n10.id, n2.id, "HAS_MEMBER")
  val e23 = MemRelationship(23L, n10.id, n3.id, "HAS_MEMBER")


}
