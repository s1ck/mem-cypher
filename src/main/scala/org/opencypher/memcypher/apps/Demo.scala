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
import org.opencypher.okapi.api.configuration.Configuration.PrintTimings
import org.opencypher.okapi.api.value.CypherValue.{CypherInteger, CypherMap, CypherString}
import org.opencypher.memcypher.api.value.{MemNode, MemRelationship}
import org.opencypher.memcypher.api.{MemCypherGraph, MemCypherSession}
import org.opencypher.okapi.relational.api.configuration.CoraConfiguration.PrintPhysicalPlan

object Demo extends App {

  val logger = Logger("Demo")

  PrintTimings.set()
  PrintPhysicalPlan.set()

  val query = "MATCH (n)-[r:KNOWS]->(m) WHERE n.age = 42 RETURN n, n.age, n.foo, labels(n), type(r)"

  logger.info(
    s"""Executing query:
       |$query
       """.stripMargin)

  implicit val memCypher: MemCypherSession = MemCypherSession.create

  val graph = MemCypherGraph.create(DemoData.nodes, DemoData.rels)

  graph.cypher(query).show
}

object DemoData {

  def nodes = Seq(alice, bob)

  def rels = Seq(aliceKnowsBob, aliceOwnsBob)

  val aliceId = 0L
  val alice = MemNode(
    aliceId,
    Set("Person"),
    CypherMap(
      "name" -> CypherString("Alice"),
      "age" -> CypherInteger(42)
    )
  )

  val bobId = 1L
  val bob = MemNode(
    bobId,
    Set("Person"),
    CypherMap(
      "age" -> CypherInteger(23)
    )
  )

  val aliceKnowsBob = MemRelationship(
    0L,
    aliceId,
    bobId,
    "KNOWS",
    CypherMap("since" -> CypherInteger(2018))
  )

  val aliceOwnsBob = MemRelationship(
    0L,
    aliceId,
    bobId,
    "OWNS",
    CypherMap("since" -> CypherInteger(2017))
  )
}
