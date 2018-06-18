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
package org.opencypher.memcypher.matching

import org.opencypher.memcypher.MemCypherTestSuite
import org.opencypher.memcypher.api.{Embeddings, MemCypherSession, MemRecords}
import org.opencypher.memcypher.api.value.{MemNode, MemRelationship}
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.ir.api.Label

class ConstructAcceptanceTest extends MemCypherTestSuite {

  describe("node-constructs") {

    it("without unnamed construct-variable") {
      val graph = initGraph("CREATE (:Person), (:Car)")
      val result = graph.cypher("CONSTRUCT NEW() RETURN GRAPH")
      val recordArray = result.getRecords.collect
      recordArray.length should be(1)
      recordArray(0) should be(CypherMap("  FRESH_VAR13" -> MemNode(0))) //var-name "FRESH_VAR13" is generated
      /*result.getGraph.nodes("n").collect.length should be(1)
      result.getGraph.nodes("n").collect should be(Array(CypherMap("n" -> MemNode(1, Set.empty, CypherMap.empty))))*/
    }

    it("with unbound construct variable") {
      val graph = initGraph("CREATE (:PERSON),(:CAR)")
      val result = graph.cypher("Match (i) CONSTRUCT NEW(n) RETURN GRAPH")
      val records = result.getRecords
      records.size shouldBe (2)
      records.collect should be(Array(CypherMap("n" -> MemNode(0)), CypherMap("n" -> MemNode(1))))
      /*result.getGraph.nodes("n").collect.length should be(2)
      result.getGraph.nodes("n").collect should be(Array(CypherMap("n" -> MemNode(1, Set(""), CypherMap.empty)), CypherMap("n" -> MemNode(2, Set(""), CypherMap.empty))))*/
    }

    it("with bound construct variable and copying properties and labels") {
      val graph = initGraph("""CREATE (:Person{age:10}),(:Car{color:"blue"})""")
      val result = graph.cypher("MATCH (n) CONSTRUCT NEW(n COPY OF n) RETURN GRAPH")
      val records = result.getRecords

      records.size shouldBe (2)
      records.collect should be(Array(CypherMap("n" -> MemNode(0, Set("Person"), CypherMap("age" -> 10)), "n" -> MemNode(1, Set("Car"), CypherMap("color" -> "blue")))))

      //result should contain same nodes as "graph"(except id?!)
      /*result.getGraph.nodes("n").collect.toSet should be(graph.nodes.toSet)*/
    }

    it("with group by valid set of columns") {
      val graph = initGraph("CREATE (a:Person)-[:likes]->(b:Car)-[:boughtby]->(a), (a)-[:owns]->(b)")
      val result = graph.cypher("MATCH (n)-->(m) CONSTRUCT NEW(x{groupby:['m','n']}) RETURN GRAPH")
      val records = result.getRecords

      records.collect should be(Array(CypherMap("x" -> MemNode(0)), CypherMap("x" -> MemNode(1)), CypherMap("x" -> MemNode(0))))
      //result should construct 2 new nodes (one for (a,b) and another one for (b,a))
      /*result.getGraph.nodes("n").collect.length should be(2)*/
    }

    it("with group by property") {
      val graph = initGraph("CREATE (:Person{age:18}),(:Person{age:18}),(:Person{age:20})")
      val result = graph.cypher("MATCH (n) CONSTRUCT NEW(x{groupby:'n.age'}) RETURN GRAPH")
      val records = result.getRecords

      records.collect should be(Array(CypherMap("x" -> MemNode(0)), CypherMap("x" -> MemNode(1)), CypherMap("x" -> MemNode(1))))
      //result should construct 2 new nodes (for n.age=18 and n.age=20)
      /*result.getGraph.nodes("n").collect.length should be(2)*/
    }

    it("with group by relationship-type") {
      val graph = initGraph("CREATE (a:Person{age:18}),(b:Item{price:20}),(a)-[:SOLD]->(b),(a)-[:SOLD]->(b),(a)-[:BOUGHT]->(b)")
      val result = graph.cypher("MATCH (n)-[b]->(m) CONSTRUCT NEW(x{groupby:'type(b)'}) RETURN GRAPH")
      val records = result.getRecords

      records.collect should be(Array(CypherMap("x" -> MemNode(0)), CypherMap("x" -> MemNode(1)), CypherMap("x" -> MemNode(1))))

      //result should construct 2 new nodes (bought and sold)
      /*result.getGraph.nodes("n").collect.length should be(2)*/
    }

    it("with group by constant") {
      val graph = initGraph("CREATE (:Person{age:18}),(:Person{age:18}),(:Person{age:20})")
      val result = graph.cypher("MATCH (n) CONSTRUCT NEW(x{groupby:1}) RETURN GRAPH")
      val records = result.getRecords

      records.collect should be(Array(CypherMap("x" -> MemNode(0)), CypherMap("x" -> MemNode(0)), CypherMap("x" -> MemNode(0))))
      //result should construct 1 new node
      /*result.getGraph.nodes("n").collect.length should be(1)*/
    }

    it("with group by invalid set of variables") {
      val graph = initGraph("CREATE (a:Person)-[:likes]->(b:Car)-[:boughtby]->(a), (a)-[:owns]->(b)")

      val thrown = intercept[Exception] {
        graph.cypher("MATCH (n)-->(m) CONSTRUCT NEW(x{groupby:['m','z']}) RETURN GRAPH")
      }

      // throw error, as grouping variable z is not part of the match
      assert(thrown.getMessage.contains("invalid groupBy parameter z"))
    }

    it("with setting properties") {
      val graph = initGraph("CREATE (:Person),(:Car)")
      val result = graph.cypher("""CONSTRUCT NEW (x{color:"blue",price:1000}) RETURN GRAPH""")
      val records = result.getRecords

      records.collect should be(Array(CypherMap("x" -> MemNode(0, Set.empty, CypherMap("color" -> "blue", "price" -> 1000)))))
      /*result.getGraph.nodes("n").collect should contain(CypherMap("n" -> MemNode(1, Set.empty, CypherMap("color" -> "blue", "price" -> 1000))))*/
    }
    it("with setting one label") {
      val graph = initGraph("CREATE (:Person),(:Car)")
      val result = graph.cypher("CONSTRUCT NEW (x:Person) RETURN GRAPH")
      val records = result.getRecords

      records.collect should be(Array(CypherMap("x" -> MemNode(0, Set("Person")))))
      /*result.getGraph.nodes("n").collect should contain(CypherMap("n" -> MemNode(1, Set("Person"))))*/
    }

    it("with setting multiple labels") {
      val graph = initGraph("CREATE (:Car)")
      val result = graph.cypher("CONSTRUCT NEW (x:Person:Actor) RETURN GRAPH")

      result.getRecords.collect should be(Array(CypherMap("x" -> MemNode(0, Set("Person", "Actor")))))
      /*result.getGraph.nodes("n").collect should be(Array(CypherMap("n" -> MemNode(1, Set("Person", "Actor")))))*/
    }

    it("with cloning") {
      val graph = initGraph("CREATE (:Car),(:Person{age:10,hobbies:['chess','reading']})")
      val result = graph.cypher("Match (p:Person) CONSTRUCT Clone p NEW(x{groupby:1}) RETURN GRAPH")
      //check that id gets copied and new ids arent colliding with copied ones
      result.getRecords.collect should be(Array(CypherMap("p" -> MemNode(1, Set("Person"), CypherMap("age" -> 10, "hobbies" -> List("chess", "reading"))),
        "x" -> MemNode(2))))
    }

    it("with overwriting copied properties") {
      val graph = initGraph("CREATE (:Car),(:Person{age:10,hobbies:['chess','reading']})")
      val result = graph.cypher("Match (p:Person) CONSTRUCT NEW(p2 COPY OF p{age:11, hobbies:'none'}) RETURN GRAPH")

      result.getRecords.collect should be(Array(CypherMap("p2" -> MemNode(1, Set("Person"), CypherMap("age" -> 11, "hobbies" -> "none")))))
    }

    it("with aggregated properties") {
      //max(n.age) must be a string (otherwise syntaxexception from cypher)
      val graph = initGraph("CREATE ({age:10}),({age:12}),({age:14})")
      val result = graph.cypher("""MATCH (n) CONSTRUCT NEW(x{max_age:"max(n.age)"}) RETURN GRAPH""")

      result.getRecords.collect should be(Array(CypherMap("x" -> MemNode(0, Set.empty, CypherMap("max_age" -> 14)))))
      /*result.getGraph.nodes("n").collect should contain(CypherMap("n" -> MemNode(1, Set.empty, CypherMap("max_age" -> 14))))*/
    }

    it("with group by and aggregation") {
      val graph = initGraph("CREATE (:Car{price:10,model:'BMW'}),(:Car{price:20,model:'BMW'}),(:Car{price:30,model:'VW'}), (:Car{price:10,model:'VW'})")
      val result = graph.cypher("""MATCH (n) CONSTRUCT NEW(x{prices:"collect(distinct n.price)",groupby:['n.model']}) RETURN GRAPH""")
      val records = result.getRecords

      records.collect should contain(CypherMap("x" -> MemNode(1, Set.empty, CypherMap("prices" -> List(10, 30)))))
      records.collect should contain(CypherMap("x" -> MemNode(0, Set.empty, CypherMap("prices" -> List(10, 20)))))
      /*result.getGraph.nodes("n").collect.length should be(2)
      result.getGraph.nodes("n").collect should contain(CypherMap("nodes" -> MemNode(2, Set.empty, CypherMap("prices" -> List(10, 30)))))*/
    }

    it("multiple node-constructs") {
      // union node tables
      val graph = initGraph("CREATE (:Car{price:10}),(:Car{price:20}),(:Person{age:2})")
      val result = graph.cypher("MATCH (n:Car),(m:Person) CONSTRUCT NEW (x{groupby:['n']}) NEW (y{groupby:['m']}) RETURN GRAPH")
      val records = result.getRecords

      records.collect should be(Array(CypherMap("x" -> MemNode(0), "y" -> MemNode(2)),
        CypherMap("x" -> MemNode(1), "y" -> MemNode(2))))

      /*result.getGraph.nodes("n").collect.distinct.length should be(3)*/
    }
  }

  describe("edge-construct") {
    it("with unbound edge-construct variables") {
      //automatisch gruppiert via n&m
      val graph = initGraph("CREATE (:filler)")
      val result = graph.cypher("CONSTRUCT NEW (a) NEW (b) NEW (a)-[:edge]->(b) RETURN GRAPH")

      result.getRecords.collect should be(Array(CypherMap("a" -> MemNode(0),
        "b" -> MemNode(1),
        "  FRESH_VAR33" -> MemRelationship(2, 0, 1, "edge"))))
      /*result.getGraph.nodes("n").collect.length should be(2)
      result.getGraph.relationships("n").collect.length should be(1)
      result.getGraph.relationships("e").collect should contain(CypherMap("e" -> MemRelationship(3, 1, 2, "edge", CypherMap.empty)))*/
    }

    it("with bound node-variables and unbound edge-variables") {
      val graph = initGraph("CREATE (a:Person)-[:likes]->(b:Car)-[:boughtby]->(a), (a)-[:owns]->(b)")
      val result = graph.cypher("MATCH (m)-->(n) CONSTRUCT Clone m,n NEW (m)-[e:edge]->(n) RETURN GRAPH")

      result.getRecords.collect should contain(CypherMap("m" -> MemNode(0, Set("Person")), "n" -> MemNode(1, Set("Car")),
        "e" -> MemRelationship(2, 0, 1, "edge")))
      result.getRecords.collect should contain(CypherMap("m" -> MemNode(1, Set("Car")), "n" -> MemNode(0, Set("Person")),
        "e" -> MemRelationship(3, 1, 0, "edge")))
      result.getRecords.collect should contain(CypherMap("m" -> MemNode(0, Set("Person")), "n" -> MemNode(1, Set("Car")),
        "e" -> MemRelationship(2, 0, 1, "edge")))

      //edges implicit grouped by source and target node (here m,n). thus 2 edges created (and 2 nodes copied)
      /* result.getGraph.nodes("n").collect.length should be(2)
       result.getGraph.relationships("n").collect.length should be(2)
       result.getGraph.relationships("e").collect should contain(CypherMap("e" -> MemRelationship(3, 1, 2, "edge", CypherMap.empty)))
       result.getGraph.relationships("e").collect should contain(CypherMap("e" -> MemRelationship(3, 2, 1, "edge", CypherMap.empty)))*/
    }

    it("with bound edge-construct variable with copying properties") {
      val graph = initGraph("CREATE (a:Person)-[:likes{since:2018}]->(b:Car)-[:boughtby{in:2017}]->(a), (a)-[:owns{for:1}]->(b)")
      val result = graph.cypher("MATCH (m)-[e]->(n) CONSTRUCT NEW(Copy of m) NEW(Copy of n) NEW (n)-[e]->(m) RETURN GRAPH")

      result.getRecords.collect should contain(CypherMap("m" -> MemNode(0, Set("Person")), "n" -> MemNode(1, Set("Car")),
        "e" -> MemRelationship(2, 0, 1, "likes", CypherMap("since" -> 2018))))
      result.getRecords.collect should contain(CypherMap("m" -> MemNode(1, Set("Car")), "n" -> MemNode(0, Set("Person")),
        "e" -> MemRelationship(3, 1, 0, "boughtby", CypherMap("in" -> 2017))))
      result.getRecords.collect should contain(CypherMap("m" -> MemNode(0, Set("Person")), "n" -> MemNode(1, Set("Car")),
        "e" -> MemRelationship(2, 0, 1, "owns", CypherMap("for" -> 1))))
      /*result.getGraph.relationships("e").collect should be(Array(CypherMap("edge" -> MemRelationship(3, 1, 2, "likes", CypherMap("since" -> 2018))),
        CypherMap("e" -> MemRelationship(4, 2, 1, "likes", CypherMap("in" -> 2017))),
        CypherMap("e" -> MemRelationship(5, 1, 2, "likes", CypherMap("for" -> 1)))))*/
    }

    it("with implicit cloning edge") {
      val graph = initGraph("CREATE (a:Person)-[:likes{since:2018}]->(b:Car)")
      val result = graph.cypher("MATCH (n)-[e:likes]->(m) CONSTRUCT NEW (n)-[e]->(m) NEW (x{groupby:1}) RETURN GRAPH")
      //check that ids are copied and new ids don't collide
      result.getRecords.collect should be(CypherMap("n" -> MemNode(0, Set("Person")), "m" -> MemNode(0, Set("Car")),
        "e" -> MemRelationship(2, 0, 1, "likes", CypherMap("since" -> 2018)), "x" -> MemNode(3)))
    }

    it("with bound construct variable with overwriting copied properties & type") {
      // construct (n),(m), (n)-[e:edge]->(m)
      val graph = initGraph("CREATE (a:Person)-[:likes{since:2018}]->(b:Car)-[:boughtby{in:2017}]->(a), (a)-[:owns{for:1}]->(b)")
      val result = graph.cypher("MATCH (n)-[e:likes]->(m) CONSTRUCT NEW(Copy of n) NEW(Copy of m) NEW (n)-[y Copy of e:sold{since:2022}]->(m) RETURN GRAPH")

      result.getRecords.collect should be(CypherMap("n" -> MemNode(0, Set("Person")), "m" -> MemNode(0, Set("Car")),
        "y" -> MemRelationship(2, 0, 1, "sold", CypherMap("since" -> 2022))))
      /*result.getGraph.nodes("n").collect.length should be(2)
      result.getGraph.relationships("n").collect.length should be(3)*/
    }

    //better example?
    it("with grouped edges") {
      val graph = initGraph(
        s"""|CREATE (a:Person),(b:Product),(a)-[:buys{amount:10, year:2010}]->(b),
            |(a)-[:buys{amount:10, year:2011}]->(b), (a)-[:buys{amount:10, year:2010}]->(b)""".stripMargin)
      val result = graph.cypher("MATCH (a)-[e]->(b) CONSTRUCT NEW (m{groupby:'a'})-[y:PurchaseYear{groupby:['e.year']}]->(n{groupby:'b'}) RETURN GRAPH")
      print(result.getRecords)
      result.getRecords.collect should be(Array(CypherMap("m" -> MemNode(0), "n" -> MemNode(1), "y" -> MemRelationship(2, 0, 1, "PurchaseYear")),
        CypherMap("m" -> MemNode(0), "n" -> MemNode(1), "y" -> MemRelationship(3, 0, 1, "PurchaseYear")),
        CypherMap("m" -> MemNode(0), "n" -> MemNode(1), "y" -> MemRelationship(2, 0, 1, "PurchaseYear"))))

      // 2 new nodes, 2 new edges
      /*result.getGraph.nodes("n").collect.length should be(2)
      result.getGraph.relationships("n").collect.length should be(2)
      result.getGraph.relationships("e").collect should be(Array(CypherMap("e" -> MemRelationship(3, 1, 2, "PurchaseYear", CypherMap.empty)),
        CypherMap("e" -> MemRelationship(4, 1, 2, "PurchaseYear", CypherMap.empty))))*/
    }

    it("with setting properties") {
      //construct (n),(m), (n)-[e{color:"blue",year:2018}]->(m)
      val graph = initGraph("Create (:filler)")
      val result = graph.cypher("""CONSTRUCT NEW (n),(m),(n)-[e:edge{color:"blue",year:2018}]->(m) RETURN GRAPH""")

      result.getRecords.collect should be(Array(CypherMap("n" -> MemNode(0), "m" -> MemNode(1), "e" -> MemRelationship(2, 0, 1, "edge", CypherMap("color" -> "blue", "year" -> 2018)))))
      /*result.getGraph.relationships("e").collect should contain(CypherMap("e" -> MemRelationship(3, 1, 2, "edge", CypherMap("color" -> "blue", "year" -> 2018))))*/
    }

    it("with setting aggregated properties") {
      val graph = initGraph(
        s"""|CREATE (a:Person),(b:Product),(a)-[:buys{amount:10, year:2010}]->(b),
            |(a)-[:buys{amount:10, year:2011}]->(b), (a)-[:buys{amount:10, year:2010}]->(b)""".stripMargin)
      val result = graph.cypher("""MATCH (m)-[e]->(n) CONSTRUCT NEW (m)-[:PurchaseYear{groupby:['e.year'], amount:"sum(e.amount)"}]->(n) RETURN GRAPH""")

      result.getRecords.collect should contain(CypherMap("m" -> MemNode(0), "n" -> MemNode(0),
        "e" -> MemRelationship(2, 0, 1, "PurchaseYear", CypherMap("amount" -> 20))))
      result.getRecords.collect should contain(CypherMap("m" -> MemNode(1), "n" -> MemNode(0),
        "e" -> MemRelationship(3, 0, 1, "PurchaseYear", CypherMap("amount" -> 10))))
      /* result.getGraph.relationships("e").collect should be(Array(CypherMap("e" -> MemRelationship(3, 1, 2, "PurchaseYear", CypherMap("amount" -> 20))),
         CypherMap("e" -> MemRelationship(4, 1, 2, "PurchaseYear", CypherMap("amount" -> 10)))))*/
    }

    it("multiple edge constructs") {
      val graph = initGraph("Create (:filler)")
      val result = graph.cypher("CONSTRUCT NEW (n),(m), (n)-[x:edge]->(m) , (m)-[y:edge]->(n) RETURN GRAPH")

      result.getRecords.collect should be(Array(CypherMap("n" -> MemNode(0), "m" -> MemNode(1), "x" -> MemRelationship(2, 0, 1, "edge"), "y" -> MemRelationship(3, 1, 0, "edge"))))

      /*result.getGraph.nodes("n").collect.distinct.length should be(2)
      result.getGraph.relationships("n").collect.distinct.length should be(2)*/
    }

  }

  describe("full-constructs") {
    val people_graph = initGraph("Create (:Person), (:Person)")
    val car_graph = initGraph("CREATE (:Car), (:Car)")
    val session = MemCypherSession.create
    session.store("people_graph", people_graph)
    session.store("car_graph", car_graph)

    it("with gid") {
      val result = session.cypher("CONSTRUCT ON people_graph, car_graph RETURN GRAPH")

      /* result.getGraph.nodes("n").collect.length should be(4)*/
      // check that ids are unique after unique
    }

    it("with nodes and gids") {
      val result = session.cypher("CONSTRUCT ON people_graph NEW (n) RETURN GRAPH")

      /*result.getGraph.nodes("n").collect.length should be(3)*/
    }

    it("with nodes, edges and gids") {
      val result = session.cypher("CONSTRUCT ON people_graph NEW (n),(m),(n)-[:edge]->(m) RETURN GRAPH")

      /*result.getGraph.nodes("n").collect.length should be(4)
      result.getGraph.relationships("n").collect.length should be(1)*/
    }


  }
}
