package org.opencypher.memcypher.matching

import org.opencypher.memcypher.MemCypherTestSuite
import org.opencypher.memcypher.api.value.MemNode
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.ir.test.support.Bag

class ConstructAcceptanceTest extends MemCypherTestSuite {
  //TODO: fill with tests; NEW OR CLONE before each object construct?!
  describe("node-constructs") {
    it("should run a specific test(filler)") {
      val graph = initGraph("CREATE (a:A {value: 1})-[:KNOWS]->(b:B {value: 2})-[:FRIEND]->(c:C {value: 3})")

      val result = graph.cypher("MATCH (n)-->(a)-->(b) RETURN b")

      result.getRecords.collect should be (Array(CypherMap("b"->MemNode(3,Set("C"),CypherMap("value"->3)))))
    }

    it("without unnamed construct-variable")
    {
      // Construct NEW() MATCH .... --> should create one node for each match or only one node in total?

    }

    it("with unbound construct variable") {
      // Construct NEW(n) MATCH .... --> in match-table columns like n_id

    }

    it("with bound construct variable without copying properties") {
      // Construct NEW(m) Match (m)-->(n)  --> implicit group by (m); create one node for each distinct m node
      // todo: solved via CLONE ?!

    }

    it("with bound construct variable with copying properties") {
      // Match (m)-->(n) Construct CLONE(m)  --> implicit group by (m); copy m (including properties (except id?!))

    }

    it("with group by valid set of columns") {
      // Construct NEW(x{groupby:['m','n']}) Match (m)-->(n)

    }

    it("with group by invalid set of variables"){
      // Construct (x{groupby:['m','z']}) Match (m)-->(n) --> throw error, as z is not part of the match

    }

    it("with setting properties") {
      //Construct ({color:"blue",price:1000})
    }
    it("with setting one label") {
      // construct (:wow)
    }

    it("with setting multiple labels") {
      // construct (:wow:amazing)
    }

    it("with aggregated properties") {
      // construct ({minimum:min(n.price)) Match (n) ?how many nodes should be created here?
    }

    it("with group by and aggregation") {
      // construct (x{count(distinct n.price),groupby:[m,n]}
    }

    it("multiple node-constructs") {
      // union node tables ?!
      // construct (n),(m) match (n)-->(m)

    }
  }
  describe("edge-construct") {
    it("with unbound constructed variable") {
      //construct (n),(m), (n)-->(m) --> automatisch gruppiert via n&m
    }

    it("with bound construct variable without copying properties") {
      // construct (n),(m), (n)-[e:edge]->(m)
    }

    it("with bound edge-construct variable with copying properties") {
      // construct clone e (n),(m),  (n)-[e]->(m)
    }

    it("with grouped edges") {
      // construct (n),(m), (n)-[e{groupby:[x,y]}]->(m)
    }

    it("with setting properties") {
      //construct (n),(m), (n)-[e{color:"blue",year:2018}]->(m)
    }

    it("with setting aggregated properties") {
      //construct (n),(m), (n)-[e{avg_time:avg(t.time)}]->(m)
    }

    it("with invalid nodes") {
      //Construct (n)-->(m) ; Construct (n),(n)-->(m) Match (n) TODO: gets accepted by opencypher (n) & (m) created than

    }

    it("multiple edge constructs") {
      //construct (n), (m),(n)<--(m),(m)-->(n)
    }

  }

  describe("full-constructs") {
    it("with gids") {
      // construct social_graph,test_graph
    }

    it("with nodes and gids") {
      // construct social_graph, (n) match (n)
    }

    it("with nodes, edges and gids") {
      // rearrange nodes before edge constructs

    }
  }
}
