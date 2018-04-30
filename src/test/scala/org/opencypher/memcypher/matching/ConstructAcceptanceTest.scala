package org.opencypher.memcypher.matching

import org.opencypher.memcypher.MemCypherTestSuite
import org.opencypher.memcypher.api.value.MemNode
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.ir.test.support.Bag

class ConstructAcceptanceTest extends MemCypherTestSuite {
  //TODO: fill with tests
  describe("node-constructs") {
    it("should run a specific test(filler)") {
      val graph = initGraph("CREATE (a:A {value: 1})-[:KNOWS]->(b:B {value: 2})-[:FRIEND]->(c:C {value: 3})")

      val result = graph.cypher("MATCH (n)-->(a)-->(b) RETURN b")

      result.getRecords.collect should be (Array(CypherMap("b"->MemNode(3,Set("C"),CypherMap("value"->3)))))
    }

    it("without unnamed construct-variable")
    {
      // Construct () MATCH .... --> should create one node for each match or only one node in total?

    }

    it("with unbound construct variable") {
      // Construct(n) MATCH .... --> in match-table columns like n_id

    }

    it("with bound construct variable without copying properties") {
      // Construct (m) Match (m)-->(n)  --> implicit group by (m); create one node for each distinct m node

    }

    it("with bound construct variable with copying properties") {
      // Construct CLONE(m) Match (m)-->(n)  --> implicit group by (m); copy m (including properties (except id?!))

    }

    it("with group by valid set of columns") {
      // Construct (x{groupby:[m,n"]}) Match (m)-->(n)

    }

    it("with group by invalid set of columns"){
      // Construct (x{groupby:[m,z]}) Match (m)-->(n) --> throw error, as z is not part of the match

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
  describe("edge-constructs") {

  }

  describe("full-constructs") {

  }
}
