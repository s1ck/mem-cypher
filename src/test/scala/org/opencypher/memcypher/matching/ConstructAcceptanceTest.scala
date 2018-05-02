package org.opencypher.memcypher.matching

import org.opencypher.memcypher.MemCypherTestSuite
import org.opencypher.memcypher.api.{MemCypherSession, MemRecords}
import org.opencypher.memcypher.api.value.{MemNode, MemRelationship}
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.ir.test.support.Bag

class ConstructAcceptanceTest extends MemCypherTestSuite {

  describe("node-constructs") {
    /*queries with "return graph"? ; first idea was to return a node and an edge table/record
      groupby property saved in object or just extracted?
    TODO: copy of ... not covered yet; check if ids are unique / better checks(more than length)*/
    it("without unnamed construct-variable")
    {
      val graph = initGraph("CREATE (:Person), (:Car)")
      val result = graph.cypher("CONSTRUCT NEW() RETURN GRAPH")

      //should create one node
      result.getRecords.collect.length should be (1)
      result.getRecords.collect should be (Array(CypherMap("nodes"->MemNode(1,Set(""),CypherMap("color"->"blue","price"->1000)))))
    }

    it("with unbound construct variable") {
     val graph = initGraph("CREATE (:PERSON),(:CAR)")
      val result = graph.cypher("Match (i) CONSTRUCT NEW(n) RETURN GRAPH")

      //result should contain one new node
      result.getRecords.collect.length should be (1)
    }

    it("with bound construct variable without copying properties") {
      // Construct NEW(m) Match (m)-->(n)  --> implicit group by (m); create one node for each distinct m node
      val graph = initGraph("CREATE (:PERSON),(:CAR)")
      val result = graph.cypher("Match (n) CONSTRUCT NEW(n) RETURN GRAPH")

      //result should contain 2 new nodes (implicit group by n)
      result.getRecords.collect.length should be (2)
    }

    it("with bound construct variable with copying properties and labels") {
      // Match (m)-->(n) Construct CLONE(m)  --> implicit group by (m); copy m (including properties (except id?!))
      val graph = initGraph("""CREATE (:Person{age:10}),(:Car{color:"blue"})""")
      val result = graph.cypher("MATCH (n) CONSTRUCT CLONE n RETURN GRAPH")

      //result should contain same nodes as "graph"
      result.getRecords.collect.toSet should be (graph.nodes.toSet)
    }

    it("with group by valid set of columns") {
      // Construct NEW(x{groupby:['m','n']}) Match (m)-->(n)
      val graph = initGraph("CREATE (a:Person)-[:likes]->(b:Car)-[:boughtby]->(a), (a)-[:owns]->(b)")
      val result = graph.cypher("MATCH (n)-->(m) CONSTRUCT NEW(x{groupby:['m','n']}) RETURN GRAPH")

      //result should construct 2 new nodes (one for (a,b) and another one for (b,a))
      result.getRecords.collect.length should be (2)
    }

    it("with group by invalid set of variables"){
      val graph = initGraph("CREATE (a:Person)-[:likes]->(b:Car)-[:boughtby]->(a), (a)-[:owns]->(b)")

      val thrown = intercept[Exception] {
        graph.cypher("MATCH (n)-->(m) CONSTRUCT NEW(x{groupby:['m','z']}) RETURN GRAPH")
      }
      // throw error, as grouping variable z is not part of the match
      thrown.getMessage should be ("invalid set of variables")
    }

    it("with setting properties") {
      //Construct ({color:"blue",price:1000})
      val graph = initGraph("CREATE (:Person),(:Car)")
      val result = graph.cypher("""CONSTRUCT NEW ({color:"blue",price:1000}) RETURN GRAPH""")

      //result contains one node with properties
      result.getRecords.collect should be (Array(CypherMap("nodes"->MemNode(1,Set(""),CypherMap("color"->"blue","price"->1000)))))
    }
    it("with setting one label") {
      val graph = initGraph("CREATE (:Person),(:Car)")
      val result = graph.cypher("CONSTRUCT NEW (:Person) RETURN GRAPH")

      //check label of node
      result.getRecords.collect should be (Array(CypherMap("nodes"->MemNode(1,Set("Person")))))
    }

    it("with setting multiple labels") {
      val graph = initGraph("CREATE (:Person),(:Car)")
      val result = graph.cypher("CONSTRUCT NEW (:Person:Actor) RETURN GRAPH")

      // check labels of node
      result.getRecords.collect should be (Array(CypherMap("nodes"->MemNode(1,Set("Person","Actor")))))
    }

    it("with aggregated properties") {
      //max(n.age) must be a string (otherwise syntaxexception from cypher)
      val graph = initGraph("CREATE ({age:10}),({age:12}),({age:14})")
      val result = graph.cypher("""MATCH (n) CONSTRUCT NEW({max_age:"max(n.age)"}) RETURN GRAPH""")

      // node with property max_age:14
      result.getRecords.collect should be (Array(CypherMap("nodes"->MemNode(1,Set(""),CypherMap("max_age"->14)))))
    }

    it("with group by and aggregation") {
      val graph = initGraph("CREATE (:Car{price:10}),(:Car{price:20}),(:Car{price:30}), (:Car{price:10})")
      val result = graph.cypher("""MATCH (m) CONSTRUCT NEW({prices:"collect(distinct n.price)",groupby:['m']}) RETURN GRAPH""")

      // for each m from match clause construct one node
      result.getRecords.collect.length should be (4)
      result.getRecords.collect should contain (CypherMap("nodes"->MemNode(1,Set(""),CypherMap("prices"->List(10,20,30)))))
    }

    it("multiple node-constructs") {
      // union node tables
      val graph = initGraph("CREATE (:Car{price:10}),(:Car{price:20}),(:Person{age:2})")
      val result = graph.cypher("MATCH (n:Car),(m:Person) CONSTRUCT CLONE (n) NEW (m) RETURN GRAPH")

      // in total 3 nodes ; unique ids (distinct should reduce number of nodes otherwise)
      result.getRecords.collect.distinct.length should be (3)
    }
  }
  describe("edge-construct") {
    it("with unbound edge-construct variables") {
      //construct (n),(m), (n)-->(m) --> automatisch gruppiert via n&m
      val graph = initGraph("CREATE (:filler)")
      val result = graph.cypher("CONSTRUCT NEW (a) NEW (b) NEW (a)-[:edge]->(b) RETURN GRAPH")

      // 2 nodes and 1 relationship created
      result.getRecords.collect.length should be (3)
      result.getRecords.collect should contain (CypherMap("edge"->MemRelationship(3,1,2,"edge",CypherMap.empty)))
    }

    it("with bound node-variables and unbound edge-variables") {
      val graph = initGraph("CREATE (a:Person)-[:likes]->(b:Car)-[:boughtby]->(a), (a)-[:owns]->(b)")
      val result = graph.cypher("MATCH (m)-->(n) CONSTRUCT CLONE (m) CLONE (n) NEW (m)-[:edge]->(n) RETURN GRAPH")

      //edges implicit grouped by source and target node (here m,n). thus 2 edges created (and 2 nodes copied)
      result.getRecords.collect.length should be (4)
      result.getRecords.collect should contain (CypherMap("edge"->MemRelationship(3,1,2,"edge",CypherMap.empty)))
      result.getRecords.collect should contain (CypherMap("edge"->MemRelationship(3,2,1,"edge",CypherMap.empty)))
      // better check?
    }

    it("with bound construct variable without copying properties") {
      // construct (n),(m), (n)-[e:edge]->(m)
      val graph = initGraph("CREATE (a:Person)-[:likes{since:2018}]->(b:Car)-[:boughtby{in:2017}]->(a), (a)-[:owns{for:1}]->(b)")
      val result = graph.cypher("MATCH (n)-[e]->(m) CONSTRUCT CLONE (n),(m) NEW (n)-[e]->(m) RETURN GRAPH")

      result.getRecords.collect.length should be (4)
      //check that edge properties aren't copied
    }

    it("with bound edge-construct variable with copying properties") {
      // construct clone e (n),(m),  (n)-[e]->(m)
      val graph = initGraph("CREATE (a:Person)-[:likes{since:2018}]->(b:Car)-[:boughtby{in:2017}]->(a), (a)-[:owns{for:1}]->(b)")
      val result = graph.cypher("MATCH (m)-[e]->(n) CONSTRUCT CLONE (m),(n),e NEW (n)-[e]->(m) RETURN GRAPH")

      //check that edge prop. are copied
      result.getRecords.collect should contain (CypherMap("edge"->MemRelationship(3,1,2,"likes",CypherMap("since"->2018))))
      result.getRecords.collect should contain (CypherMap("edge"->MemRelationship(4,2,1,"likes",CypherMap("in"->2017))))
      result.getRecords.collect should contain (CypherMap("edge"->MemRelationship(5,1,2,"likes",CypherMap("for"->1))))
    }

    //find better example
    it("with grouped edges") {
      val graph = initGraph(
        s"""|CREATE (a:Person),(b:Product),(a)-[:buys{amount:10, year:2010}]->(b),
            |(a)-[:buys{amount:10, year:2011}]->(b), (a)-[:buys{amount:10, year:2010}]->(b)""".stripMargin)
      val result = graph.cypher("MATCH (m)-[e]->(n) CONSTRUCT NEW (m)-[:PurchaseYear{groupby:['e.year']}]->(n) RETURN GRAPH")

      // 2 new nodes, 2 new edges
      result.getRecords.collect.length should be (4)
      result.getRecords.collect should contain (CypherMap("edge"->MemRelationship(3,1,2,"PurchaseYear",CypherMap.empty)))
      result.getRecords.collect should contain (CypherMap("edge"->MemRelationship(4,1,2,"PurchaseYear",CypherMap.empty)))
    }

    it("with setting properties") {
      //construct (n),(m), (n)-[e{color:"blue",year:2018}]->(m)
      val graph = initGraph("Create (:filler)")
      val result = graph.cypher("""CONSTRUCT NEW (n),(m),(n)-[:edge{color:"blue",year:2018}]->(m) RETURN GRAPH""")

      result.getRecords.collect should contain (CypherMap("edge"->MemRelationship(3,1,2,"edge",CypherMap("color"->"blue","year"->2018))))
    }

    it("with setting aggregated properties") {
      val graph = initGraph(
        s"""|CREATE (a:Person),(b:Product),(a)-[:buys{amount:10, year:2010}]->(b),
            |(a)-[:buys{amount:10, year:2011}]->(b), (a)-[:buys{amount:10, year:2010}]->(b)""".stripMargin)
      val result = graph.cypher("""MATCH (m)-[e]->(n) CONSTRUCT NEW (m)-[:PurchaseYear{groupby:['e.year'], amount:"sum(e.amount)"}]->(n) RETURN GRAPH""")

      result.getRecords.collect should contain (CypherMap("edge"->MemRelationship(3,1,2,"PurchaseYear",CypherMap("amount"->20))))
      result.getRecords.collect should contain (CypherMap("edge"->MemRelationship(4,1,2,"PurchaseYear",CypherMap("amount"->10))))
    }

    /*it("with invalid nodes") {
      //Match (n) Construct (n)-->(m) or Construct (n),(n)-->(m); gets accepted by opencypher (n) & (m) created than
      val graph = initGraph("")
      val result = graph.cypher("")
    }*/

    it("multiple edge constructs") {
      //construct (n), (m),(n)<--(m),(m)-->(n)
      val graph = initGraph("Create (:filler)")
      val result = graph.cypher("CONSTRUCT NEW (n),(m), (n)-[:edge]->(m) , (m)-[:edge]->(n) RETURN GRAPH")

      result.getRecords.collect.distinct.length should be (4)
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

      result.getRecords.collect.length should be (4)
      // check that ids are unique after unique
    }

    it("with nodes and gids") {
      val result = session.cypher("CONSTRUCT ON people_graph NEW (n) RETURN GRAPH")

      result.getRecords.collect.length should be (3)
    }

    it("with nodes, edges and gids") {
      val result = session.cypher("CONSTRUCT ON people_graph NEW (n),(m),(n)-[:edge]->(m) RETURN GRAPH")

      result.getRecords.collect.length should be (5)
    }


  }
}
