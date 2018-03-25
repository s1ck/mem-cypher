package org.opencypher.memcypher.matching

import org.opencypher.memcypher.MemCypherTestSuite

class MatchAcceptanceTest extends MemCypherTestSuite {

  it("should run a specific test") {
    val graph = initGraph("CREATE (a:A {value: 1})-[:KNOWS]->(b:B {value: 2})-[:FRIEND]->(c:C {value: 3})")

    val result = graph.cypher("MATCH (n)-->(a)-->(b) RETURN b")

    result.show
  }

}
