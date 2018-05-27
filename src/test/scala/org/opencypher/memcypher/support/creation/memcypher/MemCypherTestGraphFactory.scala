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
package org.opencypher.memcypher.support.creation.memcypher

import org.opencypher.memcypher.api.value.{MemNode, MemRelationship}
import org.opencypher.memcypher.api.{MemCypherGraph, MemCypherSession}
import org.opencypher.okapi.api.graph.PropertyGraph
import org.opencypher.okapi.testing.propertygraph.{CypherTestGraphFactory, InMemoryTestGraph}

object MemCypherTestGraphFactory extends CypherTestGraphFactory[MemCypherSession] {

  override def apply(propertyGraph: InMemoryTestGraph)(implicit caps: MemCypherSession): PropertyGraph = {
    val nodes = propertyGraph.nodes.map(n => MemNode(n.id, n.labels, n.properties))
    val rels = propertyGraph.relationships.map(r => MemRelationship(r.id, r.source, r.target, r.relType, r.properties))

    MemCypherGraph.create(nodes, rels)
  }

  override def name: String = "MemTestGraphFactory"
}
