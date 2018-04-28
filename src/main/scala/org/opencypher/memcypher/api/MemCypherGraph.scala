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
package org.opencypher.memcypher.api

import org.opencypher.memcypher.api
import org.opencypher.memcypher.api.value.{MemNode, MemRelationship}
import org.opencypher.okapi.api.graph.PropertyGraph
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types.CypherType._
import org.opencypher.okapi.api.types.{CTNode, CTRelationship}
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.ir.api.expr.Var
import org.opencypher.okapi.relational.impl.table.RecordHeader
object MemCypherGraph {
  def empty(implicit session: MemCypherSession): MemCypherGraph = MemCypherGraph(Seq.empty, Seq.empty)(session)

  def create(nodes: Seq[MemNode], rels: Seq[MemRelationship])(implicit session: MemCypherSession): MemCypherGraph = {
    api.MemCypherGraph(nodes, rels)
  }
}

case class MemCypherGraph(
  nodes: Seq[MemNode],
  rels: Seq[MemRelationship])
  (implicit memSession: MemCypherSession) extends PropertyGraph {

  type CypherSession = MemCypherSession

  private lazy val labelNodeMap: Map[Set[String], Seq[MemNode]] = nodes.groupBy(_.labels)

  private lazy val typeRelMap: Map[String, Seq[MemRelationship]] = rels.groupBy(_.relType)

  private def schemaForNodes(nodes: Seq[MemNode], initialSchema: Schema = Schema.empty): Schema =
    nodes.foldLeft(initialSchema) {
      case (tmpSchema, node) =>
        val properties = node.properties.value.map { case (key, value) => key -> value.cypherType }
        tmpSchema.withNodePropertyKeys(node.labels, properties)
    }

  private def schemaForRels(rels: Seq[MemRelationship], initialSchema: Schema = Schema.empty): Schema =
    rels.foldLeft(initialSchema) {
      case (tmpSchema, rel) =>
        val properties = rel.properties.value.map { case (key, value) => key -> value.cypherType }
        tmpSchema.withRelationshipPropertyKeys(rel.relType, properties)
    }

  /**
    * The schema that describes this graph.
    *
    * @return the schema of this graph.
    */
  override def schema: Schema = schemaForRels(rels, initialSchema = schemaForNodes(nodes))


  /**
    * The session in which this graph is managed.
    *
    * @return the session of this graph.
    */
  override def session: CypherSession = memSession

  /**
    * Constructs a scan table of all the nodes in this graph with the given cypher type.
    *
    * @param name the field name for the returned nodes.
    * @return a table of nodes of the specified type.
    */
  override def nodes(name: String, nodeCypherType: CTNode): MemRecords = {
    val node = Var(name)(nodeCypherType)
    val filteredNodes = if (nodeCypherType.labels.isEmpty) {
      nodes
    } else {
      labelNodeMap.filterKeys(nodeCypherType.labels.subsetOf).values.reduce(_ ++ _)
    }
    val filteredSchema = schemaForNodes(filteredNodes)
    val targetNodeHeader = RecordHeader.nodeFromSchema(node, filteredSchema)
    MemRecords.create(filteredNodes.map(node => CypherMap(name -> node)).toList, targetNodeHeader)
  }

  /**
    * Constructs a scan table of all the relationships in this graph with the given cypher type.
    *
    * @param name the field name for the returned relationships.
    * @return a table of relationships of the specified type.
    */
  override def relationships(name: String, relCypherType: CTRelationship): MemRecords = {
    val rel = Var(name)(relCypherType)
    val filteredRels = if (relCypherType.types.isEmpty) rels else typeRelMap.filterKeys(relCypherType.types.contains).values.flatten.toSeq
    val filteredSchema = schemaForRels(filteredRels)
    val targetHeader = RecordHeader.relationshipFromSchema(rel, filteredSchema)
    MemRecords.create(filteredRels.map(rel => CypherMap(name -> rel)).toList, targetHeader)
  }

  override def unionAll(others: PropertyGraph*): PropertyGraph = ???
}
