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
package org.opencypher.memcypher.api.value

import org.opencypher.okapi.api.value.CypherValue.{CypherMap, CypherNode, CypherRelationship}

case class MemNode(
  override val id: Long,
  override val labels: Set[String] = Set.empty,
  override val properties: CypherMap = CypherMap.empty) extends CypherNode[Long]{

  type I = MemNode

  override def copy(id: Long = id, labels: Set[String] = labels, properties: CypherMap = properties): MemNode = {
    MemNode(id, labels, properties).asInstanceOf[this.type]
  }
}

case class MemRelationship(
  override val id: Long,
  override val source: Long,
  override val target: Long,
  override val relType: String,
  override val properties: CypherMap = CypherMap.empty) extends CypherRelationship[Long] {

  type I = MemRelationship

  override def copy(id: Long = id, source: Long = source, target: Long = target, relType: String = relType, properties: CypherMap = properties): MemRelationship = {
    MemRelationship(id, source, target, relType, properties).asInstanceOf[this.type]
  }

}
