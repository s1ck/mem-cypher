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
package org.opencypher.memcypher.impl

import org.opencypher.okapi.api.graph.QualifiedGraphName
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.relational.api.physical.RuntimeContext
import org.opencypher.memcypher.api.{MemCypherGraph, MemRecords}

object MemRuntimeContext {
  val empty = MemRuntimeContext(CypherMap.empty, _ => None)
}

case class MemRuntimeContext(
  parameters: CypherMap,
  resolve: QualifiedGraphName => Option[MemCypherGraph]
) extends RuntimeContext[MemRecords, MemCypherGraph]


