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

import org.opencypher.okapi.relational.api.physical.PhysicalResult
import org.opencypher.memcypher.api.{MemCypherGraph, MemRecords}
import org.opencypher.okapi.api.graph.QualifiedGraphName

case class MemPhysicalResult(
  records: MemRecords,
  workingGraph: MemCypherGraph,
  workingGraphName: QualifiedGraphName)
  extends PhysicalResult[MemRecords, MemCypherGraph] {

  /**
    * Performs the given function on the underlying records and returns the updated records.
    *
    * @param f map function
    * @return updated result
    */
  override def mapRecordsWithDetails(f: MemRecords => MemRecords): MemPhysicalResult =
    copy(records = f(records))
}
