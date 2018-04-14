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
package org.opencypher.memcypher.impl.planning

import org.opencypher.memcypher.api.{MemCypherSession, MemRecords}
import org.opencypher.memcypher.impl.{MemPhysicalResult, MemRuntimeContext}
import org.opencypher.okapi.api.graph.QualifiedGraphName
import org.opencypher.okapi.relational.impl.table.RecordHeader

private [memcypher] abstract class LeafOperator extends MemOperator

final case class Start(qgn: QualifiedGraphName, recordsOpt: Option[MemRecords])(implicit memCypher: MemCypherSession)
  extends LeafOperator {

  override val header: RecordHeader = recordsOpt.map(_.header).getOrElse(RecordHeader.empty)

  override def execute(implicit context: MemRuntimeContext): MemPhysicalResult = {
    val records = recordsOpt.getOrElse(MemRecords.unit())
    MemPhysicalResult(records, resolve(qgn), qgn)
  }
}
