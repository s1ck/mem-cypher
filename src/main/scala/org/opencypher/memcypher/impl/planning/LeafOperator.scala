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
import org.opencypher.okapi.logical.impl.LogicalExternalGraph
import org.opencypher.okapi.relational.impl.table.RecordHeader
import org.opencypher.memcypher.impl.{MemPhysicalResult, MemRuntimeContext}

private [memcypher] abstract class LeafOperator extends MemOperator

final case class Start(records: MemRecords, graph: LogicalExternalGraph) extends LeafOperator {

  override val header: RecordHeader = records.header

  override def execute(implicit context: MemRuntimeContext): MemPhysicalResult =
    MemPhysicalResult(records, Map(graph.name -> resolve(graph.qualifiedGraphName)))
}

final case class StartFromUnit(graph: LogicalExternalGraph)(implicit session: MemCypherSession) extends LeafOperator {

  override def header: RecordHeader = RecordHeader.empty

  override def execute(implicit context: MemRuntimeContext): MemPhysicalResult =
    MemPhysicalResult(MemRecords.unit(), Map(graph.name -> resolve(graph.qualifiedGraphName)))
}
