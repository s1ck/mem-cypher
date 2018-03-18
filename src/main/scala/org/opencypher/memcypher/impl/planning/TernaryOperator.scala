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

import org.opencypher.memcypher.api.MemRecords
import org.opencypher.memcypher.impl.{MemPhysicalResult, MemRuntimeContext}
import org.opencypher.okapi.api.types.CTInteger
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.relational.impl.table.RecordHeader

private [memcypher] abstract class TernaryOperator extends MemOperator {
  def first: MemOperator

  def second: MemOperator

  def third: MemOperator

  override def execute(implicit context: MemRuntimeContext): MemPhysicalResult =
    executeTernary(first.execute, second.execute, third.execute)

  def executeTernary(
    first: MemPhysicalResult,
    second: MemPhysicalResult,
    third: MemPhysicalResult)(implicit context: MemRuntimeContext): MemPhysicalResult
}

final case class ExpandSource(
  first: MemOperator,
  second: MemOperator,
  third: MemOperator,
  source: Var,
  rel: Var,
  target: Var,
  header: RecordHeader,
  removeSelfRelationships: Boolean) extends TernaryOperator {

  override def executeTernary(
    first: MemPhysicalResult,
    second: MemPhysicalResult,
    third: MemPhysicalResult)(implicit context: MemRuntimeContext): MemPhysicalResult = {
    logger.info(s"Expand from $source via $rel to $target")

    implicit val h: RecordHeader = header

    val newData = first.records.data
      .join(second.records.data, Id(source)(CTInteger), StartNode(rel)(CTInteger))
      .join(third.records.data, EndNode(rel)(CTInteger), Id(target)(CTInteger))

    MemPhysicalResult(MemRecords(newData, header), first.graphs)
  }
}
