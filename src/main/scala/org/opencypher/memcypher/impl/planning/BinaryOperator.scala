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
import org.opencypher.okapi.impl.exception.NotImplementedException
import org.opencypher.okapi.ir.api.expr.{Expr, Id, Var}
import org.opencypher.okapi.relational.impl.physical.JoinType
import org.opencypher.okapi.relational.impl.table.RecordHeader

private [memcypher] abstract class BinaryOperator extends MemOperator {

  def left: MemOperator

  def right: MemOperator

  override def execute(implicit context: MemRuntimeContext): MemPhysicalResult =
    executeBinary(left.execute, right.execute)

  def executeBinary(left: MemPhysicalResult, right: MemPhysicalResult)(implicit context: MemRuntimeContext): MemPhysicalResult
}

final case class Join(
  left: MemOperator,
  right: MemOperator,
  joinExprs: Seq[(Expr, Expr)],
  header: RecordHeader,
  joinType: JoinType) extends BinaryOperator {

  override def executeBinary(left: MemPhysicalResult, right: MemPhysicalResult)(implicit context: MemRuntimeContext): MemPhysicalResult = {
    if (joinExprs.length > 1) throw NotImplementedException("Multi-way join support")

    val (leftExpr, rightExpr) = joinExprs.head
    val leftId = leftExpr match {
      case v:Var => Id(v)(CTInteger)
      case other => other
    }
    val rightId = rightExpr match {
      case v:Var => Id(v)(CTInteger)
      case other => other
    }

    val newData = left.records.data.hashJoin(right.records.data, leftId, rightId)(header, context)
    MemPhysicalResult(MemRecords(newData, header), left.workingGraph, left.workingGraphName)
  }

}
