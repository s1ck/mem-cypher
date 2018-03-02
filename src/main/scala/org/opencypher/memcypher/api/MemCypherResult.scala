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

import org.opencypher.okapi.api.graph.{CypherQueryPlans, CypherResult, PropertyGraph}
import org.opencypher.okapi.api.table.CypherRecords
import org.opencypher.okapi.impl.util.PrintOptions
import org.opencypher.okapi.logical.impl.LogicalOperator
import org.opencypher.okapi.relational.impl.flat.FlatOperator
import org.opencypher.okapi.trees.TreeNode
import org.opencypher.memcypher.impl.MemRuntimeContext
import org.opencypher.memcypher.impl.planning.MemOperator

case class MemCypherResult(
  graph: Option[PropertyGraph],
  records: Option[CypherRecords],
  plans: CypherQueryPlans) extends CypherResult {

  override def show(implicit options: PrintOptions): Unit =
    records match {
      case Some(r) => r.show
      case None => options.stream.print("No results")
    }
}

object MemCypherResultBuilder {

  def from(
    logical: LogicalOperator,
    flat: FlatOperator,
    physical: MemOperator)(implicit context: MemRuntimeContext): MemCypherResult = {
    lazy val result = physical.execute
    MemCypherResult(result.graphs.values.headOption, Some(result.records), QueryPlans(logical, flat, physical))
  }
}

case class QueryPlans(
  logicalPlan: TreeNode[LogicalOperator],
  flatPlan: TreeNode[FlatOperator],
  physicalPlan: TreeNode[MemOperator]) extends CypherQueryPlans {

  override def logical: String = logicalPlan.pretty

  override def physical: String = physicalPlan.pretty
}
