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

import org.opencypher.okapi.api.graph.QualifiedGraphName
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.relational.api.physical.PhysicalOperator
import org.opencypher.okapi.trees.AbstractTreeNode
import org.opencypher.memcypher.api.MemCypherConverters._
import org.opencypher.memcypher.api.{MemCypherGraph, MemRecords}
import org.opencypher.memcypher.impl.{MemPhysicalResult, MemRuntimeContext}

abstract class MemOperator extends AbstractTreeNode[MemOperator]
  with PhysicalOperator[MemRecords, MemCypherGraph, MemRuntimeContext] {

  def execute(implicit context: MemRuntimeContext): MemPhysicalResult

  protected def resolve(qualifiedGraphName: QualifiedGraphName)(implicit context: MemRuntimeContext): MemCypherGraph = {
    context.resolve(qualifiedGraphName).map(_.asMemCypher).getOrElse(throw IllegalArgumentException(s"a graph at $qualifiedGraphName"))
  }
}

