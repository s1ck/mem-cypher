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
import org.opencypher.okapi.api.types.{CTNode, CTRelationship}
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.ir.api.expr.{Expr, Var}
import org.opencypher.okapi.relational.impl.table.{ColumnName, RecordHeader}

private [memcypher] abstract class UnaryOperator extends MemOperator {

  def in: MemOperator

  override def execute(implicit context: MemRuntimeContext): MemPhysicalResult = executeUnary(in.execute)

  def executeUnary(prev: MemPhysicalResult)(implicit context: MemRuntimeContext): MemPhysicalResult
}

final case class Scan(in: MemOperator, v: Var, header: RecordHeader) extends UnaryOperator {

  override def executeUnary(prev: MemPhysicalResult)(implicit context: MemRuntimeContext): MemPhysicalResult = {
    val graph = prev.workingGraph
    val records = v.cypherType match {
      case r: CTRelationship =>
        graph.relationships(v.name, r)
      case n: CTNode =>
        graph.nodes(v.name, n)
      case x =>
        throw IllegalArgumentException("an entity type", x)
    }
    assert(header == records.header)
    MemPhysicalResult(records, graph, prev.workingGraphName)
  }
}

final case class Alias(in: MemOperator, expr: Expr, alias: Var, header: RecordHeader) extends UnaryOperator {

  override def executeUnary(prev: MemPhysicalResult)(implicit context: MemRuntimeContext): MemPhysicalResult = {
    logger.info(s"Projecting $expr to alias var: $alias")
    val data = prev.records.data
    val newData = data.project(expr, ColumnName.of(header.slotFor(alias)))(header, context)
    MemPhysicalResult(MemRecords.create(newData, header), prev.workingGraph, prev.workingGraphName)
  }
}

final case class SelectFields(in: MemOperator, fields: Seq[Var], header: RecordHeader) extends UnaryOperator {

  override def executeUnary(prev: MemPhysicalResult)(implicit context: MemRuntimeContext): MemPhysicalResult = {
    logger.info(s"Selecting fields: ${fields.mkString(",")}")
    val columnNames = fields.map(header.slotFor).map(ColumnName.of)
    val newData = prev.records.data.select(columnNames)(header, context)
    MemPhysicalResult(MemRecords.create(newData, header), prev.workingGraph, prev.workingGraphName)
  }
}

case class SelectGraphs(in: MemOperator, graphs: Set[String], header: RecordHeader) extends UnaryOperator {

  override def executeUnary(prev: MemPhysicalResult)(implicit context: MemRuntimeContext): MemPhysicalResult =
    prev
}

case class Project(in: MemOperator, expr: Expr, header: RecordHeader) extends UnaryOperator {

  override def executeUnary(prev: MemPhysicalResult)(implicit context: MemRuntimeContext): MemPhysicalResult = {
    val headerNames = header.slotsFor(expr).map(ColumnName.of)
    val dataNames = prev.records.data.columns.toSeq
    val data = prev.records.data

    val newData = headerNames.diff(dataNames) match {
      case Seq(one) =>
        logger.info(s"Projecting $expr to key $one")
        data.project(expr, one)(header, context)
    }

    MemPhysicalResult(MemRecords.create(newData, header), prev.workingGraph, prev.workingGraphName)
  }
}

case class Filter(in: MemOperator, expr: Expr, header: RecordHeader) extends UnaryOperator {

  override def executeUnary(prev: MemPhysicalResult)(implicit context: MemRuntimeContext): MemPhysicalResult = {
    logger.info(s"Filtering based on predicate: $expr")
    val newData = prev.records.data.filter(expr)(header, context)
    MemPhysicalResult(MemRecords.create(newData, header), prev.workingGraph, prev.workingGraphName)
  }
}

//TODO: Test functionality
case class Distinct(in: MemOperator, fields: Set[Var]) extends UnaryOperator  with InheritedHeader {

  override def executeUnary(prev: MemPhysicalResult)(implicit context: MemRuntimeContext): MemPhysicalResult = {
    logger.info(s"Distinct $fields")
    val data = prev.records.data
    val columnNames = for{ x <- fields} yield x.name
    val newData = data.distinct(columnNames.toSeq)(header,context)

    MemPhysicalResult(MemRecords.create(newData, header),prev.graphs)
  }
}

