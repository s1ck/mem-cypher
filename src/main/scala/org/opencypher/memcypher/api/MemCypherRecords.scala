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

import org.opencypher.memcypher.impl.MemRuntimeContext
import org.opencypher.memcypher.impl.value.CypherMapOps._
import org.opencypher.okapi.api.table.{CypherRecords, CypherRecordsCompanion}
import org.opencypher.okapi.api.types.CypherType
import org.opencypher.okapi.api.types.CypherType._
import org.opencypher.okapi.api.value.CypherValue.{CypherMap, CypherValue}
import org.opencypher.okapi.impl.table.RecordsPrinter
import org.opencypher.okapi.impl.util.PrintOptions
import org.opencypher.okapi.ir.api.expr.Expr
import org.opencypher.okapi.relational.impl.table.{ColumnName, RecordHeader}

object MemRecords extends CypherRecordsCompanion[MemRecords, MemCypherSession] {

  def create(rows: List[CypherMap], header: RecordHeader): MemRecords = MemRecords(Embeddings(rows), header)

  def create(embeddings: Embeddings, header: RecordHeader): MemRecords = MemRecords(embeddings, header)

  override def unit()(implicit session: MemCypherSession): MemRecords = MemRecords(Embeddings.empty, RecordHeader.empty)
}

case class MemRecords(
  data: Embeddings,
  header: RecordHeader) extends CypherRecords {

  override def rows: Iterator[String => CypherValue] = data.rows.map(_.value)

  override def columns: Seq[String] = header.fieldsInOrder

  override def columnType: Map[String, CypherType] = data.data.headOption match {
    case Some(row) => row.value.mapValues(_.cypherType)
    case None => Map.empty
  }

  override def iterator: Iterator[CypherMap] = data.rows

  override def size: Long = rows.size

  override def show(implicit options: PrintOptions): Unit = RecordsPrinter.print(this)

  override def collect: Array[CypherMap] = iterator.toArray
}


object Embeddings {

  def empty: Embeddings = Embeddings(List.empty)
}

case class Embeddings(data: List[CypherMap]) {

  def columns: Set[String] = data.headOption match {
    case Some(row) => row.keys
    case None => Set.empty
  }

  def rows: Iterator[CypherMap] = data.iterator

  def project(expr: Expr, toKey: String)(implicit header: RecordHeader, context: MemRuntimeContext): Embeddings =
    copy(data = data.map(row => row.updated(toKey, row.evaluate(expr))))

  def filter(expr: Expr)(implicit header: RecordHeader, context: MemRuntimeContext): Embeddings =
    copy(data = data.filter(row => row.evaluate(expr).as[Boolean].getOrElse(false)))

  def select(fields: Seq[String])(implicit header: RecordHeader, context: MemRuntimeContext): Embeddings =
    copy(data = data.map(row => row.filterKeys(fields)))

  // O(n * m), where n = |left| and m = |right|
  def loopJoin(other: Embeddings, left: Expr, right: Expr)(implicit header: RecordHeader, context: MemRuntimeContext): Embeddings = {
    val newData = rows.flatMap(leftRow => {
      val leftVal = leftRow.evaluate(left)
      other.rows
        .filter(rightRow => leftVal == rightRow.evaluate(right))
        .map(rightRow => leftRow ++ rightRow)
    }).toList

    copy(data = newData)
  }

  // O(n * log(m)), where n = |left| and m = |right|
  def hashJoin(other: Embeddings, left: Expr, right: Expr)(implicit header: RecordHeader, context: MemRuntimeContext): Embeddings = {
    if (this.data.size > other.data.size) {
      other.hashJoin(this, right, left)
    } else {
      val
      hashTable = this.rows.map(row => row.evaluate(left).hashCode() -> row)
        .toSeq
        .groupBy(_._1)

      val newData = other.rows
        .filter(rightRow => hashTable.contains(rightRow.evaluate(right).hashCode()))
        .flatMap(rightRow => {
          val rightValue = rightRow.evaluate(right)
          hashTable(rightValue.hashCode())
            .map(_._2)
            .filter(leftRow => leftRow.evaluate(left) == rightValue) // hash collision check
            .map(leftRow => leftRow ++ rightRow)
        }).toList

      copy(data = newData)
    }
  }
}


