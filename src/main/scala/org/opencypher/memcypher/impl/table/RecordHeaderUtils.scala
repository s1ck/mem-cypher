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
package org.opencypher.memcypher.impl.table

import org.opencypher.okapi.ir.api.expr.{Expr, Property}
import org.opencypher.okapi.relational.impl.table.{FieldSlotContent, ProjectedExpr, RecordSlot, SlotContent}

object RecordHeaderUtils {

  implicit class RichRecordSlot(slot: RecordSlot) {

    def columnName: String = slot.content match {
      case fieldContent: FieldSlotContent => fieldContent.field.name
      case ProjectedExpr(p: Property) => s"${p.withoutType}:${p.cypherType.material.name}"
      case ProjectedExpr(expr) => expr.withoutType
    }
  }

  implicit class RichRecordSlotContent(content: SlotContent) {
    def columnName: String = content match {
      case fieldContent: FieldSlotContent => fieldContent.field.name
      case ProjectedExpr(p: Property) => s"${p.withoutType}:${p.cypherType.material.name}"
      case ProjectedExpr(expr) => expr.withoutType
    }
  }

  implicit class RichExpression(expr: Expr) {
    def columnName: String = expr match {
      case _: Property => s"${expr.withoutType}:${expr.cypherType.material.name}"
      case _ => expr.withoutType
    }
  }
}
