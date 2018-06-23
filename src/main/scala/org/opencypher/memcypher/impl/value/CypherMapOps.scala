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
package org.opencypher.memcypher.impl.value

import com.typesafe.scalalogging.Logger
import org.opencypher.memcypher.api.value.{MemNode, MemRelationship}
import org.opencypher.memcypher.impl.MemRuntimeContext
import org.opencypher.memcypher.impl.planning.IdGenerator.generateID
import org.opencypher.memcypher.impl.table.RecordHeaderUtils._
import org.opencypher.memcypher.impl.value.CypherValueOps._
import org.opencypher.okapi.api.types.{CTNode, CTRelationship}
import org.opencypher.okapi.api.value.CypherValue.{CypherBoolean, CypherInteger, CypherList, CypherMap, CypherNull, CypherString, CypherValue}
import org.opencypher.okapi.impl.exception.{IllegalArgumentException, UnsupportedOperationException}
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.relational.impl.table.RecordHeader

object CypherMapOps {

  implicit class RichCypherMap(map: CypherMap) {

    def filterKeys(keys: Set[String]): CypherMap = map.value.filterKeys(keys.contains)

    def evaluate(expr: Expr)(implicit header: RecordHeader, context: MemRuntimeContext): CypherValue = {

      val logger = Logger("CypherMapOps#evaluate")

      expr match {

        case Param(name) =>
          logger.info(s"Parameter lookup: `$name` in ${context.parameters}")
          context.parameters(name)

        case _: Var | _: Param | _: Property | _: HasLabel | _: Type | _: StartNode | _: EndNode =>
          logger.info(s"Direct lookup: Expr `$expr` with column name `${expr.columnName}` in $map")
          map(expr.columnName)

        //also used to generate new IDs for construct here!
        case Id(v) =>
          v match {
            case v: Var => {
              logger.info(s"Id lookup: `$v` in $map")
              evaluate(v)
            }
            case l: ListLit => {
              logger.info(s"Id generation: `$l` in $map")
              val list = evaluate(l).cast[CypherList].value //can be only cypherList as ListLit gets evaluated to Cypherlist
              generateID(list.head.toString(), list.tail.map(_.toString())) //maybe make generateID arguments just one list?
            }
            case x => throw new IllegalArgumentException("unexpected type for id-expr evaluation " + x.getClass)
          }

        case HasType(rel, relType) =>
          evaluate(Type(rel)()) == CypherString(relType.name)

        case Labels(innerExpr) =>
          logger.info(s"Labels: `$innerExpr` in: $map")
          val node = Var(innerExpr.columnName)(CTNode)
          val labelExprs = header.labels(node)
          val labelNames = labelExprs.map(_.label.name)
          val labelColumns = labelExprs.map(evaluate)
          labelNames
            .zip(labelColumns)
            .filter(_._2 == CypherBoolean(true))
            .map(pair => CypherString(pair._1))
            .toList

        case Equals(lhs, rhs) =>
          logger.info(s"Equals: `$expr` in: $map")
          evaluate(lhs) == evaluate(rhs)

        case Not(innerExpr) =>
          logger.info(s"Not: `$innerExpr` in: $map")
          !evaluate(innerExpr)

        case IsNotNull(innerExpr) =>
          logger.info(s"IsNotNull: `$innerExpr` in: $map")
          evaluate(innerExpr) != CypherNull

        case Ands(exprs) =>
          logger.info(s"Ands: ${exprs.mkString("[", ",", "]")}")
          exprs.map(evaluate).reduce(_ && _)

        case Ors(exprs) =>
          logger.info(s"Ors: ${exprs.mkString("[", ",", "]")}")
          exprs.map(evaluate).reduce(_ || _)

        case GreaterThan(lhs, rhs) =>
          logger.info(s"GreaterThan: `$expr` in: $map")
          evaluate(lhs) > evaluate(rhs)

        case Add(lhs, rhs) =>
          logger.info(s"Add: `$expr` in: $map")
          evaluate(lhs) + evaluate(rhs)

        case _: TrueLit =>
          true

        case _: FalseLit =>
          false

        case i: IntegerLit =>
          i.v

        case s: StringLit =>
          s.v

        case l: ListLit =>
          l.v.toList.map(evaluate(_))

        case b: BoolLit =>
          b.v

        case _ =>
          throw IllegalArgumentException("Supported Cypher Expression", expr.getClass.getSimpleName)
      }
    }

    def nest(header: RecordHeader): CypherMap = {
      val values = header.internalHeader.fields.map { field =>
        field.name -> nestField(map, field, header)
      }.toSeq

      CypherMap(values: _*)
    }

    private def nestField(row: CypherMap, field: Var, header: RecordHeader): CypherValue = {
      field.cypherType match {
        case _: CTNode =>
          nestNode(row, field, header)

        case _: CTRelationship =>
          nestRel(row, field, header)

        case _ =>
          row(header.slotFor(field).columnName)
      }
    }

    private def nestNode(row: CypherMap, field: Var, header: RecordHeader): CypherValue = {
      val idValue = row(header.slotFor(field).columnName)
      val columnName = header.slotFor(field).columnName
      idValue match {
        case id: CypherInteger =>
          val labels = header
            .labelSlots(field)
            .mapValues { s =>
              row(s.columnName) match {
                case CypherNull => false
                case value => value.cast[Boolean]
              }
            } //check for CypherNull as cast[Boolean] could throw NullPointerException otherwise
            .collect { case (h, b) if b => h.label.name }
            .toSet

          val properties = header
            .propertySlots(field)
            .mapValues { s => row(s.columnName) }
            .collect { case (p, v) if !v.isNull => p.key.name -> v }

          MemNode(id.value, labels, properties)

        case invalidID => throw UnsupportedOperationException(s"MemNode ID has to be a Long instead of ${invalidID.getClass}")
      }
    }

    private def nestRel(row: CypherMap, field: Var, header: RecordHeader): CypherValue = {
      val idValue = row(header.slotFor(field).columnName)
      idValue match {
        case id: CypherInteger =>
          val source = row(header.sourceNodeSlot(field).columnName).cast[Long]
          val target = row(header.targetNodeSlot(field).columnName).cast[Long]
          val relType = row(header.typeSlot(field).columnName).cast[String]

          val properties = header
            .propertySlots(field)
            .mapValues { s => row(s.columnName) }
            .collect { case (p, v) if !v.isNull => p.key.name -> v }

          MemRelationship(id.value, source, target, relType, properties)
        case invalidID => throw UnsupportedOperationException(s"CAPSRelationship ID has to be a Long instead of ${invalidID.getClass}")
      }
    }
  }

}
