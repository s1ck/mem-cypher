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
import org.opencypher.memcypher.impl.MemRuntimeContext
import org.opencypher.memcypher.impl.table.RecordHeaderUtils._
import org.opencypher.memcypher.impl.value.CypherValueOps._
import org.opencypher.okapi.api.types.CTNode
import org.opencypher.okapi.api.value.CypherValue.{CypherBoolean, CypherList, CypherMap, CypherString, CypherValue}
import org.opencypher.okapi.impl.exception.IllegalArgumentException
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
          logger.info(s"Direct lookup: `$expr` in $map")
          map(expr.columnName)

        case Id(v) =>
          logger.info(s"Id lookup: `$v` in $map")
          evaluate(v)

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

        case Ands(exprs) =>
          logger.info(s"Ands: ${exprs.mkString("[", ",", "]")}")
          exprs.map(evaluate).reduce(_ && _)

        case Ors(exprs) =>
          logger.info(s"Ors: ${exprs.mkString("[", ",", "]")}")
          exprs.map(evaluate).reduce(_ || _)

        case GreaterThan(lhs, rhs) =>
          logger.info(s"GreaterThan: `$expr` in: $map")
          evaluate(lhs) > evaluate(rhs)

        case _: TrueLit =>
          true

        case _: FalseLit =>
          false

        case _ =>
          throw IllegalArgumentException("Supported Cypher Expression", expr.getClass.getSimpleName)
      }
    }
  }

}
