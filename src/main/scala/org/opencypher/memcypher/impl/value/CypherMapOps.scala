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
import org.opencypher.okapi.api.value.CypherValue.{CypherMap, CypherNode, CypherValue}
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.ir.api.PropertyKey
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.relational.impl.table.RecordHeader
import org.opencypher.memcypher.impl.MemRuntimeContext
import org.opencypher.memcypher.impl.value.CypherValueOps._

object CypherMapOps {

  implicit class RichCypherMap(map: CypherMap) {

    def filterKeys(keys: Seq[String]): CypherMap = map.value.filterKeys(keys.contains)

    def evaluate(expr: Expr)(implicit header: RecordHeader, context: MemRuntimeContext): CypherValue = {

      val logger = Logger("CypherMapOps#evaluate")

      expr match {

        case Param(name) =>
          logger.info(s"Parameter lookup: `$name` in ${context.parameters}")
          context.parameters(name)

        case Property(Var(v), PropertyKey(k)) =>
          logger.info(s"Property lookup: `$v.$k` in $map")
          map(v) match {
            case MemNode(_, _, props) => props(k)
            case MemRelationship(_, _, _, _, props) => props(k)
            case _ => throw IllegalArgumentException("MemNode or MemRelationship", v)
          }

        case Id(Var(v)) =>
          logger.info(s"Id lookup: `$v` in $map")
          map(v) match {
            case MemNode(id, _, _) => id
            case _ => throw IllegalArgumentException("MemNode", v)
          }

        case StartNode(Var(r)) =>
          logger.info(s"StartNode lookup: `r` in $map")
          map(r) match {
            case rel: MemRelationship => rel.source
            case _ => throw IllegalArgumentException("MemRelationship", r)
          }

        case EndNode(Var(r)) =>
          logger.info(s"EndNode lookup: `r` in $map")
          map(r) match {
            case rel: MemRelationship => rel.target
            case _ => throw IllegalArgumentException("MemRelationship", r)
          }

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
          throw IllegalArgumentException("supported expression", expr.getClass.getSimpleName)
      }
    }
  }

}
