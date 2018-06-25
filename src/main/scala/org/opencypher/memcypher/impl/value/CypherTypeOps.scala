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

import org.opencypher.okapi.api.types._
import org.opencypher.okapi.impl.exception.IllegalArgumentException

object CypherTypeOps {

  implicit class OrderingCypherType(ct: CypherType) {
    def ordering: Ordering[_] = ct match {
      case CTBoolean => Ordering[Boolean]
      case CTFloat => Ordering[Float]
      case CTInteger|CTIntegerOrNull => Ordering[Long]
      case CTString => Ordering[String]
      case CTStringOrNull => CTStringOrNullOrdering
      case CTNull => CTNullOrdering
      //todo: check if CTIntegerOrNull now fully supported
      case _ => throw IllegalArgumentException("Cypher type with ordering support", ct)
    }

    def equivalence: Equiv[_] = ct match {
      case CTBoolean => Equiv[Boolean]
      case CTFloat => Equiv[Float]
      case CTInteger => Equiv[Long]
      case CTString => Equiv[String]
      case _ => throw IllegalArgumentException("Cypher type with equivalence support", ct)
    }
  }

}

//just implemented needed functions for group-op
object CTNullOrdering extends Ordering[Null] {
  override def compare(x: Null, y: Null): Int = 0

  override def gt(x: Null, y: Null): Boolean = true
}

object CTStringOrNullOrdering extends Ordering[String] {
  override def compare(x: String, y: String): Int = 0

  override def gt(x: String, y: String): Boolean = {
    if (x == null && y == null) true
    else if (x == null) false
    else if (y == null) true
    else super.gt(x, y)
  }
}

