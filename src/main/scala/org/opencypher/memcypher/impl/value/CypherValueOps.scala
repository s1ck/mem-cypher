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

import org.opencypher.okapi.api.types.CypherType._
import org.opencypher.okapi.api.value.CypherValue.{CypherBoolean, CypherValue}
import CypherTypeOps._

object CypherValueOps {

  implicit class RichCypherValue(val value: CypherValue) extends AnyVal {

    def unary_! : Boolean =
      !value.asInstanceOf[CypherBoolean].unwrap

    def &&(other: CypherValue): CypherValue = {
      value.asInstanceOf[CypherBoolean].unwrap && other.asInstanceOf[CypherBoolean].unwrap
    }

    def ||(other: CypherValue): CypherValue = {
      value.asInstanceOf[CypherBoolean].unwrap || other.asInstanceOf[CypherBoolean].unwrap
    }

    def ==(other: CypherValue): Boolean = {
      value.cypherType.join(other.cypherType).equivalence.asInstanceOf[Equiv[Any]].equiv(value.unwrap, other.unwrap)
    }

    def !=(other: CypherValue): Boolean = {
      !value.cypherType.join(other.cypherType).equivalence.asInstanceOf[Equiv[Any]].equiv(value.unwrap, other.unwrap)
    }

    def >(other: CypherValue): Boolean = {
      value.cypherType.join(other.cypherType).ordering.asInstanceOf[Ordering[Any]].gt(value.unwrap, other.unwrap)
    }

    def >=(other: CypherValue): Boolean = {
      value.cypherType.join(other.cypherType).ordering.asInstanceOf[Ordering[Any]].gteq(value.unwrap, other.unwrap)
    }

    def <(other: CypherValue): Boolean = {
      value.cypherType.join(other.cypherType).ordering.asInstanceOf[Ordering[Any]].lt(value.unwrap, other.unwrap)
    }

    def <=(other: CypherValue): Boolean = {
      value.cypherType.join(other.cypherType).ordering.asInstanceOf[Ordering[Any]].lteq(value.unwrap, other.unwrap)
    }
  }

}
