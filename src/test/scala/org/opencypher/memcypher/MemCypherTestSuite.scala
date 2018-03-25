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
package org.opencypher.memcypher

import org.opencypher.memcypher.api.{MemCypherGraph, MemCypherSession}
import org.opencypher.memcypher.support.creation.memcypher.MemCypherTestGraphFactory
import org.opencypher.okapi.ir.test.support.creation.propertygraph.TestPropertyGraphFactory
import org.scalatest.{FunSpec, Matchers}

import org.opencypher.memcypher.api.MemCypherConverters._

abstract class MemCypherTestSuite extends FunSpec with Matchers {
  implicit lazy val session: MemCypherSession = MemCypherSession.create

  val initGraph: String => MemCypherGraph = (createQuery) =>
    MemCypherTestGraphFactory(TestPropertyGraphFactory(createQuery)).asMemCypher
}
