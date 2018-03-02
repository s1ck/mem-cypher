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

import org.opencypher.okapi.api.graph.{CypherResult, CypherSession, PropertyGraph}
import org.opencypher.okapi.api.table.CypherRecords
import org.opencypher.okapi.impl.exception.UnsupportedOperationException

object MemCypherConverters {

  implicit class RichSession(session: CypherSession) {
    def asMemCypher: MemCypherSession = session match {
      case s: MemCypherSession => s
      case _ => throw UnsupportedOperationException(s"can only handle MemCypher sessions, got $session")
    }
  }

  implicit class RichPropertyGraph(graph: PropertyGraph) {
    def asMemCypher: MemCypherGraph = graph match {
      case g: MemCypherGraph => g
      case _ => throw UnsupportedOperationException(s"can only handle MemCypher graphs, got $graph")
    }
  }

  implicit class RichCypherResult(result: CypherResult) {
    def asMemCypher: MemCypherResult = result match {
      case r: MemCypherResult => r
      case _ => throw UnsupportedOperationException(s"can only handle MemCypher result, got $result")
    }
  }

  implicit class RichCypherRecords(records: CypherRecords) {
    def asMemCypher: MemRecords = records match {
      case r: MemRecords => r
      case _ => throw UnsupportedOperationException(s"can only handle MemCypher records, got $records")
    }
  }

}
