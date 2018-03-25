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
package org.opencypher.memcypher.tck

import java.io.File

import org.opencypher.memcypher.MemCypherTestSuite
import org.opencypher.memcypher.api.{MemCypherGraph, MemCypherSession}
import org.opencypher.memcypher.support.creation.memcypher.MemCypherTestGraphFactory
import org.opencypher.okapi.ir.test.support.creation.TestGraphFactory
import org.opencypher.okapi.tck.test.{ScenariosFor, TCKGraph}
import org.opencypher.okapi.tck.test.Tags.{BlackList, WhiteList}
import org.opencypher.tools.tck.api.CypherTCK
import org.scalatest.Tag
import org.scalatest.prop.TableDrivenPropertyChecks._

import scala.util.{Failure, Success, Try}

class TckMemCypherTest extends MemCypherTestSuite {

  object TckMemCypher extends Tag("TckMemCypher")

  val defaultFactory: TestGraphFactory[MemCypherSession] = MemCypherTestGraphFactory

  private val scenarios = ScenariosFor(getClass.getResource("/scenario_blacklist").getFile)

  // white list tests are run on all factories
  forAll(scenarios.whiteList) { scenario =>
    it(s"[${defaultFactory.name}, ${WhiteList.name}] $scenario", WhiteList, TckMemCypher) {
      scenario(TCKGraph(defaultFactory, MemCypherGraph.empty)).execute()
    }
  }

  // black list tests are run on default factory
  forAll(scenarios.blackList) { scenario =>
    it(s"[${defaultFactory.name}, ${BlackList.name}] $scenario", BlackList, TckMemCypher) {
      val tckGraph = TCKGraph(defaultFactory, MemCypherGraph.empty)

      Try(scenario(tckGraph).execute()) match {
        case Success(_) =>
          throw new RuntimeException(s"A blacklisted scenario actually worked: $scenario")
        case Failure(_) =>
          ()
      }
    }
  }

  ignore("run Custom Scenario") {
    val file = new File(getClass.getResource("CustomTest.feature").toURI)

    CypherTCK
      .parseFilesystemFeature(file)
      .scenarios
      .foreach(scenario => scenario(TCKGraph(defaultFactory, MemCypherGraph.empty)).execute())
  }

  ignore("run Single Scenario") {
    scenarios.get("A simple pattern with one bound endpoint")
      .foreach(scenario => scenario(TCKGraph(defaultFactory, MemCypherGraph.empty)).execute())
  }
}
