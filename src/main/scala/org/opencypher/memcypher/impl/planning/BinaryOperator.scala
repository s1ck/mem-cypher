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
package org.opencypher.memcypher.impl.planning

import java.util.concurrent.atomic.AtomicLong

import org.opencypher.memcypher.api.value.{MemNode, MemRelationship}
import org.opencypher.memcypher.api.{Embeddings, MemCypherGraph, MemCypherSession, MemRecords}
import org.opencypher.memcypher.impl.value.CypherMapOps._
import org.opencypher.memcypher.impl.{MemPhysicalResult, MemRuntimeContext}
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types.CTInteger
import org.opencypher.okapi.api.value.CypherValue.{CypherBoolean, CypherInteger, CypherMap}
import org.opencypher.okapi.impl.exception.NotImplementedException
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.ir.api.set.SetPropertyItem
import org.opencypher.okapi.logical.impl.{ConstructedEntity, ConstructedNode, LogicalPatternGraph}
import org.opencypher.okapi.relational.impl.physical.{InnerJoin, JoinType, LeftOuterJoin, RightOuterJoin}
import org.opencypher.okapi.relational.impl.table.RecordHeader

private[memcypher] abstract class BinaryOperator extends MemOperator {

  def left: MemOperator

  def right: MemOperator

  override def execute(implicit context: MemRuntimeContext): MemPhysicalResult =
    executeBinary(left.execute, right.execute)

  def executeBinary(left: MemPhysicalResult, right: MemPhysicalResult)(implicit context: MemRuntimeContext): MemPhysicalResult
}

final case class Join(
                       left: MemOperator,
                       right: MemOperator,
                       joinExprs: Seq[(Expr, Expr)],
                       header: RecordHeader,
                       joinType: JoinType) extends BinaryOperator {

  override def executeBinary(left: MemPhysicalResult, right: MemPhysicalResult)(implicit context: MemRuntimeContext): MemPhysicalResult = {
    if (joinExprs.length > 1) throw NotImplementedException("Multi-way join support")

    val (leftExpr, rightExpr) = joinExprs.head
    val leftId = leftExpr match {
      case v: Var => Id(v)(CTInteger)
      case other => other
    }
    val rightId = rightExpr match {
      case v: Var => Id(v)(CTInteger)
      case other => other
    }

    val newData = joinType match {
      case InnerJoin => left.records.data.innerJoin(right.records.data, leftId, rightId)(header, context)
      case RightOuterJoin => left.records.data.rightOuterJoin(right.records.data, leftId, rightId)(header, context)
      case LeftOuterJoin => right.records.data.rightOuterJoin(left.records.data, rightId, leftId)(header, context)
      case other => throw NotImplementedException(s"Unsupported JoinType: $other")
    }

    MemPhysicalResult(MemRecords(newData, header), left.workingGraph, left.workingGraphName)
  }

}

final case class CartesianProduct(left: MemOperator, right: MemOperator, header: RecordHeader) extends BinaryOperator {
  override def executeBinary(left: MemPhysicalResult, right: MemPhysicalResult)(implicit context: MemRuntimeContext): MemPhysicalResult = {
    val newData = left.records.data.cartesianProduct(right.records.data)
    MemPhysicalResult(MemRecords(newData, header), left.workingGraph, left.workingGraphName)
  }
}

final case class TabularUnionAll(left: MemOperator, right: MemOperator) extends BinaryOperator with InheritedHeader {
  override def executeBinary(left: MemPhysicalResult, right: MemPhysicalResult)(implicit context: MemRuntimeContext): MemPhysicalResult = {
    val newData = left.records.data.unionAll(right.records.data)
    MemPhysicalResult(MemRecords(newData, header), left.workingGraph, left.workingGraphName)
  }
}

final case class ConstructGraph(left: MemOperator, right: MemOperator, construct: LogicalPatternGraph) extends BinaryOperator {
  val current_max_id = new AtomicLong()

  //toString method from openCypher
  override def toString: String = {
    val entities = construct.clones.keySet ++ construct.newEntities.map(_.v)
    s"ConstructGraph(on=[${construct.onGraphs.mkString(", ")}], entities=[${entities.mkString(", ")}])"
  }

  override def header: RecordHeader = RecordHeader.empty

  override def executeBinary(left: MemPhysicalResult, right: MemPhysicalResult)(implicit context: MemRuntimeContext): MemPhysicalResult = {

    implicit val session: MemCypherSession = left.workingGraph.session
    val matchTable = left.records
    val LogicalPatternGraph(schema, clonedVarsToInputVars, newEntities, sets, _, name) = construct

    val clonedEntitiesTable = clonedVarsToInputVars.foldLeft(Seq.empty[CypherMap]) {
      case (clonedEntities, (newVar, oldVar)) => clonedEntities ++ copyContents(oldVar, Option(newVar), matchTable).data.data.map(_.updated(newVar.name, CypherInteger(generateID())))
    }

    val entityTable = createEntities(newEntities, schema, matchTable)
    val entityTableWithProperties = sets.foldLeft(entityTable) {
      case (table, SetPropertyItem(key, v, expr)) => constructProperty(v, key, expr, table)(table.header, context)
    }

    val nodesUnited = entityTableWithProperties.data.unionAll(Embeddings(clonedEntitiesTable))

    //Todo: MemRecords to MemGraph (use nestNode --> getHeaderRight!)
    //---------------------------------------------------------------------
    //straight-forward version (newEntities+their labels & properties)(without use of MemRecords)
    val nodes_2 = newEntities.foldLeft(Seq.empty[MemNode]) {
      (list, b) =>
        b match {
          case x: ConstructedNode =>
            val labels = x.labels.map(_.name.toString)
            val properties = sets.filter(_.variable == x.v).foldLeft(CypherMap.empty) {
              (map, setItem) => map.updated(setItem.propertyKey, RichCypherMap(CypherMap.empty).evaluate(setItem.setValue)(RecordHeader.empty, context))
            }
            list :+ MemNode(generateID(), labels, properties)
          case _ => list //println("not implemented for" + z.getClass)
        }
    }


    val newGraph = MemCypherGraph.create(nodes_2, Seq[MemRelationship]())
    MemPhysicalResult(MemRecords.unit(), newGraph, name)
  }

  def createEntities(newEntities: Set[ConstructedEntity], schema: Schema, matchTable: MemRecords): MemRecords = {

    val nodes = newEntities.collect {
      case c@ConstructedNode(_, _, _) => c
    }
    var header = RecordHeader.empty
    val nodeData = nodes.foldLeft(Seq[CypherMap]()) {
      (generatedNodes, node) => {
        header ++= RecordHeader.nodeFromSchema(node.v, schema) //useless now?
        //todo baseNodes.header --> use to union with existing header!
        val baseNodes = node.baseEntity match {
          case Some(baseNode) => copyContents(baseNode, Option(node.v), matchTable)
          case None => MemRecords(Embeddings.empty, RecordHeader.empty)
        }
        val newNode = CypherMap(node.labels.map(label => node.v.name + ":" + label.name -> CypherBoolean(true)).toMap)
        if (baseNodes.data.data.nonEmpty) {
          var newNodesWithbaseNodes = Seq[CypherMap]()
          baseNodes.data.data.foreach(copiedNode => {
            newNode.updated(node.v.name, generateID())
            newNodesWithbaseNodes :+= (copiedNode ++ newNode)
          }
          )
          generatedNodes ++ newNodesWithbaseNodes
        }
        else generatedNodes :+ newNode.updated(node.v.name, generateID())
        //problem: both freshvars in Recordheader --> both vars must have an entry in one Cyphermap? (with header?)
      }
    }

    MemRecords.create(Embeddings(nodeData), header)
  }

  // with grouping make with map (key = variablename|groupedbyvars(List(var,value))
  def generateID(): Long = current_max_id.incrementAndGet()

  //return memRecord --> header too
  //maybe copy id as @oldID for later
  def copyContents(baseEntity: Var, newEntity: Option[Var], baseRecords: MemRecords): MemRecords = {
    implicit val header: RecordHeader = baseRecords.header
    implicit val context: MemRuntimeContext = MemRuntimeContext.empty

    val origLabelSlots = header.labelSlots(baseEntity).values.toSet
    val origPropertySlots = header.propertySlots(baseEntity).values.toSet
    val copySlotContents = origLabelSlots.map(_.withOwner(baseEntity)).map(_.content)
    val explRow = baseRecords.data.data.head
    val explKey = copySlotContents.head.key
    val storedCypherValue = RichCypherMap(explRow).evaluate(explKey) //getValue of the row --> possible to get values for all keys nested in expr


    val labelFields = baseRecords.data.columns.filter(_.matches(baseEntity.name + ":.*")) //use naming convention x.propertykey & x:label (todo: find better way)
    val propertyFields = baseRecords.data.columns.filter(_.matches(baseEntity.name + "[.].*"))
    val fieldsToCopy = labelFields ++ propertyFields
    val baseData = baseRecords.data.select(fieldsToCopy)
      .distinct(fieldsToCopy) //right Context?

    val newData = newEntity match {
      case Some(newEntity) => {
        val renaming = fieldsToCopy.map(oldName => oldName -> oldName.replaceFirst(baseEntity.name, newEntity.name)).toMap
        baseData.withColumnsRenamed(renaming)
      }
      case None => baseData
    }

    MemRecords(newData, header)
  }

  def constructProperty(variable: Var, propertyKey: String, propertyValue: Expr, constructedTable: MemRecords)(implicit header: RecordHeader, context: MemRuntimeContext): MemRecords = {
    val dataToUpdate = constructedTable.data.filter(IsNotNull(variable)(CTInteger))
    val dataWithProperty = dataToUpdate.data.map(_.updated(variable.name + "." + propertyKey, RichCypherMap(CypherMap.empty).evaluate(propertyValue))) //use Columnname funktion? (would get escaped names)

    //fixes problem that entities with no new property are left out (find better way!)
    val dataWithNoProperty = constructedTable.data.data.diff(dataToUpdate.data)
    MemRecords.create(dataWithProperty ++ dataWithNoProperty, constructedTable.header)
  }
}
