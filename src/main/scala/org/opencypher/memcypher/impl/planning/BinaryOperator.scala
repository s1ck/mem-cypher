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

import org.opencypher.memcypher.api.{Embeddings, MemCypherGraph, MemCypherSession, MemRecords}
import org.opencypher.memcypher.impl.table.RecordHeaderUtils._
import org.opencypher.memcypher.impl.value.CypherMapOps._
import org.opencypher.memcypher.impl.{MemPhysicalResult, MemRuntimeContext}
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.api.value.CypherValue.{CypherBoolean, CypherInteger, CypherList, CypherMap, CypherString}
import org.opencypher.okapi.impl.exception.{IllegalArgumentException, NotImplementedException}
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.ir.api.set.SetPropertyItem
import org.opencypher.okapi.ir.api.{Label, PropertyKey}
import org.opencypher.okapi.logical.impl.{ConstructedEntity, ConstructedNode, ConstructedRelationship, LogicalPatternGraph}
import org.opencypher.okapi.relational.impl.physical.{InnerJoin, JoinType, LeftOuterJoin, RightOuterJoin}
import org.opencypher.okapi.relational.impl.table._

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

sealed trait extendedEntity {
  def wrappedEntity: ConstructedEntity

  def groupBy: Set[Expr]

  def aggregatedProperties: List[SetPropertyItem[Aggregator]]
}

//cant be case class because ConstructedEntity trait is sealed in openCypher
//todo: case class wrapping constructedNode
case class ConstructedNodeExtended(wrappedEntity: ConstructedNode,
                                   groupBy: Set[Expr],
                                   aggregatedProperties: List[SetPropertyItem[Aggregator]]) extends extendedEntity

//todo: case class wrapping constructedRel.
case class ConstructedRelationshipExtended(wrappedEntity: ConstructedRelationship,
                                           groupBy: Set[Expr],
                                           aggregatedProperties: List[SetPropertyItem[Aggregator]]) extends extendedEntity


final case class ConstructGraph(left: MemOperator, right: MemOperator, construct: LogicalPatternGraph) extends BinaryOperator {

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
    var groupBy_sets = List[SetPropertyItem[Expr]]()
    var aggregated_sets = List[SetPropertyItem[Aggregator]]()
    var remaining_sets = List[SetPropertyItem[Expr]]()
    //empty result due to empty matchTable as constructPattern evaluated for each row in matchTable
    if (matchTable.size == 0)
      return MemPhysicalResult(MemRecords(Embeddings.empty, RecordHeader.empty), MemCypherGraph.empty, name)

    IdGenerator.init(getHighestIDtoClone(clonedVarsToInputVars.values.toSet, matchTable))
    //put each SetPropertyItem object into corresponding list
    sets.foreach {
      case item if item.propertyKey == "groupby" => groupBy_sets ::= item
      case item@other =>
        other.setValue match {
          case s: StringLit if s.v.matches("(count|sum|min|max|collect)\\Q(\\E(.*)\\Q)\\E") => //todo: use pattenmatch to give parameters into function (otherwise twice aggr. pattern)
            aggregated_sets ::= SetPropertyItem(item.propertyKey, item.variable, stringToAggrExpr(s.v, matchTable.columnType, matchTable.header))
          case _ => remaining_sets ::= other
        }
    }

    val copiedHeader = clonedVarsToInputVars.foldLeft(RecordHeader.empty) { (head, tuple) =>
      tuple._1.cypherType match {
        case _: CTNode => head ++ RecordHeader.nodeFromSchema(tuple._1, schema)
        case _: CTRelationship => head ++ RecordHeader.relationshipFromSchema(tuple._1, schema)
        case _ => head
      }
    }
    val constructHeader = newEntities.foldLeft(copiedHeader) { (head, entity) =>
      head ++ (entity match {
        case _: ConstructedNode => RecordHeader.nodeFromSchema(entity.v, schema)
        case _: ConstructedRelationship => RecordHeader.relationshipFromSchema(entity.v, schema)
      })
    }

    val extendedEntities = newEntities.map(x => createExtendedEntity(x, groupBy_sets.find(_.variable == x.v), aggregated_sets.filter(_.variable == x.v), matchTable))

    val extendedNodes = extendedEntities.collect { case n: ConstructedNodeExtended => n }
    val extendedRelationships = extendedEntities.collect { case r: ConstructedRelationshipExtended => r }

    val clonedNodes = clonedVarsToInputVars.collect { case n if n._1.cypherType.equals(CTNode) => n }
    val clonedRelationships = clonedVarsToInputVars.collect { case r if r._1.cypherType.equals(CTRelationship) => r }

    var extendedMatchTable = matchTable //for copy of normal header needed

    //rewrite foreach into foldLeft ? (would need more variables)
    extendedNodes.foreach(entity => extendedMatchTable = extendMatchTable(entity, extendedMatchTable)(context))
    clonedNodes.foreach(x => extendedMatchTable = copySlotContents(x._1, x._2, clone = true, extendedMatchTable)) //nodes before relationships
    extendedRelationships.foreach(entity => extendedMatchTable = extendMatchTable(entity, extendedMatchTable)(context))
    clonedRelationships.foreach(x => extendedMatchTable = copySlotContents(x._1, x._2, clone = true, extendedMatchTable))

    //apply remaining SetPropertyItems
    val result = remaining_sets.foldLeft(extendedMatchTable.data) { (data, item) =>
      data.project(item.setValue, RichExpression(Property(Var(item.variable.name)(), PropertyKey(item.propertyKey))(item.setValue.cypherType)).columnName)(constructHeader, context) // use columnname fct
    }
    val stored_ids = IdGenerator.storedIDs //for testing purpose

    val important_columns = constructHeader.contents.map(slot => RichRecordSlotContent(slot).columnName)
    val compact_result = result.select(important_columns)(constructHeader, context)

    MemPhysicalResult(MemRecords(compact_result, constructHeader), MemCypherGraph.empty, name)
  }

  //todo: groupby no list just one SetPropertyItem
  def createExtendedEntity(newEntity: ConstructedEntity, groupBy: Option[SetPropertyItem[Expr]], aggregatedProperties: List[SetPropertyItem[Aggregator]], matchTable: MemRecords): extendedEntity = {
    var groupByVarSet = newEntity.baseEntity match {
      case Some(v) => Set[Expr](Id(v)())
      case None =>
        groupBy match {
          case Some(setItem) =>
            val potentialListOfGroupings = RichCypherMap(CypherMap.empty).evaluate(setItem.setValue)(RecordHeader.empty, MemRuntimeContext.empty) //evaluate Expr , maybe sort this list?
            potentialListOfGroupings match {
              case CypherList(values) => values.map(y => stringToExpr(y.toString(), matchTable.columnType, matchTable.header)).toSet
              case CypherString(string) => Set[Expr](stringToExpr(string, matchTable.columnType, matchTable.header))
              case _: CypherInteger | _: CypherBoolean => Set[Expr](StringLit("constant")(CTString)) //groupby constant --> only one entity created
              case error => throw IllegalArgumentException("wrong value typ for groupBy: should be CypherList but found " + error.getClass.getSimpleName)
            }
          case None => Set[Expr]()
        }
    }
    newEntity match {
      case n: ConstructedNode => ConstructedNodeExtended(n, groupByVarSet, aggregatedProperties)
      case r: ConstructedRelationship =>
        groupByVarSet ++= Set(Id(r.source)(CTNode), Id(r.target)(CTNode)) // relationships implicit grouped by source and target node
        ConstructedRelationshipExtended(r, groupByVarSet, aggregatedProperties)
    }
  }

  //todo: put in extra methods
  def extendMatchTable(entity: extendedEntity, matchTable: MemRecords)(implicit context: MemRuntimeContext): MemRecords = {
    implicit val header: RecordHeader = matchTable.header
    val idExpr = Id(ListLit(StringLit(entity.wrappedEntity.v.name)() +: entity.groupBy.toIndexedSeq)())()
    var newData = matchTable.data

    if (entity.aggregatedProperties.nonEmpty) {
      //workaround with renamings of Var-name in idExpr, because group-op alters column names from f.i. a to id(a)
      val renamedExpr = entity.groupBy.map {
        case expr@Id(Var(_)) => Id(Var(RichExpression(expr).columnName)())()
        case expr@Labels(Var(_)) => Id(Var(RichExpression(expr).columnName)())()
        case x => x
      }
      val aggregations = entity.aggregatedProperties.foldLeft(List[(Var, Aggregator)]())((list, setItem) => //todo: maybe method setItem to columName?
        list :+ (Var(RichExpression(Property(Var(setItem.variable.name)(), PropertyKey(setItem.propertyKey))(CTString)).columnName)() -> setItem.setValue)) //:STRING because header expects string todo: use columnName
      newData = newData.group(entity.groupBy, aggregations.toSet)
        .innerJoin(newData, ListLit(renamedExpr.toIndexedSeq)(), ListLit(entity.groupBy.toIndexedSeq)())
    }
    entity.wrappedEntity match {
      case r: ConstructedRelationship =>
        newData = newData
          .project(Id(r.source)(), RichExpression(StartNode(r.v)()).columnName)
          .project(Id(r.target)(), RichExpression(EndNode(r.v)()).columnName)
        r.typ match {
          case Some(typ) => newData = newData.project(StringLit(typ)(), RichExpression(Type(r.v)()).columnName)
          case None =>
        }
      case n: ConstructedNode =>
        n.labels.foreach(label => newData = newData.project(TrueLit(), RichExpression(HasLabel(Var(n.v.name)(), Label(label.name))()).columnName))
    }
    newData = newData.project(idExpr, entity.wrappedEntity.v.name)
    entity.wrappedEntity.baseEntity match {
      case Some(base) =>
        newData = copySlotContents(entity.wrappedEntity.v, base, clone = false, MemRecords(newData, header)).data
      case None =>
    }
    MemRecords(newData, header)
  }

  def copySlotContents(newVar: Var, baseVar: Var, clone: Boolean, table: MemRecords): MemRecords = {

    if (newVar.equals(baseVar)) return table
    var slotsToCopy = table.header.childSlots(baseVar)
    if (clone) slotsToCopy :+= table.header.slotFor(baseVar) //adds id slot

    val newData = slotsToCopy.foldLeft(table.data) { (data, slot) => {
      val columnName = RichRecordSlotContent(slot.withOwner(newVar).content).columnName
      val columnExists = slot.content.key match {
        case _@Property(_, key) =>
          val columnNameWithoutType = newVar.name + "." + key.name //for properties ... check if one of the columns starts with varname.propertyname (ignore cyphertype)
          table.columnType.keySet.exists(_.startsWith(columnNameWithoutType)) //only x.age:Integer or x.age:String can exist this way
        case _ =>
          table.columnType.keySet.contains(columnName)
      }
      if (!columnExists) data.project(slot.content.key, columnName)(table.header, MemRuntimeContext.empty)
      else data
    }
    }
    MemRecords(newData, header)
  }

  def stringToAggrExpr(value: String, validColumns: Map[String, CypherType], header: RecordHeader): Aggregator = {
    val aggregationPattern = "(count|sum|min|max|collect)\\Q(\\E(distinct )?(.*)\\Q)\\E".r //in outer method this pattern already checked
    value match {
      case aggregationPattern(aggr, distinct, inner) =>
        val innerExpr = stringToExpr(inner, validColumns, header)
        aggr match {
          case "count" => Count(innerExpr, distinct != null)()
          case "sum" => Sum(innerExpr)()
          case "min" => Min(innerExpr)()
          case "max" => Max(innerExpr)()
          case "collect" => Collect(innerExpr, distinct != null)()
          case unknown => throw NotImplementedException(unknown + "aggregator not implemented yet")
        }
      case _ => throw IllegalArgumentException("no correct aggregator")
    }
  }

  def stringToExpr(value: String, validColumns: Map[String, CypherType], header: RecordHeader): Expr = {
    val propertyPattern = "(.*)\\Q.\\E(.*)".r
    val typePattern = "type\\Q(\\E(.*)\\Q)\\E".r
    val labelsPattern = "labels\\Q(\\E(.*)\\Q)\\E".r
    //maybe also allow groupby labels() or haslabel() ? ...allow for property optional type given via pattern? ... string to cyphertype needed then (worth?)
    value match {
      case propertyPattern(varName, propertyName) =>
        //get Cypher type via header, because in validColumns-Map wrong sometimes Cypher type saved (like CTNULL instead of CTStringOrNull
        val matchingSlots = header.childSlots(Var(varName)()).filter(_.content.key match {
          case x: Property => x.key.name.equals(propertyName)
          case _ => false
        })
        if (matchingSlots.isEmpty) throw IllegalArgumentException("valid property for groupBy", "invalid groupBy property " + value)
        Property(Var(varName)(), PropertyKey(propertyName))(matchingSlots.head.content.cypherType)
      case typePattern(validTypeParameter) if validColumns.keySet.contains(value) =>
        Type(Var(validTypeParameter)())()
      case validVarParameter if validColumns.keySet.contains(validVarParameter) => Id(Var(validVarParameter)())()
      case labelsPattern(varName) if validColumns.keySet.contains(varName) => Labels(Var(varName)())() //maybe check via header if node
      case _ => throw IllegalArgumentException("valid parameter for groupBy", "invalid groupBy parameter " + value)
    }
  }

  //alternative: check entities of working graph for highest id instead?
  def getHighestIDtoClone(varsToClone: Set[Var], table: MemRecords): Long = {
    var highestID: Long = -1
    if (varsToClone.nonEmpty) {
      val aggregations = varsToClone.map(v => v -> Max(Id(v)())().asInstanceOf[Aggregator])
      val highestIDs = table.data.group(Set(TrueLit()), aggregations)(table.header, MemRuntimeContext.empty)
      //scan the one resulting row (due to group by constant) for highest id
      highestID = highestIDs.rows.next().value
        .maxBy(_._2 match {
          case i: CypherInteger => i.value
          case _ => -1
        })._2.cast[Long]
    }
    highestID
  }
}

object IdGenerator {
  private val current_max_id = new AtomicLong(-1)
  var storedIDs: Map[String, Long] = Map[String, Long]() //todo: make private

  def generateID(constructedEntityName: String, groupBy: List[String] = List()): Long = {
    if (groupBy.nonEmpty) {
      //generate with groupbyKey ( schema: "entityName|baseValue1|baseValue2...")
      val key = groupBy.mkString(constructedEntityName, "|", "")
      if (storedIDs.contains(key)) storedIDs(key) //return found Id
      else { //generate and save new ID put in storedIds.getOrElse
        storedIDs += key -> current_max_id.incrementAndGet()
        current_max_id.get()
      }
    }
    //entities implicit grouped by rowId --> for each call generate a new Id
    else current_max_id.incrementAndGet()
  }

  //needed, so that next query works on empty storedIDs Map
  // current_max = -1 --> id generation starts with 0
  def init(current_max: Long = -1): Unit = {
    storedIDs = Map[String, Long]()
    current_max_id.set(current_max)
  }
}
