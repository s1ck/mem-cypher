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
import org.opencypher.memcypher.impl.table.RecordHeaderUtils
import org.opencypher.memcypher.impl.value.CypherMapOps._
import org.opencypher.memcypher.impl.{MemPhysicalResult, MemRuntimeContext}
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.api.value.CypherValue.{CypherBoolean, CypherInteger, CypherList, CypherMap, CypherString, CypherValue}
import org.opencypher.okapi.impl.exception.{IllegalArgumentException, NotImplementedException}
import org.opencypher.okapi.ir.api.{Label, PropertyKey}
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.ir.api.set.SetPropertyItem
import org.opencypher.okapi.logical.impl.{ConstructedEntity, ConstructedNode, ConstructedRelationship, LogicalPatternGraph}
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

sealed trait aggregationExtension {
  def groupBy: Set[Expr]

  def aggregatedProperties: List[SetPropertyItem[Aggregator]]
}

class ConstructedNodeExtended(v: Var, labels: Set[Label], baseEntity: Option[Var],
                              val groupBy: Set[Expr],
                              val aggregatedProperties: List[SetPropertyItem[Aggregator]]) extends ConstructedNode(v, labels, baseEntity) with aggregationExtension

class ConstructedRelationshipExtended(v: Var, baseEntity: Option[Var], source: Var, target: Var, typ: Option[String],
                                      val groupBy: Set[Expr],
                                      val aggregatedProperties: List[SetPropertyItem[Aggregator]]) extends ConstructedRelationship(v, source, target, typ, baseEntity) with aggregationExtension


final case class ConstructGraph(left: MemOperator, right: MemOperator, construct: LogicalPatternGraph) extends BinaryOperator {

  //toString method from openCypher
  override def toString: String = {
    val entities = construct.clones.keySet ++ construct.newEntities.map(_.v)
    s"ConstructGraph(on=[${construct.onGraphs.mkString(", ")}], entities=[${entities.mkString(", ")}])"
  }

  override def header: RecordHeader = RecordHeader.empty

  override def executeBinary(left: MemPhysicalResult, right: MemPhysicalResult)(implicit context: MemRuntimeContext): MemPhysicalResult = {

    implicit val session: MemCypherSession = left.workingGraph.session
    IdGenerator.init() //todo: when clonedVarsToInputVars is not empty --> start after highest Id got via matchTable
    val matchTable = left.records
    val LogicalPatternGraph(schema, clonedVarsToInputVars, newEntities, sets, _, name) = construct
    if (matchTable.size == 0) return MemPhysicalResult(MemRecords(Embeddings.empty, RecordHeader.empty), MemCypherGraph.empty, name) //empty result on empty matchTable
    //clone_entites first !
    var groupby_sets = List[SetPropertyItem[Expr]]()
    var aggregated_sets = List[SetPropertyItem[Aggregator]]()
    var remaining_sets = List[SetPropertyItem[Expr]]()

    sets.foreach {
      case item if item.propertyKey == "groupby" => groupby_sets ::= item
      case item@other => {
        other.setValue match {
          case s: StringLit if s.v.matches("(count|sum|min|max|collect)\\Q(\\E(.*)\\Q)\\E") => aggregated_sets ::= SetPropertyItem(item.propertyKey, item.variable, stringtoAggrExpr(s.v, matchTable.columnType))
          case _ => remaining_sets ::= other
        }
      }
    }

    val constructHeader = newEntities.foldLeft(RecordHeader.empty) { (head, entity) =>
      head ++ (entity match {
        case _: ConstructedNode => RecordHeader.nodeFromSchema(entity.v, schema)
        case _: ConstructedRelationship => RecordHeader.relationshipFromSchema(entity.v, schema)
      })
    }

    val extendedEntities = newEntities.map(x => createExtendedEntity(x, groupby_sets.filter(_.variable == x.v), aggregated_sets.filter(_.variable == x.v), matchTable))
    val extendedNodes = extendedEntities.collect { case n: ConstructedNodeExtended => n }
    val extendedRelationships = extendedEntities.collect { case r: ConstructedRelationshipExtended => r }
    var extendedMatchTable = matchTable
    extendedNodes.foreach(entity => extendedMatchTable = extendMatchTable(entity, extendedMatchTable)(context))
    extendedRelationships.foreach(entity => extendedMatchTable = extendMatchTable(entity, extendedMatchTable)(context)) //only relationship id generated for now
    val stored_ids = IdGenerator.storedIDs

    val important_columns = extendedMatchTable.columnType.keySet.diff(matchTable.columnType.keySet)
    val compact_result = extendedMatchTable.data.select(important_columns)(constructHeader, context)

    val result = remaining_sets.foldLeft(compact_result) { (table, item) => table.project(item.setValue, item.variable.name + "." + item.propertyKey + ":" + item.setValue.cypherType.material.name)(constructHeader, context) }

    MemPhysicalResult(MemRecords(result, constructHeader), MemCypherGraph.empty, name)
  }

  //todo: if baseEntity --> id should get copied from baseentity --> put in idMap at the beginning of construct
  def createExtendedEntity(newEntity: ConstructedEntity, groupby: List[SetPropertyItem[Expr]], aggregatedProperties: List[SetPropertyItem[Aggregator]], matchTable: MemRecords): ConstructedEntity with aggregationExtension = {
    //todo: move to another position?
    var groupByVarSet = newEntity.baseEntity match {
      case Some(v) => Set[Expr](Id(v)())
      case None => Set[Expr]()
    }

    if (groupby.nonEmpty) {
      val potentialListOfGroupings = RichCypherMap(CypherMap.empty).evaluate(groupby.head.setValue)(RecordHeader.empty, MemRuntimeContext.empty) //evaluate Expr , maybe sort this list?
      potentialListOfGroupings match {
        case groupByStringList: CypherList => groupByVarSet ++= groupByStringList.value.map(y => stringToExpr(y.toString(), matchTable.columnType))
        case s: CypherString => groupByVarSet += stringToExpr(s.value, matchTable.columnType)
        case _: CypherInteger | _: CypherBoolean => groupByVarSet += StringLit("constant")(CTString) //groupby constant --> only one entity created
        case error => throw IllegalArgumentException("wrong value typ for groupBy: should be CypherList but found " + error.getClass.getSimpleName)
      }
    }

    newEntity match {
      case n: ConstructedNode => new ConstructedNodeExtended(n.v, n.labels, n.baseEntity, groupByVarSet, aggregatedProperties)
      case r: ConstructedRelationship =>
        groupByVarSet ++= Set(Id(r.source)(), Id(r.target)()) // relationships implicit grouped by source and target node
        new ConstructedRelationshipExtended(r.v, r.baseEntity, r.source, r.target, r.typ, groupByVarSet, aggregatedProperties)
    }
  }

  //todo: join doesnt work with group by after id(..)? whats with type(...) (after group by columnname is f.i. id(a) instead of a)
  def extendMatchTable(entity: ConstructedEntity with aggregationExtension, matchTable: MemRecords)(implicit context: MemRuntimeContext): MemRecords = {
    implicit val header: RecordHeader = matchTable.header
    val idExpr = Id(ListLit(StringLit(entity.v.name)() +: entity.groupBy.toIndexedSeq)())() //if id should be copied idExpr= Id(entity.baseEntity.v)

    var newData = matchTable.data

    if (entity.aggregatedProperties.nonEmpty) {
      //workaround with renamings, because group-op alters column names
      val renamings = entity.groupBy.foldLeft(Map[String, String]())((result, expr) =>
        expr match {
          case expr@Id(inner@Var(name)) => result + ("id(" + name + ")" -> name)
          case _ => result
        })

      val aggregations = entity.aggregatedProperties.foldLeft(List[(Var, Aggregator)]())((list, setItem) => list :+ (Var(setItem.variable.name + "." + setItem.propertyKey + ":STRING")() -> setItem.setValue)) //todo: use of Var() feels wrong .. get columnName via Header?
      newData = newData.group(entity.groupBy, aggregations.toSet).withColumnsRenamed(renamings) // 2 times ? .project(idExpr, entity.v.name)
        .innerJoin(matchTable.data, ListLit(entity.groupBy.toIndexedSeq)(), ListLit(entity.groupBy.toIndexedSeq)())
    }
    entity match {
      case r: ConstructedRelationshipExtended =>
        newData = newData
          .project(Id(r.source)(), "source(" + r.v.name + ")")
          .project(Id(r.target)(), "target(" + r.v.name + ")")
          .project(StringLit(r.typ.getOrElse("null"))(), "type(" + r.v.name + ")")
      case n => //_:ConstructedNodeExtended throws compiler error "unreachable code"  //todo: project labels maybe via expr haslabel(baseEntity) if baseentity exists
        n match {
          case z: ConstructedNodeExtended =>
            z.labels.foreach(label => newData = newData.project(TrueLit(), entity.v.name + ":" + label.name))
          case _ =>
        }
    }
    newData = newData.project(idExpr, entity.v.name)

    //todo: move in extra method with optional propject of id for cloning(pattern matching fails :O ... check if entityname is correct from pattern! ... problem with f.i. CTStringOrNull
    entity.baseEntity match {
      case Some(base) => {
        val thingsToCopy = matchTable.columnType.keySet.foldLeft(List[(String, Expr)]())((list, s) => {
          val labelPattern = "(\\w*):(\\w*)".r
          val propertyPattern = "(\\w*).(\\w*):(\\w*)".r
          s match {
            case labelPattern(entityname, label) =>
              if (entityname.equals(base.name)) list :+ entity.v.name + ":" + label -> HasLabel(Var(entityname)(), Label(label))(CTBoolean)
              else list
            case propertyPattern(entityname, propname, valuetype) =>
              //problem with types like CTStringOrNull ! ... get CTtype via header?
              if (entityname.equals(base.name)) list :+ entity.v.name + "." + propname + ":" + valuetype -> Property(Var(entityname)(), PropertyKey(propname))(matchTable.columnType.getOrElse(s, CTWildcard))
              list
            case _ => list
          }

        })
        thingsToCopy.foreach(x => newData = newData.project(x._2, x._1))
      }
      case None =>
    }

    MemRecords(newData, header)
  }

  //todo: throw error or maybe decide here if aggregator property wanted ... else put setItem into remaining_set
  def stringtoAggrExpr(value: String, validColumns: Map[String, CypherType]): Aggregator = {
    val aggregationPattern = "(count|sum|min|max|collect)\\Q(\\E(distinct )?(.*)\\Q)\\E".r //move in extra function or change error messages
    value match {
      case aggregationPattern(aggr, distinct, inner) =>
        val innerExpr = stringToExpr(inner, validColumns)
        aggr match {
          case "count" => Count(innerExpr, distinct != null)()
          case "sum" => Sum(innerExpr)()
          case "min" => Min(innerExpr)()
          case "max" => Max(innerExpr)()
          case "collect" => Collect(innerExpr, distinct != null)()
          case unknown => throw NotImplementedException(unknown + "aggregator not implemented yet")
        }
      case _ => throw IllegalArgumentException("no correct aggregator") //catch in outer method?
    }
  }

  def stringToExpr(value: String, validColumns: Map[String, CypherType]): Expr = {
    val propertyPattern = "(.*)\\Q.\\E(.*)".r
    val typePattern = "type\\Q(\\E(.*)\\Q)\\E".r

    //todo: find bug in regExp (found?!)
    value match {
      case propertyPattern(varName, propertyName) =>
        val matchingKeys = validColumns.keySet.filter(_.matches(value + ":.++"))
        if (matchingKeys.isEmpty) throw IllegalArgumentException("valid property for groupBy", "invalid groupBy property " + value)
        val propertyCypherType = validColumns.getOrElse(matchingKeys.head, CTWildcard)
        Property(Var(varName)(), PropertyKey(propertyName))(propertyCypherType)
      case typePattern(validTypeParameter) if validColumns.keySet.contains(value) =>
        Type(Var(validTypeParameter)())()
      case validVarParameter if validColumns.keySet.contains(validVarParameter) => Id(Var(validVarParameter)())()
      case _ => throw IllegalArgumentException("valid parameter for groupBy", "invalid groupBy parameter " + value)
    }
  }
}

object IdGenerator {
  private val current_max_id = new AtomicLong(-1)
  var storedIDs: Map[String, Long] = Map[String, Long]()

  def generateID(constructedEntityName: String, groupBy: List[String] = List()): Long = {
    var key = constructedEntityName

    if (groupBy.nonEmpty) {
      key += groupBy.foldLeft("")((x, based_value) => x + "|" + based_value); //generate with groupbyKey
      if (storedIDs.contains(key)) storedIDs.getOrElse(key, -1) //return found Id
      else { //generate new ID
        storedIDs += key -> current_max_id.incrementAndGet()
        current_max_id.get()
      }
    }
    else {
      val newID = current_max_id.incrementAndGet()
      storedIDs += key + newID -> newID //every time id fct for ungrouped var gets called --> new ID ;todo: for final version remove this line?
      newID
    }
  }

  def init(current_max: Long = -1): Unit = {
    storedIDs = Map[String, Long]()
    current_max_id.set(current_max)
  } //needed, so that next query works on empty storedIDs Map
}
