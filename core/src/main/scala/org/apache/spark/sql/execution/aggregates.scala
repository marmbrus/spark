package org.apache.spark.sql
package execution

import org.apache.spark.SparkContext

import catalyst.errors._
import catalyst.expressions._
import catalyst.plans.physical.{UnspecifiedDistribution, ClusteredDistribution, AllTuples}
import catalyst.types._

import org.apache.spark.rdd.SharkPairRDDFunctions._

/**
 * Groups input data by `groupingExpressions` and computes the `aggregateExpressions` for each
 * group.
 *
 * @param partial if true then aggregation is done partially on local data without shuffling to
 *                ensure all values where `groupingExpressions` are equal are present.
 * @param groupingExpressions expressions that are evaluated to determine grouping.
 * @param aggregateExpressions expressions that are computed for each group.
 * @param child the input data source.
 */
case class Aggregate(
    partial: Boolean,
    groupingExpressions: Seq[Expression],
    aggregateExpressions: Seq[NamedExpression],
    child: SparkPlan)(@transient sc: SparkContext)
  extends UnaryNode {

  override def requiredChildDistribution =
    if (partial) {
      UnspecifiedDistribution :: Nil
    } else {
      if (groupingExpressions == Nil) {
        AllTuples :: Nil
      } else {
        ClusteredDistribution(groupingExpressions) :: Nil
      }
    }

  override def otherCopyArgs = sc :: Nil

  def output = aggregateExpressions.map(_.toAttribute)

  /* Replace all aggregate expressions with spark functions that will compute the result. */
  def createAggregateImplementations() = aggregateExpressions.map { agg =>
    val impl = agg transform {
      case a: AggregateExpression => a.newInstance
    }

    val remainingAttributes = impl.collect { case a: Attribute => a }
    // If any references exist that are not inside agg functions then the must be grouping exprs
    // in this case we must rebind them to the grouping tuple.
    if (remainingAttributes.nonEmpty) {
      val unaliasedAggregateExpr = agg transform { case Alias(c, _) => c }

      // An exact match with a grouping expression
      val exactGroupingExpr = groupingExpressions.indexOf(unaliasedAggregateExpr) match {
        case -1 => None
        case ordinal => Some(BoundReference(0, ordinal, Alias(impl, "AGGEXPR")().toAttribute))
      }

      exactGroupingExpr.getOrElse(
        sys.error(s"$agg is not in grouping expressions: $groupingExpressions"))
    } else {
      impl
    }
  }

  def execute() = attachTree(this, "execute") {
    // TODO: If the child of it is an [[catalyst.execution.Exchange]],
    // do not evaluate the groupingExpressions again since we have evaluated it
    // in the [[catalyst.execution.Exchange]].
    val grouped = child.execute().map { row =>
      (buildRow(groupingExpressions.map(Evaluate(_, Vector(row)))), row)
    }.groupByKeyLocally()

    val result = grouped.map { case (group, rows) =>
      val aggImplementations = createAggregateImplementations()

      // Pull out all the functions so we can feed each row into them.
      val aggFunctions = aggImplementations.flatMap(_ collect { case f: AggregateFunction => f })

      rows.foreach { row =>
        val input = Vector(row)
        aggFunctions.foreach(_.apply(input))
      }
      buildRow(aggImplementations.map(Evaluate(_, Vector(group))))
    }

    // TODO: THIS DOES NOT PRESERVE LINEAGE AND BREAKS PIPELINING.
    if (groupingExpressions.isEmpty && result.count == 0) {
      // When there there is no output to the Aggregate operator, we still output an empty row.
      val aggImplementations = createAggregateImplementations()
      sc.makeRDD(buildRow(aggImplementations.map(Evaluate(_, Nil))) :: Nil)
    } else {
      result
    }
  }
}
