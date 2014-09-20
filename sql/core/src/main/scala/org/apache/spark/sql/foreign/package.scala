/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions.{Expression, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.LeafNode

/**
 * A set of APIs for adding foreign data sources to Spark SQL.
 */
package object foreign {

  // Should be in SQLContext
  implicit def toSchemaRDD(baseRelation: BaseRelation): SchemaRDD = {
    baseRelation.sqlContext.logicalPlanToSparkQuery(LogicalRelation(baseRelation))
  }

  /**
   * Represents a relation in a foreign data source.  Classes that extend BaseRelation must be able
   * to produce the schema of their data as well as a SQLContext.
   */
  @DeveloperApi
  abstract class BaseRelation {
    def sqlContext: SQLContext
    def schema: StructType
  }

  /**
   * Mixed into a BaseRelation that can produce all of its tuples as an RDD of Row objects.
   */
  @DeveloperApi
  trait TableScan {
    self: BaseRelation =>

    def buildScan(): RDD[Row]
  }

  /**
   * Mixed into a BaseRelation that can eliminate unneeded columns before producing an RDD
   * containing all of its tuples as Row objects.
   */
  @DeveloperApi
  trait PrunedScan {
    self: BaseRelation =>

    def buildScan(requiredColumns: Seq[AttributeReference]): RDD[Row]
  }

  /**
   * Mixed into a BaseRelation that can eliminate unneeded columns and filter using selected
   * predicates before producing an RDD containing all matching tuples as Row objects.
   */
  @DeveloperApi
  trait FilteredScan {
    self: BaseRelation =>

    def buildScan(
      requiredColumns: Seq[AttributeReference],
      availableFilters: Seq[Expression] => Seq[Expression]): RDD[Row]
  }

  // Belongs in catalyst?
  @DeveloperApi
  case class LogicalRelation(relation: BaseRelation)
    extends LogicalPlan
    with LeafNode[LogicalPlan]
    with MultiInstanceRelation {

    val output = relation.schema.toAttributes

    def newInstance() = LogicalRelation(relation).asInstanceOf[this.type]
  }
}
