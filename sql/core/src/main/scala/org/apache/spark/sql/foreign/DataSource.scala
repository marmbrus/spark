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
package org.apache.spark.sql.foreign

import org.apache.spark.sql.SQLContext

/**
 * Implemented by objects that produce relations for a specific kind of foreign data source.  When
 * Spark SQL is given a DDL operation with a USING clause specified, this interface is used to
 * pass in the parameters specified by a user.
 */
trait DataSource {
  /** Returns a new base relation with the given parameters. */
  def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation
}
