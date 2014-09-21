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

package org.apache.spark.sql.avro

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test._

/* Implicits */
import TestSQLContext._

class AvroSuite extends QueryTest {
  test("dsl test") {
    val results = TestSQLContext
      .avroFile("../hive/src/test/resources/data/files/episodes.avro")
      .select('title)
      .collect()

    assert(results.size === 8)
  }

  test("sql test") {
    sql(
      """
        |CREATE FOREIGN TEMPORARY TABLE avroTable
        |USING org.apache.spark.sql.avro
        |OPTIONS (path "../hive/src/test/resources/data/files/episodes.avro")
      """.stripMargin.replaceAll("\n", " "))

    assert(sql("SELECT * FROM avroTable").collect().size === 8)
  }
}