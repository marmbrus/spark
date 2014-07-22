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

import org.scalatest.FunSuite

import org.apache.spark.sql.test.TestSQLContext

case class Person(name: String, age: Int)

case class Car(owner: Person, model: String)

class TypedSqlSuite extends FunSuite {
  import TestSQLContext._

  val people = sparkContext.parallelize(
    Person("Michael", 30) ::
    Person("Bob", 40) :: Nil)

  val cars = sparkContext.parallelize(
    Car(Person("Michael", 30), "GrandAm") :: Nil)

  test("typed query") {
    val results = sql"SELECT name FROM $people WHERE age = 30"
    assert(results.first().name == "Michael")
  }

  test("int results") {
    val results = sql"SELECT * FROM $people WHERE age = 30"
    assert(results.first().name == "Michael")
    assert(results.first().age == 30)
  }

  ignore("nested results") {
    val results = sql"SELECT * FROM $cars"
    assert(results.first().owner.name === "Michael")
  }

  test("join query") {
    val results = sql"""SELECT a.name FROM $people a JOIN $people b ON a.age = b.age"""

    assert(results.first().name == "Michael")
  }
}