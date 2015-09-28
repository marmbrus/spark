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

package test.org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions.UnsafeRow

import scala.reflect.runtime.universe.TypeTag

import records._

import org.apache.spark.sql._
import org.apache.spark.sql.test.SharedSQLContext

case class MyClass(b: Int, a: Int)

case class UppercaseLetter(a: Int, letter: String)
case class LowercaseLetter(b: Int, Letter: String)

class DatasetSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  implicit def productEncoder[T <: Product : TypeTag] = ProductEncoder[T]
  implicit def recordEncoder[A <: UnsafeRow with Rec[_] : TypeTag] = new RecordEncoder[A]
  implicit def intEncoder = new IntEncoder
  implicit def stringEncoder = new StringEncoder

  test("create a dataset with tuples") {
    val df = Seq((1, 2)).toDF("a", "b")
    val mapped = df.as[(Int, Int)].map { case (a, b) => (a, b, a + b) }

    checkAnswer(
      mapped.toDF(),
      Row(1, 2, 3) :: Nil)
  }

  test("create a dataset with a case class") {
    val df = Seq(MyClass(a = 1, b = 2)).toDF()
    val mapped = df.as[MyClass].map { r => (r.a, r.b) }

    checkAnswer(
      mapped.toDF(),
      Row(1, 2) :: Nil)
  }

  test("create a dataset using records") {
    val ds = Seq(R(a = 2, b = 1L)).ds
    checkAnswer(
      ds.map(r => R(c = r.a, d = r.b * r.a)).toDF(),
      Row(2, 2L) :: Nil)
  }

  test("join test") {
    val ds1 = Seq(UppercaseLetter(1, "A"), UppercaseLetter(2, "B")).ds
    val ds2 = Seq(LowercaseLetter(1, "a"), LowercaseLetter(2, "b")).ds

    val joined = ds1.join(ds2, $"a" === $"b")

    checkAnswer(
      joined.toDF(),
      Row(1, "A", 1, "a") :: Row(2, "B", 2, "b") :: Nil)

    val mapped = joined.map {
      case (u, l) => (u.letter, l.Letter)
    }

    checkAnswer(
      mapped.toDF(),
      Row("A", "a") :: Row("B", "b") :: Nil)
  }

  test("int encoder") {
    val enc = new IntEncoder
    val one = enc.toRow(1)
    assert(enc.fromRow(one) === 1)
  }

  test("primitive encoder test") {
    val ints = Seq(1, 2).ds
    checkAnswer(
      ints.toDF().select('value),
      Row(1) :: Row(2) :: Nil)
  }

  test("group and count") {
    val ds = Seq(1, 2, 3, 4, 5).ds

    // Count number of odd numbers.
    checkAnswer(
      ds.groupBy(_ % 2).countByKey.toDF(),
      Row(0, 2L) :: Row(1, 3L) :: Nil)
  }

  test("mapGroups") {
    val ds = Seq(1, 2, 3, 4, 5).ds

    val stringed = ds.groupBy(_ % 2).mapGroups { (rem, nums) =>
      val t = if (rem == 0) "even: " else "odd: "
      Iterator(t + nums.mkString(", "))
    }

    checkAnswer(
      stringed.toDF(),
      Row("even: 2, 4") :: Row("odd: 1, 3, 5") :: Nil)
  }
}