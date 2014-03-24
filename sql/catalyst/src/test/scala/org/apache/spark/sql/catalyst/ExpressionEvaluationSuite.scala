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
package catalyst
package expressions

import org.scalatest.FunSuite
import types._
import expressions._
import dsl._
import dsl.expressions._


abstract class ExprEval(exprs: Array[Expression]) {
  type Execution = (Row => Row)

  def engine: Execution

  private val compare: PartialFunction[(Int, (Boolean, Any), Row), Boolean] = {
    case (idx, (false, field), row) => {
      (row.isNullAt(idx) == false) && (row.apply(idx) == field)
    }
    case (idx, (true, _), row) => {
      row.isNullAt(idx)
    }
  }

  def verify(expected: Array[(Boolean, Any)], result: Row) {
    Seq.tabulate(expected.size) { i =>
      assert(compare.lift(i, expected(i), result).get)
    }
  }

  def verify(expected: Array[Array[(Boolean, Any)]], inputs: Array[Row]) {
    val result = inputs.map(engine.apply(_))

    expected.zip(result).foreach { case (expectedRow, row) => verify(expectedRow, row) }
  }
}

case class CGExprEval(exprs: Array[Expression]) extends ExprEval(exprs) {
  override def engine: Execution = GenerateProjection(exprs)
}

case class InterpretEngine(exprs: Array[Expression]) extends ExprEval(exprs) {
  override def engine: Execution = new InterpretedProjection(exprs)
}

class ExpressionEvaluationSuite extends FunSuite {
  val data = Array.fill[Row](5)(new GenericRow(Array(1, null, 1.0, true, 4, 5, null, "abcccd")))

  // TODO add to DSL
  val c1 = BoundReference(0, AttributeReference("a", IntegerType)())
  val c2 = BoundReference(1, AttributeReference("b", IntegerType)())
  val c3 = BoundReference(2, AttributeReference("c", DoubleType)())
  val c4 = BoundReference(3, AttributeReference("d", BooleanType)())
  val c5 = BoundReference(4, AttributeReference("e", IntegerType)())
  val c6 = BoundReference(5, AttributeReference("f", IntegerType)())
  val c7 = BoundReference(6, AttributeReference("g", StringType)())
  val c8 = BoundReference(7, AttributeReference("h", StringType)())

  test("simple") {
    val generator =
      GenerateProjection(Array(c1, c2, c3, Add(c1, c1), Add(c1, c2), Add(c2, c2), Subtract(c1, c2)))
    val generatedRow = generator(data.head)

    // TODO: Factor out or use :javap?
    val generatedClass = generatedRow.getClass
    val classLoader =
      generatedClass
        .getClassLoader
        .asInstanceOf[scala.tools.nsc.interpreter.AbstractFileClassLoader]
    val generatedBytes = classLoader.classBytes(generatedClass.getName)

    val outfile = new java.io.FileOutputStream("generated.class")
    outfile.write(generatedBytes)
    outfile.close()

    println(generatedRow.length)
    println(generatedRow)
    println(generatedRow.getInt(0))
    println(generatedRow.isNullAt(1))
  }
  
  def verify(exprs: Array[Expression], expecteds: Array[Array[(Boolean, Any)]], input: Array[Row]) {
    val eval1 = CGExprEval(exprs)
    val eval2 = InterpretEngine(exprs)
    
    eval1.verify(expecteds, input)
    eval2.verify(expecteds, input)
  }
  
  test("logical") {
    val expecteds = Array.fill(5)(Array[(Boolean, Any)](
        (false, false), 
        (true, -1), 
        (false, true), 
        (false, true), 
        (false, false)))
    val exprs = Array[Expression](And(LessThan(Cast(c1, DoubleType), c3), LessThan(c1, c2)), 
      Or(LessThan(Cast(c1, DoubleType), c3), LessThan(c1, c2)),
      IsNull(c2),
      IsNotNull(c3),
      Not(c4))
    
    verify(exprs, expecteds, data)
  }
  
  test("arithmetic") {
    val exprs = Array[Expression](
      Add(c1, c2),
      Add(c1, c5),
      Divide(c1, c5),
      Subtract(c1, c5),
      Multiply(c1, c5),
      Remainder(c1, c5),
      UnaryMinus(c1)
    )
    val data = Array.fill[Row](5)(new GenericRow(Array(1, null, 1.0, true, 4, 5)))
    val expecteds = Array.fill(5)(Array[(Boolean, Any)](
        (true, 0), 
        (false, 5), 
        (false, 0), 
        (false, -3), 
        (false, 4),
        (false, 1),
        (false, -1)))

    verify(exprs, expecteds, data)
  }

  test("string like / rlike") {
    val exprs = Array[Expression](
      Like(c7, Literal("a", StringType)),
      Like(c7, Literal(null, StringType)),
      Like(c8, Literal(null, StringType)),
      Like(c8, Literal("a_c", StringType)),
      Like(c8, Literal("a%c", StringType)),
      RLike(c7, Literal("a+", StringType)),
      RLike(c7, Literal(null, StringType)),
      RLike(c8, Literal(null, StringType)),
      RLike(c8, Literal("a%c", StringType))
    )

    val expecteds = Array.fill(data.length)(Array[(Boolean, Any)](
      (true, false),
      (true, false),
      (true, false),
      (false, true),
      (false, true),
      (true, false),
      (true, false),
      (true, true)))
    verify(exprs, expecteds, data)
  }

  test("ordering") {
    val ordering = GenerateOrdering(Seq(c1, c2, Subtract(c1, c1)).map(_.asc))

    val generatedClass = ordering.getClass
    val classLoader =
      generatedClass
        .getClassLoader
        .asInstanceOf[scala.tools.nsc.interpreter.AbstractFileClassLoader]
    val generatedBytes = classLoader.classBytes(generatedClass.getName)
    val outfile = new java.io.FileOutputStream("ordering.class")
    outfile.write(generatedBytes)
    outfile.close()
    data.toArray.sorted(ordering)
  }
  
  test("literals") {
    assert((Literal(1) + Literal(1)).apply(null) === 2)
  }

  /**
   * Checks for three-valued-logic.  Based on:
   * http://en.wikipedia.org/wiki/Null_(SQL)#Comparisons_with_NULL_and_the_three-valued_logic_.283VL.29
   *
   * p       q       p OR q  p AND q  p = q
   * True    True    True    True     True
   * True    False   True    False    False
   * True    Unknown True    Unknown  Unknown
   * False   True    True    False    False
   * False   False   False   False    True
   * False   Unknown Unknown False    Unknown
   * Unknown True    True    Unknown  Unknown
   * Unknown False   Unknown False    Unknown
   * Unknown Unknown Unknown Unknown  Unknown
   *
   * p       NOT p
   * True    False
   * False   True
   * Unknown Unknown
   */

  val b1 = BoundReference(0, AttributeReference("a", BooleanType)())
  val b2 = BoundReference(1, AttributeReference("b", BooleanType)())
  
  test("3VL Not") {
    val table = (true, false) :: (false, true) :: (null, null) :: Nil

    val exprs = Array[Expression](Not(b1))
    val inputs = table.map { case(v, answer) => new GenericRow(Array(v)) }
    val expected = table.map { case(v, answer) => Array((answer == null, answer)) }
    
    verify(exprs, expected.toArray, inputs.toArray)
  }

  test("3VL AND") {
    val table = (true,  true,  true) ::
    (true,  false, false) ::
    (true,  null,  null) ::
    (false, true,  false) ::
    (false, false, false) ::
    (false, null,  false) ::
    (null,  true,  null) ::
    (null,  false, false) ::
    (null,  null,  null) :: Nil
    
    val exprs = Array[Expression](And(b1, b2))
    val inputs = table.map { case(v1, v2, answer) => new GenericRow(Array(v1, v2)) }
    val expected = table.map { case(v1, v2, answer) => Array((answer == null, answer)) }
    
    verify(exprs, expected.toArray, inputs.toArray)
  }

  test("3VL OR") {
    val table = (true,  true,  true) ::
    (true,  false, true) ::
    (true,  null,  true) ::
    (false, true,  true) ::
    (false, false, false) ::
    (false, null,  null) ::
    (null,  true,  true) ::
    (null,  false, null) ::
    (null,  null,  null) :: Nil
    
    val exprs = Array[Expression](Or(b1, b2))
    val inputs = table.map { case(v1, v2, answer) => new GenericRow(Array(v1, v2)) }
    val expected = table.map { case(v1, v2, answer) => Array((answer == null, answer)) }
    
    verify(exprs, expected.toArray, inputs.toArray)
  }
    
  test("3VL Equals") {
    val table = (true,  true,  true) ::
    (true,  false, false) ::
    (true,  null,  null) ::
    (false, true,  false) ::
    (false, false, true) ::
    (false, null,  null) ::
    (null,  true,  null) ::
    (null,  false, null) ::
    (null,  null,  null) :: Nil
    
    val exprs = Array[Expression](Equals(b1, b2))
    val inputs = table.map { case(v1, v2, answer) => new GenericRow(Array(v1, v2)) }
    val expected = table.map { case(v1, v2, answer) => Array((answer == null, answer)) }
    
    verify(exprs, expected.toArray, inputs.toArray)
  }
}