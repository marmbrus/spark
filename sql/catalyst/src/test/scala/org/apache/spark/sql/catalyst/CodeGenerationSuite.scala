package org.apache.spark.sql
package catalyst
package expressions

import org.scalatest.FunSuite

import types._
import expressions._

import dsl.expressions._

class CodeGenerationSuite extends FunSuite {

  val data = Array.fill(5)(new GenericRow(Array(1, null, 1.0)))

  // TODO add to DSL
  val c1 = BoundReference(0, AttributeReference("a", IntegerType)())
  val c2 = BoundReference(1, AttributeReference("b", IntegerType)())
  val c3 = BoundReference(2, AttributeReference("c", DoubleType)())

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

  /* TODO: Just test serialization.
  test("in rdd") {
    val c1 = BoundReference(0, AttributeReference("a", IntegerType)())
    val projection = GenerateRow(c1 :: Nil)
    val rdd = TestSqlContext.sc.makeRDD((1 to 1000).map(i => new GenericRow(i :: Nil)))
    rdd.mapPartitions(projection).collect()
  }
  */
}