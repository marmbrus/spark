package org.apache.spark.sql.catalyst
package expressions

import scala.concurrent._
import ExecutionContext.Implicits.global

import dsl.expressions._
import dsl.plans._
import util._

object ExpressionBenchmark {
  val resultBuffer = new scala.collection.mutable.ArrayBuffer[PerformanceMeasurement]()

  val resultThread = future {
    val a = 1
    val b = 2
    (1 to 10).foreach { _ =>
      val iterations = 1000000000

      resultBuffer += measure("while") {
        var i = 1L
        var r: Int = 0
        while(i < iterations) {
          r = a + b
          i += 1
        }
      }

      resultBuffer += measure("foreach") {
        var r: Int = 0
        (1 to iterations).foreach { _ =>
          r = a + b
        }
      }


      val currentRow = new GenericMutableRow(1)
      currentRow.setInt(0, 1)
      resultBuffer += measure("hand-written evaluation / generic mutable storage") {
        val customMutableGenericProjection = (row: Row) => {
          if(!currentRow.isNullAt(0) && !currentRow.isNullAt(0)) {
            // Not quite fair...
            currentRow.setInt(0, currentRow.getInt(0) + currentRow.getInt(0))
          }
          currentRow
        }

        var r: Row = null
        (1 to iterations).foreach { _ =>
          r = customMutableGenericProjection(EmptyRow)
        }
      }

      val addFunction = (1 to 3).map(_ => BoundReference(0, 'a.int)).reduceLeft(Add)

      resultBuffer += measure("code gen time") { GenerateMutableProjection(addFunction :: Nil) }

      val projectionBuilder = GenerateMutableProjection(addFunction :: Nil)
      val inputRow = new GenericRow(Array[Any](1))

      resultBuffer += measure("cached codegen") { projectionBuilder }

      resultBuffer += measure("mutable projection built with scala reflection") {
        var r: Row = null
        val generatedProjection = projectionBuilder()
        (1 to iterations).foreach { _ =>
          r = generatedProjection(inputRow)
        }
      }

      val immutableProjection = GenerateProjection(addFunction :: Nil)

      resultBuffer += measure("immutable projection built with scala reflection") {
        var r: Row = null
        (1 to iterations).foreach { _ =>
          r = immutableProjection(inputRow)
        }
      }

      resultBuffer += measure(s"interpreted evaluation - $addFunction") {
        var r: Int = 0
        (1 to iterations).foreach { _ =>
          r = addFunction.apply(inputRow).asInstanceOf[Int]
        }
      }

      resultBuffer += measure("MutableProjection") {
        val projection = new InterpretedMutableProjection(addFunction :: Nil)
        var r: Row = null
        (1 to iterations).foreach { _ =>
          r = projection(inputRow)
        }
      }

      resultBuffer += measure("Projection (immutable result)") {
        val projection = new InterpretedProjection(addFunction :: Nil)
        var r: Row = null
        (1 to iterations).foreach { _ =>
          r = projection(inputRow)
        }
      }

      resultBuffer += measure("immutable+functional style projection") {
        val exprList = addFunction :: Nil
        var r: Row = null
        (1 to iterations).foreach { _ =>
          r = new GenericRow(exprList.map(_.apply(inputRow): Any).toArray)
        }
      }

      true
    }
  }

  // TODO FIX: captialization bug with Hive.
  case class PerformanceMeasurement(operation: String, timens: Long)
  protected def measure[A](operationName: String)(f: => A): PerformanceMeasurement = {
    val startTime = System.nanoTime()
    val result = f
    val endTime = System.nanoTime()
    PerformanceMeasurement(operationName, endTime - startTime)
  }

  def main(args: Array[String]) {
    while(!resultThread.isCompleted) {
      resultBuffer.map(r => s"${r.operation} - ${r.timens/1000000000.0}").foreach(println)
      println()
      Thread.sleep(10000)
    }

    println("DONE")
    println(resultThread.value.get.get)
  }
}