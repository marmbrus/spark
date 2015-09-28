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

import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions.{Expression, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.types._

import scala.language.experimental.macros
import scala.language.existentials
import scala.reflect._

import records._
import Macros.RecordMacros

import org.apache.spark.annotation.Experimental
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.{SimpleCatalystConf, CatalystConf, SqlParser, ScalaReflection}

import scala.language.dynamics
import scala.language.experimental.macros

import scala.collection.JavaConverters._
import scala.util.hashing.MurmurHash3

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types.StructType

object R extends Dynamic {

  /**
   * Create a "literal record" with field value pairs `v` using named
   * parameters:
   * {{{
   * Rec(name = "Hans", age = 7)
   * }}}
   */
  def applyDynamicNamed(method: String)(v: (String, Any)*): InternalRow with Rec[Any] = macro CatalystMacros.apply_impl

  val y = CatalystMacros.x5
}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions.{UnsafeRow, Expression, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.types._

import scala.language.experimental.macros
import scala.language.existentials
import scala.reflect._

import records._
import Macros.RecordMacros

import scala.language.dynamics
import scala.language.experimental.macros

import records.Compat210._
import scala.annotation.StaticAnnotation

class RecordRow[A](val names: Array[String] = Array.empty, val types: Array[DataType] = Array.empty) extends UnsafeRow with Rec[A] {
  def __dataAny(fieldName: String): Any = ???
  def __dataCount: Int = names.length
  def __dataExists(fieldName: String): Boolean = names.contains(fieldName)

  override def __dataInt(fieldName: String): Int  = getInt(names.indexOf(fieldName))
  override def __dataLong(fieldName: String): Long  = getLong(names.indexOf(fieldName))
  override def __dataObj[T : ClassTag](fieldName: String): T = getString(names.indexOf(fieldName)).asInstanceOf[T]
}

object CatalystMacros {
  // Import macros only here, otherwise we collide with Compat210.whitebox

  import scala.reflect.macros._
  import whitebox.Context

  //  class RecordMacros[C <: Context](val c: C) extends CommonMacros.Common[C] {
  /// }

  val x5 = 2

  def apply_impl(c: Context)(method: c.Expr[String])(v: c.Expr[(String, Any)]*): c.Expr[InternalRow with Rec[Any]] = {
    import c.universe._

    val unsafeRowType = typeOf[UnsafeRow]
    val internalRowType = typeOf[InternalRow]

    method.tree match {
      case Literal(Constant(str: String)) if str == "apply" =>
        val constantLiteralsMsg =
          "Records can only be constructed with constant keys (string literals)."
        val noEmptyStrMsg =
          "Records may not have a field with an empty name"

        // List(Expr[Nothing](scala.this.Tuple2.apply[String, Int]("a", i)), Expr[Nothing](scala.this.Tuple2.apply[String, Int]("b", i.+(1))))


        val params = v.map(_.tree).map {
          case q"scala.this.Tuple2.apply[..${ _ }]($a, $b)" => (a, b)
        }

        typeOf[Int]

        def getSetter(t: Type) = t match {
          case t if t <:< typeOf[Int] => newTermName("setInt")
          case t if t <:< typeOf[Long] => newTermName("setLong")
        }

        val writers = params.zipWithIndex.map {
          case ((name, expr), i) =>
            q"outputRow.${getSetter(expr.tpe.widen)}($i, $expr)"
        }

        val typeMembers = params.map {
          case (Literal(name), expr) =>
            val encName = newTermName(name.value.toString).encodedName.toTermName
            q"def $encName: ${expr.tpe.widen}"
        }

        println("========= MACRO =======")
        val byteSize = UnsafeRow.calculateBitSetWidthInBytes(2) + params.size * 8
        val resultTree = q"""
          val outputRow = new RecordRow[{..$typeMembers}]
          outputRow.pointTo(new Array[Byte]($byteSize), 2, $byteSize);
          ..$writers
          outputRow
        """

        println(resultTree)

        c.Expr[InternalRow with Rec[Any]](resultTree)

      case Literal(Constant(str: String)) =>
        val targetName = c.prefix.actualType.typeSymbol.fullName
        c.abort(NoPosition,
          s"value $str is not a member of $targetName")
      case _ =>
        val methodName = c.macroApplication.symbol.name
        c.abort(NoPosition,
          s"You may not invoke Rec.$methodName with a non-literal method name.")
    }
  }
}