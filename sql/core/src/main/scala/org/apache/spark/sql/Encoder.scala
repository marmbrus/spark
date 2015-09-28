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

import java.lang.reflect.{ParameterizedType, Constructor}

import org.apache.spark.annotation.Experimental
import org.apache.spark.api.java.function.{Function => JFunction, Function2 => JFunction2, _}
import org.apache.spark.sql.catalyst.expressions.{SpecificMutableRow, UnsafeRow}
import org.apache.spark.sql.catalyst._
import org.apache.spark.sql.catalyst.expressions.UnsafeRowWriters.UTF8StringWriter
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.language.implicitConversions
import scala.reflect._
import scala.reflect.runtime.universe._

object Encoder {
  def fromType(genericTypes: Seq[java.lang.reflect.Type]): Encoder[_] = ???
}

/**
 * Captures how to encode JVM objects as Spark SQL rows.
 * TODO: Make unsafe row?
 */
@Experimental
trait Encoder[T] extends Serializable {
  def schema: StructType

  def fromRow(row: InternalRow): T

  def toRow(value: T): InternalRow

  def classTag: ClassTag[T]

  // TODO: Use attribute references
  def bind(ordinals: Seq[Int]): Encoder[T]
}

object ProductEncoder {
  def apply[T <: Product : TypeTag] = ???

  def tuple[T1, T2](t1: Class[T1], t2: Class[T2]): ProductEncoder[(T1, T2)] = ???
  def tuple[T1, T2, T3](t1: Class[T1], t2: Class[T2], t3: Class[T3]): ProductEncoder[(T1, T2, T3)] = ???

}

class ProductEncoder[T <: Product]

/**
 * Represents a pair of objects that are encoded as a flat row.  Pairs are created to facilitate
 * operations that calculate a grouping key, such as joins or aggregations.
 */
class Pair[L, R](val left: L, val right: R)

class PairEncoder[T, U](left: Encoder[T], right: Encoder[U]) extends Encoder[Pair[T, U]] {
  override def schema: StructType = ???

  override def fromRow(row: InternalRow): Pair[T, U] = ???

  override def classTag: ClassTag[Pair[T, U]] = ???

  override def bind(ordinals: Seq[Int]): Encoder[Pair[T, U]] = ???

  override def toRow(value: Pair[T, U]): InternalRow = ???
}
