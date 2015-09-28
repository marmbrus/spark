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

import java.util.{Iterator => JIterator}

import org.apache.spark.sql.catalyst.expressions.{SortOrder, Ascending, JoinedRow}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateOrdering
import org.apache.spark.sql.execution.QueryExecution

import scala.reflect.ClassTag

import org.apache.spark.annotation.Experimental
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

import org.apache.spark.api.java.function.{Function => JFunction, Function2 => JFunction2, _}

import scala.reflect.runtime.universe._

/**
 * A typed collection of data stored efficiently as a DataFrame.
 */
@Experimental
class Dataset[T] private[sql](
    @transient val sqlContext: SQLContext,
    @transient val queryExecution: QueryExecution)(
    implicit val encoder: Encoder[T]) extends Serializable {

  /**
   * Returns a new `Dataset` where each record has been mapped on to the specified type.
   */
  def as[U : Encoder] = new Dataset(sqlContext, queryExecution)(implicitly[Encoder[U]])

  /**
   * Concise syntax for chaining custom transformations.
   * {{{
   *   dataset
   *     .transform(featurize)
   *     .transform(...)
   * }}}
   */
  def transform[U](t: Dataset[T] => Dataset[U]): Dataset[U] = ???

  /**
   * Returns a new `Dataset` that only contains elements where `func` returns `true`.
   */
  def filter(func: T => Boolean): Dataset[T] = ???

  // TODO: Create custom function to avoid boxing.
  def filter(func: JFunction[T, java.lang.Boolean]): Dataset[T] = ???

  def filterNot(func: T => Boolean): Dataset[T] = ???

  /**
   * Returns a new Dataset that contains the result of applying `func` to each element.
   */
  def map[U : Encoder](func: T => U): Dataset[U] = ???


  def map[U](func: JFunction[T, U], uEncoder: Encoder[U]): Dataset[U] = ???

  /**
   * A version of map for Java that tries to infer the encoder using reflection.  Note this needs
   * a different name or it seems to break type inference for scala with the following error:
   * "The argument types of an anonymous function must be fully known. (SLS 8.5)"
   */
  def mapInfer[U](func: JFunction[T, U]): Dataset[U] = ???

  def flatMap[U : Encoder](func: T => TraversableOnce[U]): Dataset[U] = ???

  def flatMap[U](func: JFunction[T, JIterator[U]], encoder: Encoder[U]): Dataset[U] = ???

  /*****************
   * Side effects *
   ****************/

  def foreach(f: T => Unit): Unit = ???

  def foreachPartition(f: Iterator[T] => Unit): Unit = ???

  /*****************
    * aggregation *
    ****************/

  def reduce(f: (T, T) => T): T = ???

  def fold(zeroValue: T)(op: (T, T) => T): T = ???

  def groupBy[K : Encoder](func: T => K): GroupedDataset[K, T] = new GroupedDataset(this, func)

  def groupBy(cols: Column*): GroupedDataset[Row, T] = ???

  def groupBy[K : Encoder](col1: String, cols: String*): GroupedDataset[Row, T] = ???


  /*****************
   * joins *
   ****************/
  def join[U : Encoder](other: Dataset[U]): Dataset[(T, U)] = ???

  def join[U : Encoder, K : Encoder](other: Dataset[U], leftKey: T => K, rightKey: U => K) = ???

  def join[U : Encoder](other: Dataset[U], condition: Column): Dataset[(T, U)] = ???

  /*****************
    * Set operations *
    ****************/

  def distinct: Dataset[T] = ???

  def intersect(other: Dataset[T]): Dataset[T] = ???

  def union(other: Dataset[T]): Dataset[T] = ???

  def subtract(other: Dataset[T]): Dataset[T] = ???

  /*****************
    * actions      *
    ****************/

  def first(): T = ???
  def collect(): Array[T] = ???
  def take(num: Int): Array[T] = ???
}

trait Aggregator[T]

class GroupedDataset[K : Encoder, T](dataset: Dataset[T], keyFunction: T => K) extends Serializable {

  /** Specify a new encoder for key part of this [[GroupedDataset]]. */
  def asKey[L : Encoder]: GroupedDataset[L, T] = ???
  /** Specify a new encoder for value part of this [[GroupedDataset]]. */
  def asValue[U : Encoder]: GroupedDataset[K, T] = ???

  def keys: Dataset[K] = ???

  def agg[U1](agg: Aggregator[U1]): Dataset[(K, U1)] = ???
  def agg[U1, U2](agg: Aggregator[U1], agg2: Aggregator[U2]): Dataset[(K, U1, U2)] = ???
  // ... more agg functions

  def join[U](other: GroupedDataset[K, U]): Dataset[Pair[T, U]] = ???

  def cogroup[U](other: GroupedDataset[K, U])(f: (K, Iterator[T], Iterator[U])) = ???

  def mapGroups[U : Encoder](f: (K, Iterator[T]) => Iterator[U]): Dataset[U] = ???

  def countByKey: Dataset[(K, Long)] = ???
}