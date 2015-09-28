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
class Dataset[/* + */ T] private[sql](
    @transient val sqlContext: SQLContext,
    @transient val queryExecution: QueryExecution)(
    implicit val encoder: Encoder[T]) extends Serializable {

  def this(d: DataFrame)(implicit encoder: Encoder[T]) = this(d.sqlContext, d.queryExecution)(encoder)

  private implicit def classTag: ClassTag[T] = encoder.classTag

  /**
   * Returns a new `Dataset` where each record has been mapped on to the specified type.
   *
   * TODO: How is the mapping of fields between the two types determined, by name? ordinal?
   */
   def as[U : Encoder] = new Dataset(sqlContext, queryExecution)(implicitly[Encoder[U]])

  def dataFrame = new DataFrame(sqlContext, queryExecution)

  def toDF(): DataFrame = dataFrame

  def transform[U](t: Dataset[T] => Dataset[U]): Dataset[U] = ???

  /**
   * Returns a new `Dataset` that only contains elements where `func` returns `true`.
   */
  def filter(func: T => Boolean): Dataset[T] = {
    val enc = encoder  // to prevent closures capturing `this`.
    val filtered = rowRDD.filter(r => func(enc.fromRow(r)))
    val filteredDf = createDataFrame(filtered, enc.schema)
    new Dataset[T](dataFrame)
  }

  // TODO: Create custom function to avoid boxing.
  def filter(func: JFunction[T, java.lang.Boolean]): Dataset[T] = {
    val enc = encoder  // to prevent closures capturing `this`.
    val filtered = rowRDD.filter(r => func.call(enc.fromRow(r)))
    val filteredDf = createDataFrame(filtered, enc.schema)
    new Dataset[T](dataFrame)
  }

  def filterNot(func: T => Boolean): Dataset[T] = ???

  /**
   * Returns a new Dataset that contains the result of applying `func` to each element.
   *
   * TODO: This should apply a projection to the input to eliminate unneeded columns?
   */
  def map[U : Encoder](func: T => U): Dataset[U] = {
    val tEncoder = encoder // to prevent closures capturing `this`.
    val uEncoder = implicitly[Encoder[U]]
    val mapped = rowRDD.map(r => uEncoder.toRow(func(tEncoder.fromRow(r))))
    val mappedDF = createDataFrame(mapped, uEncoder.schema)
    new Dataset[U](mappedDF)
  }


  def map[U](func: JFunction[T, U], uEncoder: Encoder[U]): Dataset[U] = {
    val tEncoder = encoder // to prevent closures capturing `this`.
    val mapped = rowRDD.map(r => uEncoder.toRow(func.call(tEncoder.fromRow(r))))
    val mappedDF = createDataFrame(mapped, encoder.schema)
    new Dataset[U](mappedDF)(uEncoder)
  }

  // Can't be named the same as map or
  // "The argument types of an anonymous function must be fully known. (SLS 8.5)"
  def mapInfer[U](func: JFunction[T, U]): Dataset[U] = {
    val tEncoder = encoder // to prevent closures capturing `this`.
    val interfaces = func.getClass.getGenericInterfaces()
    val uEncoder = Encoder.fromType(interfaces).asInstanceOf[Encoder[U]]
    val mapped = rowRDD.map(r => uEncoder.toRow(func.call(tEncoder.fromRow(r))))
    val mappedDF = createDataFrame(mapped, encoder.schema)
    new Dataset[U](mappedDF)(uEncoder)
  }

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

  //def groupBy(keys: Column*): GroupedDataset[T] = ???


  /*****************
    * joins *
    ****************/
  //def cartisian[U : Encoder](other: Dataset[U]): Dataset[T with U] = ???
  def cartisian[U : Encoder](other: Dataset[U]): Dataset[(T, U)] = ???
  // TODO: Other join types, cogroup


  def join[U : Encoder, K : Encoder](other: Dataset[U], leftKey: T => K, rightKey: U => K) = {

  }

  def join[U : Encoder](other: Dataset[U], condition: Column): Dataset[(T, U)] = {
    val joined = dataFrame.join(other.dataFrame, condition)
    val tEncoder = implicitly[Encoder[T]]
    val uEncoder = implicitly[Encoder[U]]
    val joinedEncoder = new JoinedEncoder(tEncoder, uEncoder.bind(uEncoder.schema.fields.indices.map(_ + tEncoder.schema.size)))
    new Dataset(joined)(joinedEncoder)
  }



  /*****************
    * Set operations *
    ****************/

  def distinct: Dataset[T] = new Dataset(dataFrame.distinct())(encoder)

  // unionAll?
  def union(other: Dataset[T]) = new Dataset(dataFrame unionAll other.dataFrame)(encoder)

  // Alias? Intersecion
  def intersect(other: Dataset[T]): Dataset[T] = ???
  def intersect[U : Encoder](other: Dataset[U]) = ???


  def repartition: Dataset[T] = ???

  /*****************
    * collecting *
    ****************/

  def first(): T = ???
  def collect(): Array[T] = rowRDD.collect().map(encoder.fromRow)
  def take(num: Int): Array[T] = ???

  private def rowRDD: RDD[InternalRow] = dataFrame.queryExecution.executedPlan.execute()
  private def createDataFrame(rdd: RDD[InternalRow], schema: StructType) =
    sqlContext.internalCreateDataFrame(rdd, schema)
}

trait Aggregator[T]

class GroupedDataset[K : Encoder, T](dataset: Dataset[T], keyFunction: T => K) extends Serializable {
  implicit def tEncoder: Encoder[T] = dataset.encoder

  def keys: Dataset[K] = dataset.map(keyFunction).distinct

  def agg[U1](agg: Aggregator[U1]): Dataset[(K, U1)] = ???
  def agg[U1, U2](agg: Aggregator[U1], agg2: Aggregator[U2]): Dataset[(K, U1, U2)] = ???

  def cogroup[U](other: GroupedDataset[K, U])(f: (K, Iterator[T], Iterator[U])) = ???

  // TODO: should be iterable?
  def mapGroups[U : Encoder](f: (K, Iterator[T]) => Iterator[U]): Dataset[U] = {
    val kEncoder = implicitly[Encoder[K]]
    val uEncoder = implicitly[Encoder[U]]
    implicit val uTag = uEncoder.classTag

    val tOffset = tEncoder.bind(tEncoder.schema.indices.map(_ + keySize))
    val keys = dfWithKeys.logicalPlan.output.take(keySize).map(Column(_))
    val keyExprs = keys.map(_.expr)
    val inputSchema = dfWithKeys.orderBy(keys: _*).queryExecution.analyzed.output
    // TODO: this does not need to be a global sort...
    val rdd = dfWithKeys.orderBy(keys: _*).queryExecution.executedPlan.execute().mapPartitions { sortedRows =>
      val sortOrder = keyExprs.map(SortOrder(_, Ascending))
      val keyOrdering = GenerateOrdering.generate(sortOrder, inputSchema)

      if (sortedRows.hasNext) {
        new Iterator[InternalRow] {
          var currentRow = sortedRows.next()
          var currentGroup = currentRow.copy()
          var currentIterator: Iterator[U] = null

          def hasNext = {
            (currentIterator != null && currentIterator.hasNext) || fetchNextGroupIterator
          }

          def next(): InternalRow = {
            assert(hasNext)
            uEncoder.toRow(currentIterator.next())
          }

          def fetchNextGroupIterator(): Boolean = {
            if (sortedRows.hasNext) {
              val inputIterator = new Iterator[T] {
                def hasNext = {
                  (currentRow != null && keyOrdering.compare(currentGroup, currentRow) == 0) ||
                  fetchNextRowInGroup()
                }

                def fetchNextRowInGroup(): Boolean = {
                  if (sortedRows.hasNext) {
                    currentRow = sortedRows.next()
                    if (keyOrdering.compare(currentGroup, currentRow) == 0) {
                      true
                    } else {
                      currentIterator = null
                      currentGroup = currentRow.copy()
                      false
                    }
                  } else {
                    false
                  }
                }

                def next(): T = {
                  assert(hasNext)
                  val res = tOffset.fromRow(currentRow)
                  currentRow = null
                  res
                }
              }

              currentIterator = f(kEncoder.fromRow(currentGroup), inputIterator)

              if (currentIterator.hasNext) {
                true
              } else if (currentRow != null) {
                currentGroup = currentRow.copy()
                true
              } else {
                if (sortedRows.hasNext) {
                  fetchNextGroupIterator()
                } else {
                  false
                }
              }
            } else {
              false
            }
          }


        }
      } else {
        Iterator.empty
      }
    }

    new Dataset(createDataFrame(rdd, uEncoder.schema))(uEncoder)
  }

  // Needs to have () for compatibility...
  def countByKey: Dataset[(K, Long)] = {
    val kEncoder = implicitly[Encoder[K]]
    val keys = dfWithKeys.logicalPlan.output.take(keySize).map(Column(_))
    val withCount = dfWithKeys.groupBy(keys: _*).count()
    val countEncoder = (new LongEncoder).bind(Seq(keySize))
    new Dataset(withCount)(new JoinedEncoder(kEncoder, countEncoder))
  }

  val keySize = {
    val kEncoder = implicitly[Encoder[K]]
    kEncoder.schema.size
  }

  private lazy val dfWithKeys: DataFrame = {
    val kEncoder = implicitly[Encoder[K]]

    val withKey = dataset.dataFrame.queryExecution.executedPlan.execute().mapPartitions { iter =>
      val joined = new JoinedRow

      iter.map { row =>
        joined(kEncoder.toRow(keyFunction(tEncoder.fromRow(row))), row): InternalRow
      }
    }

    createDataFrame(withKey, StructType(kEncoder.schema ++ tEncoder.schema))
  }

  private def dataFrame = dataset.dataFrame
  private def rowRDD: RDD[InternalRow] = dataFrame.queryExecution.executedPlan.execute()
  private def createDataFrame(rdd: RDD[InternalRow], schema: StructType) =
    sqlContext.internalCreateDataFrame(rdd, schema)
  private def sqlContext = dataFrame.sqlContext
}