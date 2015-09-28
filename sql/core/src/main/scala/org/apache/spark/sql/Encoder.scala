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
  def fromType(genericTypes: Seq[java.lang.reflect.Type]): Encoder[_] = {
    val returnType = genericTypes.collect {
      case p: ParameterizedType if p.toString contains classOf[JFunction[_, _]].getName =>
        p.getActualTypeArguments.last
    }.headOption.getOrElse(sys.error(s"Can't infer return type of $genericTypes"))

    val tupleArgs = returnType match {
      case p: ParameterizedType if p.toString startsWith "scala.Tuple" =>
        p.getActualTypeArguments.toSeq.asInstanceOf[Seq[java.lang.Class[_]]]
    }

    tupleArgs.size match {
      case 2 =>
        ProductEncoder.tuple(tupleArgs(0), tupleArgs(1))
    }
  }
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

class RowEncoder(val schema: StructType) extends Encoder[Row] {
  override def fromRow(row: InternalRow): Row = ???

  override def classTag: ClassTag[Row] = ???

  override def bind(ordinals: Seq[Int]): Encoder[Row] = ???

  override def toRow(value: Row): InternalRow = ???
}

case class IntEncoder(
    fieldName: String = "value",
    ordinal: Int = 0) extends Encoder[Int] {

  val record = UnsafeRow.createFromByteArray(64, 1)

  override def schema: StructType =
    StructType(
      StructField("value", IntegerType, nullable = false) :: Nil)

  override def classTag: ClassTag[Int] = ClassTag.Int

  override def bind(ordinals: Seq[Int]): Encoder[Int] = {
    assert(ordinals.size == 1)
    copy(ordinal = ordinals.head)
  }

  override final def fromRow(row: InternalRow): Int = {
    row.getInt(ordinal)
  }

  override final def toRow(value: Int): InternalRow = {
    record.setInt(ordinal, value)
    record
  }
}

case class LongEncoder(
    fieldName: String = "value",
    ordinal: Int = 0) extends Encoder[Long] {

  val record = UnsafeRow.createFromByteArray(64, 1)

  override def schema: StructType =
    StructType(
      StructField("value", LongType, nullable = false) :: Nil)

  override def classTag: ClassTag[Long] = ClassTag.Long

  override def bind(ordinals: Seq[Int]): Encoder[Long] = {
    assert(ordinals.size == 1)
    copy(ordinal = ordinals.head)
  }

  override final def fromRow(row: InternalRow): Long = {
    row.getLong(ordinal)
  }

  override final def toRow(value: Long): InternalRow = {
    record.setLong(ordinal, value)
    record
  }
}

case class StringEncoder(
    fieldName: String = "value",
    ordinal: Int = 0) extends Encoder[String] {

  val record = new SpecificMutableRow(StringType :: Nil)

  override def schema: StructType =
    StructType(
      StructField("value", StringType, nullable = false) :: Nil)

  override def classTag: ClassTag[String] = scala.reflect.classTag[String]

  override def bind(ordinals: Seq[Int]): Encoder[String] = {
    assert(ordinals.size == 1)
    copy(ordinal = ordinals.head)
  }

  override final def fromRow(row: InternalRow): String = {
    row.getString(ordinal)
  }

  override final def toRow(value: String): InternalRow = {
    val utf8String = UTF8String.fromString(value)
    record(0) = utf8String
    record
  }
}

object ProductEncoder {
  def apply[T <: Product : TypeTag] = {
    val schema = ScalaReflection.schemaFor[T].dataType.asInstanceOf[StructType]
    val mirror = typeTag[T].mirror
    val cls = mirror.runtimeClass(typeTag[T].tpe)
    new ProductEncoder[T](schema, ClassTag[T](cls))
  }

  def tuple[T1, T2](t1: Class[T1], t2: Class[T2]) = {
    val schema = StructType(
      StructField("_1", JavaTypeInference.inferDataType(t1)._1) ::
      StructField("_2", JavaTypeInference.inferDataType(t2)._1) :: Nil)
    new ProductEncoder[Tuple2[T1, T2]](schema, classTag[Tuple2[T1, T2]])
  }
}

class ProductEncoder[T <: Product](
    val schema: StructType,
    val classTag: ClassTag[T],
    val ordinals: Seq[Int]) extends Encoder[T] {

  def this(schema: StructType, classTag: ClassTag[T]) =
    this(schema, classTag, schema.indices.toArray)

  private val converter = CatalystTypeConverters.createToCatalystConverter(schema)

  @transient private lazy val constructor = {
    val cons = classTag.runtimeClass
      .getConstructors
      .filter(_.getParameterTypes.size == schema.fields.length)
      .head
    cons.asInstanceOf[Constructor[T]]
  }

  override def fromRow(row: InternalRow): T = {
    val data = ordinals.zip(schema.fields).map {
      case (o, f) =>
        f.dataType match {
          case StringType => row.get(o, f.dataType).toString
          case _ => row.get(o, f.dataType)
        }
    }
    try {
      constructor.newInstance(data: _*)
    } catch {
      case e: Exception =>
        throw new RuntimeException(
          s"""Failed to construct row.
             |Constructor: $constructor
             |Schema: $schema
             |Row: $row
             |Data: $data
             |DataTypes: ${data.map(_.getClass.getName)}
           """.stripMargin, e)
    }
  }

  // TODO: this is not respecting the bind...
  override def toRow(value: T): InternalRow = {
    converter.apply(value).asInstanceOf[InternalRow]
  }

  def bind(ordinals: Seq[Int]) = new ProductEncoder[T](schema, classTag, ordinals)
}

class JoinedEncoder[T, U](left: Encoder[T], right: Encoder[U]) extends Encoder[(T, U)] {
  val schema = StructType(left.schema.fields ++ right.schema.fields)
  val boundLeft = left
  val boundRight = right.bind(right.schema.indices.map(_ + left.schema.size))

  val classTag = scala.reflect.classTag[(T, U)]

  def fromRow(row: InternalRow): (T, U) = {
    (left.fromRow(row), right.fromRow(row))
  }

  def toRow(value: (T, U)): InternalRow = ???

  def bind(ordinal: Seq[Int]) = ???
}

import records._

class RecordEncoder[T <: UnsafeRow with Rec[_] : TypeTag] extends Encoder[T] {
  val classTag = scala.reflect.classTag[RecordRow[_]].asInstanceOf[ClassTag[T]]
  val schema: StructType = StructType(recordFields(typeOf[T]))
  val record = new RecordRow[Any](schema.fieldNames).asInstanceOf[T]
  record.pointTo(new Array[Byte](1), schema.size, 1)

  def fromRow(row: InternalRow): T = {
    val unsafeRow = row.asInstanceOf[UnsafeRow]
    record.pointTo(unsafeRow.getBaseObject, unsafeRow.getBaseOffset, unsafeRow.numFields(), unsafeRow.getSizeInBytes)
    record
  }

  // TODO: this is still doing an allocation...
  def toRow(value: T): InternalRow = value

  def bind(ordinal: Seq[Int]) = ???

  /** Determine the fields of a record */
  private def recordFields(recType: Type): Seq[StructField] = {
    val base = recType.baseType(typeOf[Rec[Any]].typeSymbol)
    base match {
      case TypeRef(_, _, List(RefinedType(_, scope))) =>
        val fields =
          for (mem <- scope if mem.isMethod)
            yield StructField(
              mem.name.encoded,
              ScalaReflection.schemaFor(mem.asMethod.returnType).dataType)
        fields.toSeq
      case _ =>
        Nil
    }
  }
}