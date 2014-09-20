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

import org.apache.avro.Schema
import org.apache.avro.file.DataFileReader
import org.apache.avro.generic.{GenericRecord, GenericDatumReader}
import org.apache.avro.mapred.FsInput

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.foreign._
import org.apache.spark.sql.test.TestSQLContext

import scala.collection.JavaConversions._

/**
 * An example of a Spark SQL foreign data source for reading data stored in Avro.  Currently this
 * is only for illustrative purposes and much of the data type conversion logic still needs to be
 * written.
 */
package object avro extends DataSource {

  /**
   * Creates a new relation for data store in avro given a `path` as a parameter.
   */
  def createRelation(sqlContext: SQLContext, parameters: Map[String, String]) = {
    AvroRelation(parameters("path"))(sqlContext)
  }

  /**
   * Adds a method, `avroFile`, to SQLContext that allows reading data stored in Avro.
   */
  implicit class AvroContext(sqlContext: SQLContext) {
    def avroFile(filePath: String) = AvroRelation(filePath)(sqlContext)
  }

  case class AvroRelation(location: String)(val sqlContext: SQLContext)
    extends BaseRelation with TableScan {

    val schema = {
      val fileReader = newReader()
      val convertedSchema = toSqlType(newReader().getSchema) match {
        case s: StructType => s
        case other =>
          sys.error(s"Avro files must contain Records to be read, type $other not supported")
      }
      fileReader.close()
      convertedSchema
    }

    val buildScan = {
      val baseRdd = sqlContext.sparkContext.hadoopFile(
        location,
        classOf[org.apache.avro.mapred.AvroInputFormat[GenericRecord]],
        classOf[org.apache.avro.mapred.AvroWrapper[GenericRecord]],
        classOf[org.apache.hadoop.io.NullWritable],
        10)
      baseRdd.map { record =>
        val values = (0 until schema.fields.size).map { i =>
          record._1.datum().get(i) match {
            case u: org.apache.avro.util.Utf8 => u.toString
            case other => other
          }
        }

        Row.fromSeq(values)
      }
    }

    private def newReader() = {
      val path = new Path(location)
      val input = new FsInput(path, sqlContext.sparkContext.hadoopConfiguration)
      val reader = new GenericDatumReader[GenericRecord]()
      DataFileReader.openReader(input, reader)
    }

    private def toSqlType(avroSchema: Schema): DataType = {
      import Schema.Type._

      avroSchema.getType match {
        case RECORD =>
          val fields = avroSchema.getFields.map { f =>
            StructField(f.name, toSqlType(f.schema()))
          }

          StructType(fields)

        case INT => IntegerType
        case STRING => StringType
        // TODO: Other data types, nullability...
        case other => sys.error(s"Unsupported type $other")
      }
    }
  }
}
