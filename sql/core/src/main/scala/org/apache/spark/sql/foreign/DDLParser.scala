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

package org.apache.spark.sql.catalyst

import org.apache.spark.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.RunnableCommand

import scala.language.implicitConversions
import scala.util.parsing.combinator.lexical.StdLexical
import scala.util.parsing.combinator.syntactical.StandardTokenParsers
import scala.util.parsing.combinator.PackratParsers
import scala.util.parsing.input.CharArrayReader.EofCh

import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.types._

/**
 * A parser for foreign DDL commands.
 */
class DDLParser extends StandardTokenParsers with PackratParsers with Logging {

  def apply(input: String): Option[LogicalPlan] = {
    phrase(ddl)(new lexical.Scanner(input)) match {
      case Success(r, x) => Some(r)
      case x =>
        logDebug(s"Not recognized as DDL: $x")
        None
    }
  }

  protected case class Keyword(str: String)

  protected implicit def asParser(k: Keyword): Parser[String] =
    lexical.allCaseVersions(k.str).map(x => x : Parser[String]).reduce(_ | _)

  protected val CREATE = Keyword("CREATE")
  protected val FOREIGN = Keyword("FOREIGN")
  protected val TEMPORARY = Keyword("TEMPORARY")
  protected val TABLE = Keyword("TABLE")
  protected val USING = Keyword("USING")
  protected val OPTIONS = Keyword("OPTIONS")

  // Use reflection to find the reserved words defined in this class.
  protected val reservedWords =
    this.getClass
      .getMethods
      .filter(_.getReturnType == classOf[Keyword])
      .map(_.invoke(this).asInstanceOf[Keyword].str)

  override val lexical = new SqlLexical(reservedWords)

  protected lazy val ddl: Parser[LogicalPlan] = createTable

  /**
   * CREATE FOREIGN TEMPORARY TABLE avroTable
   * USING org.apache.spark.sql.avro
   * OPTIONS (path "../hive/src/test/resources/data/files/episodes.avro")
   */
  protected lazy val createTable: Parser[LogicalPlan] =
    CREATE ~> FOREIGN ~> TEMPORARY ~> TABLE ~> ident ~
    USING ~ className ~
    OPTIONS ~ options ^^ {
      case tableName ~ _ ~ provider ~ _ ~ opts =>
        CreateForeignTable(tableName, provider, opts)
    }

  protected lazy val options: Parser[Map[String, String]] =
  "(" ~> repsep(pair, ",") <~ ")" ^^ { case s: Seq[(String, String)] => s.toMap }

  protected lazy val className: Parser[String] = repsep(ident, ".") ^^ { case s => s.mkString(".")}

  protected lazy val pair: Parser[(String, String)] = ident ~ stringLit ^^ { case k ~ v => (k,v) }
}

case class CreateForeignTable(
    tableName: String,
    provider: String,
    options: Map[String, String]) extends RunnableCommand {

  val output = Nil

  def run(sqlContext: SQLContext) = {
    val clazz = Class.forName(provider + ".package$")
    val field = clazz.getFields.head
    val dataSource = field.get(clazz).asInstanceOf[org.apache.spark.sql.foreign.DataSource]

    dataSource.createRelation(sqlContext, options).registerTempTable(tableName)
    Seq.empty
  }
}
