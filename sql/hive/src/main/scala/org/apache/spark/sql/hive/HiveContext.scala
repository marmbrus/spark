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

package org.apache.spark.sql.hive

import java.io.{BufferedReader, InputStreamReader, PrintStream}
import java.sql.Timestamp
import java.util.{ArrayList => JArrayList}

import org.apache.hadoop.hive.ql.parse.VariableSubstitution
import org.apache.spark.sql.catalyst.Dialect

import scala.collection.JavaConversions._
import scala.language.implicitConversions

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.Driver
import org.apache.hadoop.hive.ql.metadata.Table
import org.apache.hadoop.hive.ql.parse.VariableSubstitution
import org.apache.hadoop.hive.ql.processors._
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.hadoop.hive.serde2.io.{DateWritable, TimestampWritable}

import org.apache.spark.SparkContext
import org.apache.spark.annotation.Experimental
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.{Analyzer, EliminateSubQueries, OverrideCatalog, OverrideFunctionRegistry}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.{ExecutedCommand, ExtractPythonUdfs, QueryExecutionException, SetCommand}
import org.apache.spark.sql.hive.client._
import org.apache.spark.sql.hive.execution.{DescribeHiveTableCommand, HiveNativeCommand}
import org.apache.spark.sql.sources.{DDLParser, DataSourceStrategy}
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils


/**
 * This is the HiveQL Dialect, this dialect is strongly bind with HiveContext
 */
private[hive] class HiveQLDialect extends Dialect {
  override def parse(sqlText: String): LogicalPlan = {
    HiveQl.parseSql(sqlText)
  }
}

/**
 * An instance of the Spark SQL execution engine that integrates with data stored in Hive.
 * Configuration for Hive is read from hive-site.xml on the classpath.
 */
class HiveContext(sc: SparkContext) extends SQLContext(sc) {
  self =>

  /**
   * When true, enables an experimental feature where metastore tables that use the parquet SerDe
   * are automatically converted to use the Spark SQL parquet table scan, instead of the Hive
   * SerDe.
   */
  protected[sql] def convertMetastoreParquet: Boolean =
    getConf("spark.sql.hive.convertMetastoreParquet", "true") == "true"

  /**
   * When true, also tries to merge possibly different but compatible Parquet schemas in different
   * Parquet data files.
   *
   * This configuration is only effective when "spark.sql.hive.convertMetastoreParquet" is true.
   */
  protected[sql] def convertMetastoreParquetWithSchemaMerging: Boolean =
    getConf("spark.sql.hive.convertMetastoreParquet.mergeSchema", "false") == "true"

  /**
   * When true, a table created by a Hive CTAS statement (no USING clause) will be
   * converted to a data source table, using the data source set by spark.sql.sources.default.
   * The table in CTAS statement will be converted when it meets any of the following conditions:
   *   - The CTAS does not specify any of a SerDe (ROW FORMAT SERDE), a File Format (STORED AS), or
   *     a Storage Hanlder (STORED BY), and the value of hive.default.fileformat in hive-site.xml
   *     is either TextFile or SequenceFile.
   *   - The CTAS statement specifies TextFile (STORED AS TEXTFILE) as the file format and no SerDe
   *     is specified (no ROW FORMAT SERDE clause).
   *   - The CTAS statement specifies SequenceFile (STORED AS SEQUENCEFILE) as the file format
   *     and no SerDe is specified (no ROW FORMAT SERDE clause).
   */
  protected[sql] def convertCTAS: Boolean =
    getConf("spark.sql.hive.convertCTAS", "false").toBoolean

  /**
   * The version of the hive client that will be used to communicate with the metastore.  Note that
   * this does not necessarily need to be the same version of Hive that is used internally by
   * Spark SQL for execution.
   */
  protected[hive] def hiveVersion: String =
    getConf("spark.sql.hive.version", "0.13.1")

  @transient
  protected[sql] lazy val substitutor = new VariableSubstitution()


  /** A local instance of hive that is only used for execution. */
  protected[hive] lazy val localMetastore = {
    val temp = Utils.createTempDir()
    temp.delete()
    temp
  }

  @transient
  protected[hive] lazy val executionConf = new HiveConf()
  executionConf.set(
    "javax.jdo.option.ConnectionURL", s"jdbc:derby:;databaseName=$localMetastore;create=true")

  /**
   * The copy of the hive client that is used for execution.  Currently this must always be
   * Hive 13 as this is the version of Hive that is packaged with Spark SQL.  This copy of the
   * client is used for execution related tasks like registering temporary functions or ensuring
   * that the ThreadLocal SessionState is correctly populated.  This copy of Hive is *not* used
   * for storing peristent metadata, and only point to a dummy metastore in a temporary directory.
   */
  @transient
  protected[hive] lazy val executionHive: ClientWrapper =
    new IsolatedClientLoader(
      version = IsolatedClientLoader.hiveVersion("13"),
      isolationOn = false,
      config = Map(
        "javax.jdo.option.ConnectionURL" ->
          s"jdbc:derby:;databaseName=$localMetastore;create=true"),
      rootClassLoader = Utils.getContextOrSparkClassLoader).client.asInstanceOf[ClientWrapper]
  SessionState.setCurrentSessionState(executionHive.state)

  /**
   * The copy of the Hive client that is used to retrieve metadata from the Hive MetaStore.  This
   * The version of the Hive client that is used here must match the metastore that is configured
   * in the hive-site.xml file.
   */
  @transient
  protected[hive] lazy val metadataHive: ClientInterface = {
    // We instantiate a HiveConf here to read in the hive-site.xml file and then pass the options
    // into the isolated client loader
    val metadataConf = new HiveConf()
    val allConfig = metadataConf.iterator.map(e => e.getKey -> e.getValue).toMap

    // Config goes second to override other settings.
    // TODO: Support for loading the jars from an already downloaded location.
    IsolatedClientLoader.forVersion(hiveVersion, allConfig ++ configure).client
  }

  protected[sql] override def parseSql(sql: String): LogicalPlan = {
    super.parseSql(substitutor.substitute(hiveconf, sql))
  }

  override protected[sql] def executePlan(plan: LogicalPlan): this.QueryExecution =
    new this.QueryExecution(plan)

  /**
   * Invalidate and refresh all the cached the metadata of the given table. For performance reasons,
   * Spark SQL or the external data source library it uses might cache certain metadata about a
   * table, such as the location of blocks. When those change outside of Spark SQL, users should
   * call this function to invalidate the cache.
   */
  def refreshTable(tableName: String): Unit = {
    // TODO: Database support...
    catalog.refreshTable("default", tableName)
  }

  protected[hive] def invalidateTable(tableName: String): Unit = {
    // TODO: Database support...
    catalog.invalidateTable("default", tableName)
  }

  /**
   * Analyzes the given table in the current database to generate statistics, which will be
   * used in query optimizations.
   *
   * Right now, it only supports Hive tables and it only updates the size of a Hive table
   * in the Hive metastore.
   */
  @Experimental
  def analyze(tableName: String) {
    val relation = EliminateSubQueries(catalog.lookupRelation(Seq(tableName)))

    relation match {
      case relation: MetastoreRelation =>
        // This method is mainly based on
        // org.apache.hadoop.hive.ql.stats.StatsUtils.getFileSizeForTable(HiveConf, Table)
        // in Hive 0.13 (except that we do not use fs.getContentSummary).
        // TODO: Generalize statistics collection.
        // TODO: Why fs.getContentSummary returns wrong size on Jenkins?
        // Can we use fs.getContentSummary in future?
        // Seems fs.getContentSummary returns wrong table size on Jenkins. So we use
        // countFileSize to count the table size.
        def calculateTableSize(fs: FileSystem, path: Path): Long = {
          val fileStatus = fs.getFileStatus(path)
          val size = if (fileStatus.isDir) {
            fs.listStatus(path).map(status => calculateTableSize(fs, status.getPath)).sum
          } else {
            fileStatus.getLen
          }

          size
        }

        def getFileSizeForTable(conf: HiveConf, table: Table): Long = {
          val path = table.getPath
          var size: Long = 0L
          try {
            val fs = path.getFileSystem(conf)
            size = calculateTableSize(fs, path)
          } catch {
            case e: Exception =>
              logWarning(
                s"Failed to get the size of table ${table.getTableName} in the " +
                s"database ${table.getDbName} because of ${e.toString}", e)
              size = 0L
          }

          size
        }

        val tableParameters = relation.hiveQlTable.getParameters
        val oldTotalSize =
          Option(tableParameters.get(HiveShim.getStatsSetupConstTotalSize))
            .map(_.toLong)
            .getOrElse(0L)
        val newTotalSize = getFileSizeForTable(hiveconf, relation.hiveQlTable)
        // Update the Hive metastore if the total size of the table is different than the size
        // recorded in the Hive metastore.
        // This logic is based on org.apache.hadoop.hive.ql.exec.StatsTask.aggregateStats().
        if (newTotalSize > 0 && newTotalSize != oldTotalSize) {
          catalog.client.alterTable(
            relation.table.copy(
              properties = relation.table.properties +
                (HiveShim.getStatsSetupConstTotalSize -> newTotalSize.toString)))
        }
      case otherRelation =>
        throw new UnsupportedOperationException(
          s"Analyze only works for Hive tables, but $tableName is a ${otherRelation.nodeName}")
    }
  }

  protected[hive] def hiveconf = tlSession.get().asInstanceOf[this.SQLSession].hiveconf

  override def setConf(key: String, value: String): Unit = {
    super.setConf(key, value)
    hiveconf.set(key, value)
    executionHive.runSqlHive(s"SET $key=$value")
    metadataHive.runSqlHive(s"SET $key=$value")
  }

  /* A catalyst metadata catalog that points to the Hive Metastore. */
  @transient
  override protected[sql] lazy val catalog =
    new HiveMetastoreCatalog(metadataHive, this) with OverrideCatalog

  // Note that HiveUDFs will be overridden by functions registered in this context.
  @transient
  override protected[sql] lazy val functionRegistry =
    new HiveFunctionRegistry with OverrideFunctionRegistry {
      def caseSensitive: Boolean = false
    }

  /* An analyzer that uses the Hive metastore. */
  @transient
  override protected[sql] lazy val analyzer =
    new Analyzer(catalog, functionRegistry, caseSensitive = false) {
      override val extendedResolutionRules =
        catalog.ParquetConversions ::
        catalog.CreateTables ::
        catalog.PreInsertionCasts ::
        ExtractPythonUdfs ::
        sources.PreInsertCastAndRename ::
        Nil
    }

  override protected[sql] def createSession(): SQLSession = {
    new this.SQLSession()
  }

  /** Overridden by child classes that need to set configuration before the client init. */
  protected def configure(): Map[String, String] = Map.empty


  protected[hive] class SQLSession extends super.SQLSession {
    protected[sql] override lazy val conf: SQLConf = new SQLConf {
      override def dialect: String = getConf(SQLConf.DIALECT, "hiveql")
    }

    protected[hive] def localSession = executionHive.state

    protected[hive] def hiveconf = executionConf
  }

  override protected[sql] def dialectClassName = if (conf.dialect == "hiveql") {
    classOf[HiveQLDialect].getCanonicalName
  } else {
    super.dialectClassName
  }

  @transient
  private val hivePlanner = new SparkPlanner with HiveStrategies {
    val hiveContext = self

    override def strategies: Seq[Strategy] = experimental.extraStrategies ++ Seq(
      DataSourceStrategy,
      HiveCommandStrategy(self),
      HiveDDLStrategy,
      DDLStrategy,
      TakeOrdered,
      ParquetOperations,
      InMemoryScans,
      ParquetConversion, // Must be before HiveTableScans
      HiveTableScans,
      DataSinks,
      Scripts,
      HashAggregation,
      LeftSemiJoin,
      HashJoin,
      BasicOperators,
      CartesianProduct,
      BroadcastNestedLoopJoin
    )
  }

  protected[hive] def runSqlHive(sql: String): Seq[String] = {
    if (sql.toLowerCase.startsWith("create temporary function")) {
      executionHive.runSqlHive(sql)
    } else if (sql.trim.toLowerCase.startsWith("set")) {
      metadataHive.runSqlHive(sql)
      executionHive.runSqlHive(sql)
    } else {
      metadataHive.runSqlHive(sql)
    }
  }

  @transient
  override protected[sql] val planner = hivePlanner

  /** Extends QueryExecution with hive specific features. */
  protected[sql] class QueryExecution(logicalPlan: LogicalPlan)
    extends super.QueryExecution(logicalPlan) {

    /**
     * Returns the result as a hive compatible sequence of strings.  For native commands, the
     * execution is simply passed back to Hive.
     */
    def stringResult(): Seq[String] = executedPlan match {
      case ExecutedCommand(desc: DescribeHiveTableCommand) =>
        // If it is a describe command for a Hive table, we want to have the output format
        // be similar with Hive.
        desc.run(self).map {
          case Row(name: String, dataType: String, comment) =>
            Seq(name, dataType,
              Option(comment.asInstanceOf[String]).getOrElse(""))
              .map(s => String.format(s"%-20s", s))
              .mkString("\t")
        }
      case command: ExecutedCommand =>
        command.executeCollect().map(_(0).toString)

      case other =>
        val result: Seq[Seq[Any]] = other.executeCollect().map(_.toSeq).toSeq
        // We need the types so we can output struct field names
        val types = analyzed.output.map(_.dataType)
        // Reformat to match hive tab delimited output.
        result.map(_.zip(types).map(HiveContext.toHiveString)).map(_.mkString("\t")).toSeq
    }

    override def simpleString: String =
      logical match {
        case _: HiveNativeCommand => "<Native command: executed by Hive>"
        case _: SetCommand => "<SET command: executed by Hive, and noted by SQLContext>"
        case _ => super.simpleString
      }
  }
}


private object HiveContext {
  protected val primitiveTypes =
    Seq(StringType, IntegerType, LongType, DoubleType, FloatType, BooleanType, ByteType,
      ShortType, DateType, TimestampType, BinaryType)

  protected[sql] def toHiveString(a: (Any, DataType)): String = a match {
    case (struct: Row, StructType(fields)) =>
      struct.toSeq.zip(fields).map {
        case (v, t) => s""""${t.name}":${toHiveStructString(v, t.dataType)}"""
      }.mkString("{", ",", "}")
    case (seq: Seq[_], ArrayType(typ, _)) =>
      seq.map(v => (v, typ)).map(toHiveStructString).mkString("[", ",", "]")
    case (map: Map[_,_], MapType(kType, vType, _)) =>
      map.map {
        case (key, value) =>
          toHiveStructString((key, kType)) + ":" + toHiveStructString((value, vType))
      }.toSeq.sorted.mkString("{", ",", "}")
    case (null, _) => "NULL"
    case (d: Int, DateType) => new DateWritable(d).toString
    case (t: Timestamp, TimestampType) => new TimestampWritable(t).toString
    case (bin: Array[Byte], BinaryType) => new String(bin, "UTF-8")
    case (decimal: java.math.BigDecimal, DecimalType()) =>
      // Hive strips trailing zeros so use its toString
      HiveShim.createDecimal(decimal).toString
    case (other, tpe) if primitiveTypes contains tpe => other.toString
  }

  /** Hive outputs fields of structs slightly differently than top level attributes. */
  protected def toHiveStructString(a: (Any, DataType)): String = a match {
    case (struct: Row, StructType(fields)) =>
      struct.toSeq.zip(fields).map {
        case (v, t) => s""""${t.name}":${toHiveStructString(v, t.dataType)}"""
      }.mkString("{", ",", "}")
    case (seq: Seq[_], ArrayType(typ, _)) =>
      seq.map(v => (v, typ)).map(toHiveStructString).mkString("[", ",", "]")
    case (map: Map[_, _], MapType(kType, vType, _)) =>
      map.map {
        case (key, value) =>
          toHiveStructString((key, kType)) + ":" + toHiveStructString((value, vType))
      }.toSeq.sorted.mkString("{", ",", "}")
    case (null, _) => "null"
    case (s: String, StringType) => "\"" + s + "\""
    case (decimal, DecimalType()) => decimal.toString
    case (other, tpe) if primitiveTypes contains tpe => other.toString
  }
}
