package org.apache.spark.sql.hive.client

import java.io.File
import java.net.URLClassLoader
import java.util

import org.apache.hadoop.hive.metastore.api.Database

import scala.language.reflectiveCalls
import scala.collection.JavaConversions._

import org.apache.spark.Logging
import org.apache.spark.deploy.SparkSubmitUtils

import org.apache.commons.io.{FileUtils, IOUtils}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.metadata.{Hive, Table}
import org.apache.hadoop.hive.ql.session.SessionState

import scala.util.Try

/**
 * An externally visible interface to the Hive client.  This interface is shared across both the
 * internal and external classloaders for a given version of Hive and thus must expose only
 * shared classes.
 */
trait ClientInterface {
  def listTables(dbName: String): Seq[String]

  def getTable(dbName: String, tableName: String)

  def createTable(tableName: String): Unit

  def createDatabase(databaseName: String): Unit
}

/**
 * A class that wraps the HiveClient and converts its responses to externally visible classes.
 * Note that this class is loaded with a internal classloader for each instantiation,
 * allowing it to interact directly with a specific isolated version of Hive.  However, this means
 * that it is not visible as a `ClientWrapper`, but only as a `ClientInterface` to the rest of
 * Spark SQL.
 */
class ClientWrapper(config: Map[String, String]) extends ClientInterface with Logging {
  private val conf = new HiveConf(classOf[SessionState])
  config.foreach { case (k, v) => conf.set(k, v)}

  private def properties = Seq(
    "javax.jdo.option.ConnectionURL",
    "javax.jdo.option.ConnectionDriverName",
    "javax.jdo.option.ConnectionUserName")

  properties.foreach(p => logInfo(s"Hive Configuration: $p = ${conf.get(p)}"))

  private val state = withClassLoader {
    val newState = new SessionState(conf)
    SessionState.start(newState)
    newState
  }

  private val client = Hive.get(conf)

  private def withClassLoader[A](f: => A) = {
    val original = Thread.currentThread().getContextClassLoader
    Thread.currentThread().setContextClassLoader(getClass.getClassLoader)
    val ret = try f finally {
      Thread.currentThread().setContextClassLoader(original)
    }
    ret
  }

  def createDatabase(tableName: String) = withClassLoader {
    val table = new Table("default", tableName)
    client.createDatabase(
      new Database("default", "", new File("").toURI.toString, new java.util.HashMap), true)
  }

  def createTable(tableName: String) = withClassLoader {
    val table = new Table("default", tableName)
    client.createTable(table, true)
  }

  def getTable(dbName: String, tableName: String) = withClassLoader {
    client.getTable(dbName, tableName, false)
  }


  def listTables(dbName: String): Seq[String] = withClassLoader {
    client.getAllTables()
  }
}

/**
 * Creates isolated Hive client loaders by downloading the requested version from maven.
 */
object IsolatedClientLoader {
  private def getVersion(version: Int): Seq[File] = {
    val hiveArtifacts =
      (Seq("hive-metastore", "hive-exec", "hive-common") ++
        (if (version <= 10) "hive-builtins" :: Nil else Nil))
        .map(a => s"org.apache.hive:$a:0.$version.0") :+
        "com.google.guava:guava:14.0" :+
        "org.apache.hadoop:hadoop-client:1.0.4" :+
        "mysql:mysql-connector-java:5.1.12"

    val classpath =
      SparkSubmitUtils.resolveMavenCoordinates(
        hiveArtifacts.mkString(","),
        Some("http://www.datanucleus.org/downloads/maven2"),
        None)
    val allFiles = classpath.split(",").map(new File(_)).toSet

    // TODO: Remove copy logic.
    val tempDir = File.createTempFile("hive", "v" + version.toString)
    tempDir.delete()
    tempDir.mkdir()

    allFiles.foreach(f => FileUtils.copyFileToDirectory(f, tempDir))
    tempDir.listFiles()
  }

  private def resolvedVersions = new scala.collection.mutable.HashMap[Int, Seq[File]]

  def forVersion(version: Int, config: Map[String, String] = Map.empty) = synchronized {
    val files = resolvedVersions.getOrElseUpdate(version, getVersion(version))
    new IsolatedClientLoader(files, config)
  }
}

/**
 * Creates a Hive `ClientInterface` using a classloader that works according to the following rules:
 *  - Shared classes: Java, Scala, logging, and Spark classes are delegated to `baseClassLoader`
 *    allowing the results of calls to the `ClientInterface` to be visible externally.
 *  - Hive classes: new instances are loaded from `execJars`.  These classes are not
 *    accessible externally due to their custom loading.
 *  - ClientWrapper: a new copy is created for each instance of `IsolatedClassLoader`.
 *    This new instance is able to see a specific version of hive without using reflection. Since
 *    this is a unique instance, it is not visible externally other than as a generic
 *    `ClientInterface`.
 */
class IsolatedClientLoader(
    execJars: Seq[File],
    config: Map[String, String] = Map.empty) extends Logging {

  /** The systems root classloader, should not not know about Hive or really anything. */
  protected val rootClassLoader = ClassLoader.getSystemClassLoader.getParent.getParent
  /** The classloader that is used to load non-hive classes */
  protected val baseClassLoader = Thread.currentThread().getContextClassLoader

  // Check to make sure that the root classloader does not know about Hive.
  assert(Try(baseClassLoader.loadClass("org.apache.hive.HiveConf")).isFailure)

  /** All jars used by the hive specific classloader.*/
  protected def allJars = execJars.map(_.toURI.toURL).toArray

  def isSharedClass(name: String) =
    name.contains("slf4j") ||
    name.contains("log4j") ||
    name.startsWith("org.apache.spark.") ||
    name.startsWith("scala.") ||
    name.startsWith("java.lang.")

  /** The classloader that is used to load an isolated version of Hive. */
  protected val classLoader: ClassLoader = new URLClassLoader(allJars, rootClassLoader) {
    override def loadClass(name: String, resolve: Boolean): Class[_] = {
      val loaded = findLoadedClass(name)
      val found =
        if(loaded == null)
          doLoadClass(name, resolve)
        else
          loaded
      found
    }

    def doLoadClass(name: String, resolve: Boolean): Class[_] = {
      val classFileName = name.replaceAll("\\.", "/") + ".class"
      if (name.startsWith(classOf[ClientWrapper].getName)) {
        val bytes = IOUtils.toByteArray(baseClassLoader.getResourceAsStream(classFileName))
        logDebug(s"custom defining: $name - ${util.Arrays.hashCode(bytes)}")
        defineClass(name, bytes, 0, bytes.length)
      } else if (!isSharedClass(name)) {
        logDebug(s"hive class: $name - ${getResource(name.replaceAll("\\.", "/") + ".class")}")
        super.loadClass(name, resolve)
      } else {
        logDebug(s"shared class: $name")
        baseClassLoader.loadClass(name)
      }
    }
  }

  // Pre-reflective instantiation setup.
  logDebug("Initializing the logger to avoid disaster...")
  Thread.currentThread.setContextClassLoader(classLoader)

  /** The isolated client interface to Hive. */
  val client: ClientInterface = try {
    classLoader
      .loadClass(classOf[ClientWrapper].getName)
      .getConstructors.head
      .newInstance(config)
      .asInstanceOf[ClientInterface]
  } finally {
    Thread.currentThread.setContextClassLoader(baseClassLoader)
  }
}