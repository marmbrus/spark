package org.apache.spark.sql.hive

import java.io.{InputStream, File}
import java.net.{URI, URL, URLClassLoader}
import java.util
import java.util.zip.{ZipEntry, ZipFile}

import org.apache.hadoop.hive.metastore.api.Database
import org.apache.hive.common.HiveVersionAnnotation
import org.apache.spark.Logging
import org.apache.spark.deploy.SparkSubmitUtils
import org.apache.spark.util.Utils

import scala.language.reflectiveCalls
import scala.collection.JavaConversions._

import org.apache.commons.io.{FileUtils, IOUtils}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.metadata.{Hive, Table}
import org.apache.hadoop.hive.ql.session.SessionState

import scala.util.Try

/**
 * An externally visible interface to the Hive client.  This interface is shared across both the
 * internal and external classloaders for a given version of Hive.
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
  protected[hive] val conf = new HiveConf()
  config.foreach { case (k, v) => conf.set(k, v)}

  def properties = Seq(
    "javax.jdo.option.ConnectionURL",
    "javax.jdo.option.ConnectionDriverName",
    "javax.jdo.option.ConnectionUserName")

  properties.foreach(p => logInfo(s"Hive Configuration: $p = ${conf.get(p)}"))

  protected[hive] val client = Hive.get(conf)
  val state = withClassLoader {
    val newState = new SessionState(new HiveConf(classOf[SessionState]))
    SessionState.start(newState)
    newState
  }

  def withClassLoader[A](f: => A) = {
    val original = Thread.currentThread().getContextClassLoader
    Thread.currentThread().setContextClassLoader(getClass.getClassLoader)
    val ret = try f finally {
      Thread.currentThread().setContextClassLoader(original)
    }
    ret
  }

  def createDatabase(tableName: String) = withClassLoader {
    val table = new Table("default", tableName)
    client.createDatabase(new Database("default", "", new File("").toURI.toString, new java.util.HashMap), true)
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

object IsolatedClientLoader {
  private def getFiles(artifact: String, version: Int): Set[File] = {
    val classpath =
      SparkSubmitUtils.resolveMavenCoordinates(
        s"org.apache.hive:$artifact:0.$version.0",
        Some("http://www.datanucleus.org/downloads/maven2"),
        None)
    classpath.split(",").map(new File(_)).toSet
  }

  private def getVersion(version: Int): Seq[File] = {
    val allFiles =
      getFiles("hive-metastore", version) ++ getFiles("hive-exec", version)

    val tempDir = File.createTempFile("hive", "v" + version.toString)
    tempDir.delete()
    tempDir.mkdir()

    allFiles.foreach(f => FileUtils.copyFileToDirectory(f, tempDir))
    tempDir.listFiles()
  }

  private def resolvedVersions = new scala.collection.mutable.HashMap[Int, Seq[File]]

  def forVersion(version: Int, config: Map[String, String]) = synchronized {
    val files = resolvedVersions.getOrElseUpdate(version, getVersion(version))
    new IsolatedClientLoader(files, config)
  }
}

/**
 * Creates a Hive `ClientInterface` using a classloader that works according to the following rules:
 *  - Hive classes: new instances are loaded from `execJars`.  These classes are not
 *    accessible externally due to their custom loading.
 *  - ClientWrapper: a new copy is created for each instance of `IsolatedClassLoader`.
 *    This new instance is able to see a specific version of hive without using reflection. Since
 *    this is a unique instance, it is not visible externally other than as a generic
 *    `ClientInterface`.
 *  - All other classes, are delegated to `baseClassLoader` allowing the results of calls to the
 *    `ClientInterface` to be visible externally.
 */
class IsolatedClientLoader(
    execJars: Seq[File],
    config: Map[String, String] = Map.empty) extends Logging {

  /** The systems root classloader, should not not know about Hive really anything. */
  protected val rootClassLoader = ClassLoader.getSystemClassLoader.getParent
  /** The classloader that is used to load non-hive classes */
  protected val baseClassLoader = Thread.currentThread().getContextClassLoader

  // Check to make sure that the root classloader does not know about Hive.
  assert(Try(baseClassLoader.loadClass("org.apache.hive.HiveConf")).isFailure)

  /** Hadoop, JDO, and datanucleus jars. */
  protected def otherJars: Array[File] = {
    new File("../../lib_managed/jars")
      .listFiles()
      .filter { f =>
        f.getName.contains("hadoop") || f.getName.contains("jdo-api") || f.getName.contains("datanucleus")
    }
  }

  /** All jars used by the hive specific classloader.*/
  protected def allJars = (execJars ++ otherJars).map(_.toURI.toURL).toArray

  allJars.foreach(println)

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
      } else if (
          // name.startsWith("com.jolbox") ||
          name.startsWith("org.datanucleus") ||
          name.startsWith("javax.jdo") ||
          name.startsWith("org.apache.hadoop.hive") ||
          name.startsWith("org.apache.hive")) {
        super.loadClass(name, resolve)
      } else {
        logDebug(s"delegating: $name")
        baseClassLoader.loadClass(name)
      }
    }
  }

  /** The isolated client interface to Hive. */
  val client: ClientInterface =
    classLoader
      .loadClass(classOf[ClientWrapper].getName)
      .getConstructors.head
      .newInstance(config)
      .asInstanceOf[ClientInterface]
}