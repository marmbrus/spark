package org.apache.spark.sql.hive

import java.io.{InputStream, File}
import java.net.{URI, URL, URLClassLoader}
import java.util
import java.util.jar.JarEntry
import java.util.zip.{ZipEntry, ZipFile}

import org.apache.hadoop.hive.metastore.api.Database
import org.apache.hive.common.HiveVersionAnnotation

import scala.language.reflectiveCalls
import scala.collection.JavaConversions._

import org.apache.commons.io.IOUtils
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.metadata.{Hive, Table}
import org.apache.hadoop.hive.ql.session.SessionState

trait ClientInterface {
  def listTables(dbName: String): Seq[String]
  /*
  def getTable(dbName: String, tableName: String)
  def createTable(tableName: String): Unit
  def createDatabase(databaseName: String): Unit
  */
}

class ClientWrapper(config: Map[String, String]) extends ClientInterface {
  config.foreach(println)

  protected[hive] val conf = new HiveConf()
  config.foreach { case (k, v) => conf.set(k, v)}

  val properties = Seq(
    "javax.jdo.option.ConnectionURL",
    "javax.jdo.option.ConnectionDriverName",
    "javax.jdo.option.ConnectionUserName")

  println("== CONFIG ==")
  properties.foreach(p => println(s"$p = ${conf.get(p)}"))
  println(classOf[HiveVersionAnnotation].getPackage())

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

  /*
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
  */

  def listTables(dbName: String): Seq[String] = withClassLoader {
    client.getAllTables()
  }

}

class IsolatedClientLoader(execJars: Seq[File], config: Map[String, String]) {
  protected val baseClassLoader = Thread.currentThread().getContextClassLoader

  def otherJars = {
    new File("../../lib_managed/jars")
      .listFiles()
      .filter(f => f.getName.contains("datanucleus") || f.getName.startsWith("jdo-api"))
  }

  protected val jarFiles = (execJars ++ otherJars).map(new ZipFile(_))

  protected def findBytes(name: String) = {
    var result: InputStream = null
    var pos: Int = 0
    while (result == null && pos < jarFiles.length) {
      val entry = jarFiles(pos).getEntry(name)
      if (entry != null) {
        result = jarFiles(pos).getInputStream(entry)
      }
      pos += 1
    }
    result
  }

  println(getClass.getClassLoader.getParent)
  println(ClassLoader.getSystemClassLoader)
  println(ClassLoader.getSystemClassLoader.getParent)

  val hiveClassLoader = new URLClassLoader(execJars.map(_.toURL).toArray, ClassLoader.getSystemClassLoader.getParent)

  val classLoader = new ClassLoader(baseClassLoader) {
    override def getResourceAsStream(name: String) = {
      println(s"STREAM: $name")
      super.getResourceAsStream(name)
    }

    override def loadClass(name: String, resolve: Boolean): Class[_] = {
      val classFileName = name.replaceAll("\\.", "/") + ".class"
      if (name.startsWith(classOf[ClientWrapper].getName)) {
        val bytes = IOUtils.toByteArray(baseClassLoader.getResourceAsStream(classFileName))
        println(s"custom defining: $name - ${util.Arrays.hashCode(bytes)}")
        defineClass(name, bytes, 0, bytes.length)
      } else if (
          name.startsWith("org.datanucleus") ||
          name.startsWith("javax.jdo") ||
          name.startsWith("org.apache.hadoop.hive") ||
          name.startsWith("org.apache.hive")) {
        /*val inputStream = findBytes(classFileName)
        if (inputStream == null) {
          throw new ClassNotFoundException(name)
        } else {
          val bytes = IOUtils.toByteArray(inputStream)
          println(s"$execJars loading: $name - ${util.Arrays.hashCode(bytes)}")
          defineClass(name, bytes, 0, bytes.length)


        } */
        hiveClassLoader.loadClass(name)
      } else {
        println(s"delegating: $name")
        super.loadClass(name, resolve)
      }
    }
  }

  val client: ClientInterface =
    classLoader
      .loadClass(classOf[ClientWrapper].getName)
      .getConstructors.head
      .newInstance(config)
      .asInstanceOf[ClientInterface]
}