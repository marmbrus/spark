package org.apache.spark.sql.hive

import java.io.File

import org.scalatest.FunSuite

class VersionsSuite extends FunSuite {
  def execJar(version: Int) =
    Seq(
      s"../../hive-common-0.$version.0.jar",
      s"../../hive-exec-0.$version.0.jar",
      s"../../hive-metastore-0.$version.0.jar").map(new File(_))

  def buildConf(version: Int) = {
    Map(
      "javax.jdo.option.ConnectionURL" -> s"jdbc:mysql://192.168.59.103:3306/hive$version",
      "javax.jdo.option.ConnectionDriverName" -> "com.mysql.jdbc.Driver",
      "javax.jdo.option.ConnectionUserName" -> "root",
      "javax.jdo.option.ConnectionPassword" -> "admin",
      "datanucleus.autoCreateSchema" -> "false")
  }

  def captureOut(f: => Unit): String = {
    ""
  }

  test("basic test") {
    val badClient = new IsolatedClientLoader(execJar(13), buildConf(13)).client
  }

  ignore("sanity check") {
    //intercept[Exception] {
      val badClient = new IsolatedClientLoader(execJar(13), buildConf(12)).client
      val badClient2 = new IsolatedClientLoader(execJar(12), buildConf(12)).client
    //}
  }

  val versions = Seq()

  versions.foreach { v =>
    import sys.process._
    Seq("/bin/bash", "-c", s"""mysql --host=192.168.59.103 -uroot --password=admin -e "SHOW DATABASES; DROP DATABASE IF EXISTS hive$v; CREATE DATABASE hive$v; SHOW DATABASES" """).!!
    Seq("/bin/bash", "-c", s"""mysql --host=192.168.59.103 -uroot --password=admin hive$v < /Users/marmbrus/workspace/hive/metastore/scripts/upgrade/mysql/hive-schema-0.$v.0.mysql.sql""").!!
  }

  versions.foreach { version =>
    val client = new IsolatedClientLoader(execJar(version), buildConf(version)).client

    test(s"$version: listTables") {
      client.listTables("default")
    }

    /*
    test(s"$version: createDatabase}") {
      client.createDatabase("default")
    }

    test(s"$version: createTable}") {
      client.createTable("src")
    }

    test(s"$version: getTable") {
      client.getTable("default", "src")
    }
    */
  }
}