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

  test("failure sanity check") {
    val e = intercept[Exception] {
      val badClient = IsolatedClientLoader.forVersion(13, buildConf(12)).client
      badClient.createTable("src")
    }

    // lol... class loaders
    assert(e.getClass.getName == "org.apache.hadoop.hive.ql.metadata.HiveException")
  }

  val versions = Seq(12, 13)

  versions.foreach { v =>
    import sys.process._
    Seq("/bin/bash", "-c", s"""mysql --host=192.168.59.103 -uroot --password=admin -e "SHOW DATABASES; DROP DATABASE IF EXISTS hive$v; CREATE DATABASE hive$v; SHOW DATABASES" """).!!
    Seq("/bin/bash", "-c", s"""mysql --host=192.168.59.103 -uroot --password=admin hive$v < /Users/marmbrus/workspace/hive/metastore/scripts/upgrade/mysql/hive-schema-0.$v.0.mysql.sql""").!!
  }

  var client: ClientInterface = null

  versions.foreach { version =>
    test(s"$version: listTables") {
      //client = IsolatedClientLoader.forVersion(version, buildConf(version)).client
      client = new IsolatedClientLoader(execJar(version), buildConf(version)).client
      client.listTables("default")
    }

    test(s"$version: createDatabase") {
      client.createDatabase("default")
    }

    test(s"$version: createTable") {
      client.createTable("src")
    }

    test(s"$version: getTable") {
      client.getTable("default", "src")
    }
  }
}