package org.apache.spark.sql.hive

import java.io.File
import javax.jdo.JDOHelper

import org.apache.spark.Logging
import org.scalatest.FunSuite

class VersionsSuite extends FunSuite with Logging {
  def buildConf(version: Int) = {
    val time = System.currentTimeMillis()

    import sys.process._
    Seq("/bin/bash", "-c", s"""mysql --host=192.168.59.103 -uroot --password=admin -e "CREATE DATABASE hive${version}_$time" """).!!
    Seq("/bin/bash", "-c", s"""mysql --host=192.168.59.103 -uroot --password=admin hive${version}_$time < /Users/marmbrus/workspace/hive/metastore/scripts/upgrade/mysql/hive-schema-0.$version.0.mysql.sql""").!!

    Map(
      "javax.jdo.option.ConnectionURL" -> s"jdbc:mysql://192.168.59.103:3306/hive${version}_$time",
      "javax.jdo.option.ConnectionDriverName" -> "com.mysql.jdbc.Driver",
      "javax.jdo.option.ConnectionUserName" -> "root",
      "javax.jdo.option.ConnectionPassword" -> "admin",
      "datanucleus.autoCreateSchema" -> "false")
  }

  test("success sanity check") {
    val badClient = IsolatedClientLoader.forVersion(13, buildConf(13)).client
    badClient.createDatabase("default")
    badClient.close()
  }

  def getNestedMessages(e: Throwable): String = {
    var causes = ""
    var lastException = e
    while (lastException != null) {
      causes += lastException.toString + "\n"
      lastException = lastException.getCause
    }
    causes
  }

  test("failure sanity check") {
    val e = intercept[Throwable] {
      val badClient = IsolatedClientLoader.forVersion(13, buildConf(12)).client
      badClient.createTable("src")
    }
    assert(getNestedMessages(e) contains "Unknown column 'A0.OWNER_NAME' in 'field list'")
  }

  val versions = Seq(10, 11, 12, 13)

  var client: ClientInterface = null

  versions.foreach { version =>
    test(s"$version: listTables") {
      client = null
      client = IsolatedClientLoader.forVersion(version, buildConf(version)).client
      //client = new IsolatedClientLoader(execJar(version), buildConf(version)).client
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