resolvers += "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository"

// Downloaded as giant jar for easy compiling during interview question.
libraryDependencies += "edu.berkeley.cs.amplab" %% "shark" % "0.9.0-SNAPSHOT" from "http://www.eecs.berkeley.edu/~marmbrus/tmp/shark-assembly-0.9.0-SNAPSHOT-hadoop1.0.4.jar"

// Hive 0.10.0 relies on a weird version of jdo that is not published anywhere... Remove when we upgrade to 0.11.0
libraryDependencies += "javax.jdo" % "jdo2-api" % "2.3-ec" from "http://www.datanucleus.org/downloads/maven2/javax/jdo/jdo2-api/2.3-ec/jdo2-api-2.3-ec.jar"

libraryDependencies ++= Seq(
 "org.apache.hadoop" % "hadoop-client" % "1.0.4",
 "org.scalatest" %% "scalatest" % "1.9.1" % "test",
 //"net.hydromatic" % "optiq-core" % "0.4.16-SNAPSHOT",
 "org.apache.hive" % "hive-metastore" % "0.10.0",
 "org.apache.hive" % "hive-exec" % "0.10.0",
 "org.apache.hive" % "hive-builtins" % "0.10.0",
  "com.typesafe" %% "scalalogging-slf4j" % "1.0.1")

// Multiple queries rely on the TestShark singleton.  See comments there for more details.
parallelExecution in Test := false

resolvers ++= Seq(
    // For Optiq
    "Conjars Repository" at "http://conjars.org/repo/",
    // For jdo-2 required by Hive < 0.12.0
    "Datanucleus Repository" at "http://www.datanucleus.org/downloads/maven2")


scalaVersion := "2.10.3"

initialCommands in console := """
import catalyst.analysis._
import catalyst.dsl._
import catalyst.errors._
import catalyst.expressions._
import catalyst.frontend._
import catalyst.plans.logical._
import catalyst.rules._
import catalyst.types._
import catalyst.util._
import catalyst.shark2.TestShark._"""