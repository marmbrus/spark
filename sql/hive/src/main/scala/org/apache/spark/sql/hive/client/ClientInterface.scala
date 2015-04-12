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

package org.apache.spark.sql.hive.client

class Table
class Database
class Partition

/**
 * An externally visible interface to the Hive client.  This interface is shared across both the
 * internal and external classloaders for a given version of Hive and thus must expose only
 * shared classes.
 */
trait ClientInterface {
  def alterTable(tableFullName: String, table: Table)

  def getTable(databaseName: String, tableName: String): Table

  def createTable(databaseName: String, tableName: String, table: Table, allowExisting: Boolean)

  def getDatabase(databaseName: String): Database

  def getPartitions(databaseName: String, tableName: String): Seq[Partition]

  def getTables(databaseName: String): Seq[Table]

  def getDatabases(): Seq[Database]
}