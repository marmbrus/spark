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

package org.apache.spark.sql
package catalyst
package expressions

import scala.util.matching.Regex

import catalyst.types.StringType
import catalyst.types.BooleanType
import analysis.UnresolvedException
import catalyst.errors.`package`.TreeNodeException


abstract class BinaryString extends BinaryExpression {
  self: Product =>

  type EvaluatedType = Any

  def nullable = left.nullable || right.nullable

  override lazy val resolved =
    left.resolved && right.resolved && left.dataType == StringType && right.dataType == StringType

  def dataType = {
    if (!resolved) {
      throw new UnresolvedException(this,
        s"datatype. Can not resolve due to non string types ${left.dataType}, ${right.dataType}")
    }
    
    BooleanType
  }
  
  @inline
  protected final def s2(
      i: Row,
      e1: Expression,
      e2: Expression,
      f: ((String, String) => Boolean)): Any  = {

    if (e1.dataType != StringType) {
      throw new TreeNodeException(this,  s"Types do not match ${e1.dataType} != StringType")
    }

    if (e2.dataType != StringType) {
      throw new TreeNodeException(this,  s"Types do not match ${e2.dataType} != StringType")
    }

    val evalE1 = e1.apply(i)
    if(evalE1 == null) {
      null
    } else {
      val evalE2 = e2.apply(i)
      if (evalE2 == null) {
        null
      } else {
        f.apply(evalE1.asInstanceOf[String], evalE1.asInstanceOf[String])
      }
    }
  }
  
  @inline
  protected final def s1(
      i: Row,
      e1: Expression,
      f: ((String) => Boolean)): Any  = {

    if (e1.dataType != StringType) {
      throw new TreeNodeException(this,  s"Types do not match ${e1.dataType} != StringType")
    }

    val evalE1 = e1.apply(i)
    if(evalE1 == null) {
      null
    } else {
      f.apply(evalE1.asInstanceOf[String])
    }
  }
}

case class Like(left: Expression, right: Literal) extends BinaryString {
  def symbol = "LIKE"
  // replace the _ with .{1} exactly match 1 time of any character
  // replace the % with .*, match 0 or more times with any character
  def regex(v: String) = v.replaceAll("_", ".{1}").replaceAll("%", ".*")
  lazy val r = regex(right.value.asInstanceOf[String]).r
    
  override def apply(input: Row): Any = if(right.value == null) {
    null
  } else {
    s1(input, left, r.findFirstIn(_) != None)
  }
}

case class RLike(left: Expression, right: Literal) extends BinaryString {
  def symbol = "RLIKE"

  lazy val r = right.value.asInstanceOf[String].r

  override def apply(input: Row): Any = if(right.value == null) { 
    null
  } else {
    s1(input, left, r.findFirstIn(_) != None)
  }
}
