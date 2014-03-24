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

import types._
//import org.apache.spark.util.collection.BitSet

class CodeGenerator extends Logging {
  import scala.reflect.runtime.{universe => ru}
  import scala.reflect.runtime.universe._

  import scala.tools.reflect.ToolBox

  val toolBox = runtimeMirror(getClass.getClassLoader).mkToolBox()


  // TODO: Use typetags?
  val rowType = tq"org.apache.spark.sql.catalyst.expressions.Row"
  val mutableRowType = tq"org.apache.spark.sql.catalyst.expressions.MutableRow"
  val genericRowType = tq"org.apache.spark.sql.catalyst.expressions.GenericRow"
  val genericMutableRowType = tq"org.apache.spark.sql.catalyst.expressions.GenericMutableRow"

  val projectionType = tq"org.apache.spark.sql.catalyst.expressions.Projection"
  val mutableProjectionType = tq"org.apache.spark.sql.catalyst.expressions.MutableProjection"

  private val curId = new java.util.concurrent.atomic.AtomicInteger()
  private val javaSeperator = "$"

  /**
   * Returns a term name that is unique within this instance of a `CodeGenerator`.
   *
   * (Since we aren't in a macro context we do not seem to have access to the built in `freshName`
   * function.)
   */
  protected def freshName(prefix: String): TermName = {
    newTermName(s"$prefix$javaSeperator${curId.getAndIncrement}")
  }

  /**
   * Scala ASTs for evaluating an [[Expression]] given a [[Row]] of input.
   *
   * @param code The sequence of statements required to evaluate the expression.
   * @param nullTerm A term that holds a boolean value representing whether the expression evaluated
   *                 to null.
   * @param primitiveTerm A term for a possible primitive value of the result of the evaluation. Not
   *                      valid if `nullTerm` is set to `false`.
   * @param objectTerm An possibly boxed version of the result of evaluating this expression.
   */
  protected case class EvaluatedExpression(
      code: Seq[Tree],
      nullTerm: TermName,
      primitiveTerm: TermName,
      objectTerm: TermName) {

    def withObjectTerm = ???
  }

  /**
   * Given an expression tree returns the code required to determine both if the result is NULL
   * as well as the code required to compute the value.
   */
  protected def expressionEvaluator(e: Expression): EvaluatedExpression = {
    val primitiveTerm = freshName("primitiveTerm")
    val nullTerm = freshName("nullTerm")
    val objectTerm = freshName("objectTerm")

    implicit class Evaluate2(expressions: (Expression, Expression)) {

      /**
       * Short hand for generating binary evaluation code, which depends on two sub-evaluations of
       * the same type.  If either of the sub-expressions is null, the results of this computation is
       * assumed to be null
       *
       * @param f a function from two primitive term names to a tree that evaluates them.
       */
      def evaluate(f: (TermName, TermName) => Tree): Seq[Tree] =
        evaluateAs(expressions._1.dataType)(f)

      def evaluateAs(resultType: DataType)(f: (TermName, TermName) => Tree): Seq[Tree] = {
        require(expressions._1.dataType == expressions._2.dataType,
          s"${expressions._1.dataType} != ${expressions._2.dataType}")

        val eval1 = expressionEvaluator(expressions._1)
        val eval2 = expressionEvaluator(expressions._2)
        val resultCode = f(eval1.primitiveTerm, eval2.primitiveTerm)

        eval1.code ++ eval2.code ++
        q"""
          val $nullTerm = ${eval1.nullTerm} || ${eval2.nullTerm}
          val $primitiveTerm: ${termForType(resultType)} =
            if($nullTerm) {
              ${defaultPrimitive(resultType)}
            } else {
              $resultCode
            }
        """.children : Seq[Tree]
      }
    }

    val inputTuple = newTermName(s"i")

    // TODO: Skip generation of null handling code when expression are not nullable.
    val primitiveEvaluation: PartialFunction[Expression, Seq[Tree]] = {
      case b @ BoundReference(ordinal, _) =>
        q"""
          val $nullTerm: Boolean = $inputTuple.isNullAt($ordinal)
          val $primitiveTerm: ${termForType(b.dataType)} =
            if($nullTerm)
              ${defaultPrimitive(e.dataType)}
            else
              ${getColumn(inputTuple, b.dataType, ordinal)}
         """.children

      case expressions.Literal(value: String, dataType) =>
        q"""
          val $nullTerm = ${value == null}
          val $primitiveTerm: ${termForType(dataType)} = $value
         """.children
      case expressions.Literal(value: Int, dataType) =>
        q"""
          val $nullTerm = ${value == null}
          val $primitiveTerm: ${termForType(dataType)} = $value
         """.children
      case expressions.Literal(value: Long, dataType) =>
        q"""
          val $nullTerm = ${value == null}
          val $primitiveTerm: ${termForType(dataType)} = $value
         """.children

      case Cast(e, StringType) =>
        val eval = expressionEvaluator(e)
        eval.code ++
        q"""
          val $nullTerm = ${eval.nullTerm}
          val $primitiveTerm =
            if($nullTerm)
              ${defaultPrimitive(StringType)}
            else
              ${eval.primitiveTerm}.toString
        """.children

      case Equals(e1, e2) =>
        (e1, e2).evaluateAs (BooleanType) { case (eval1, eval2) => q"$eval1 == $eval2" }

      case GreaterThan(e1, e2) =>
        (e1, e2).evaluateAs (BooleanType) { case (eval1, eval2) => q"$eval1 > $eval2" }
      case GreaterThanOrEqual(e1, e2) =>
        (e1, e2).evaluateAs (BooleanType) { case (eval1, eval2) => q"$eval1 >= $eval2" }
      case LessThan(e1, e2) =>
        (e1, e2).evaluateAs (BooleanType) { case (eval1, eval2) => q"$eval1 < $eval2" }
      case LessThanOrEqual(e1, e2) =>
        (e1, e2).evaluateAs (BooleanType) { case (eval1, eval2) => q"$eval1 <= $eval2" }

      case And(e1, e2) =>
        val eval1 = expressionEvaluator(e1)
        val eval2 = expressionEvaluator(e2)

        eval1.code ++ eval2.code ++
        q"""
          var $nullTerm = false
          var $primitiveTerm: ${termForType(BooleanType)} = false

          if ((!${eval1.nullTerm} && !${eval1.primitiveTerm}) ||
              (!${eval2.nullTerm} && !${eval2.primitiveTerm})) {
            $nullTerm = false
            $primitiveTerm = false
          } else if (${eval1.nullTerm} || ${eval2.nullTerm} ) {
            $nullTerm = true
          } else {
            $nullTerm = false
            $primitiveTerm = true
          }
         """.children
      case Or(e1, e2) =>
        val eval1 = expressionEvaluator(e1)
        val eval2 = expressionEvaluator(e2)

        eval1.code ++ eval2.code ++
        q"""
          var $nullTerm = false
          var $primitiveTerm: ${termForType(BooleanType)} = false

          if ((!${eval1.nullTerm} && ${eval1.primitiveTerm}) ||
              (!${eval2.nullTerm} && ${eval2.primitiveTerm})) {
            $nullTerm = false
            $primitiveTerm = true
          } else if (${eval1.nullTerm} || ${eval2.nullTerm} ) {
            $nullTerm = true
          } else {
            $nullTerm = false
            $primitiveTerm = false
          }
         """.children

      case Add(e1, e2) =>      (e1, e2) evaluate { case (eval1, eval2) => q"$eval1 + $eval2" }
      case Subtract(e1, e2) => (e1, e2) evaluate { case (eval1, eval2) => q"$eval1 - $eval2" }
      case Multiply(e1, e2) => (e1, e2) evaluate { case (eval1, eval2) => q"$eval1 * $eval2" }
      case Divide(e1, e2) =>   (e1, e2) evaluate { case (eval1, eval2) => q"$eval1 / $eval2" }
      case Remainder(e1, e2) =>(e1, e2) evaluate { case (eval1, eval2) => q"$eval1 % $eval2" }
      
      case UnaryMinus(e) =>
        val eval = expressionEvaluator(e)
        q"""
          ..${eval.code}
          val $nullTerm = ${eval.nullTerm}
          val $primitiveTerm: ${termForType(e.dataType)} = -${eval.primitiveTerm}
         """.children

      case IsNotNull(e) =>
        val eval = expressionEvaluator(e)
        q"""
          ..${eval.code}
          var $nullTerm = false
          var $primitiveTerm: ${termForType(BooleanType)} = ${eval.nullTerm}.unary_!
        """.children

      case IsNull(e) =>
        val eval = expressionEvaluator(e)
        q"""
          ..${eval.code}
          var $nullTerm = false
          var $primitiveTerm: ${termForType(BooleanType)} = ${eval.nullTerm}
        """.children

      case c @ Coalesce(children) =>
        q"""
          var $nullTerm = true
          var $primitiveTerm: ${termForType(c.dataType)} = ${defaultPrimitive(c.dataType)}
        """.children ++
        children.map { c =>
          val eval = expressionEvaluator(c)
          q"""
            if($nullTerm) {
              ..${eval.code}
              if(!${eval.nullTerm}) {
                $nullTerm = false
                $primitiveTerm = ${eval.primitiveTerm}
              }
            }
           """
        }

      case Not(e) =>
        val eval = expressionEvaluator(e)
        q"""
          ..${eval.code}
          var $nullTerm = ${eval.nullTerm}
          var $primitiveTerm: ${termForType(BooleanType)} = ${eval.primitiveTerm}.unary_!
        """.children
        
      // TODO transform the In to If
//      case In(v, list) =>

      case i @ expressions.If(condition, trueValue, falseValue) =>
        val condEval = expressionEvaluator(condition)
        val trueEval = expressionEvaluator(trueValue)
        val falseEval = expressionEvaluator(falseValue)

        q"""
          var $nullTerm = false
          var $primitiveTerm: ${termForType(i.dataType)} = ${defaultPrimitive(i.dataType)}
          ..${condEval.code}
          if(!${condEval.nullTerm} && ${condEval.primitiveTerm}) {
            ..${trueEval.code}
            $nullTerm = ${trueEval.nullTerm}
            $primitiveTerm = ${trueEval.primitiveTerm}
          } else {
            ..${falseEval.code}
            $nullTerm = ${falseEval.nullTerm}
            $primitiveTerm = ${falseEval.primitiveTerm}
          }
        """.children
    }

    // If there was no match in the partial function above, we fall back on calling the interpreted
    // expression evaluator.
    val code: Seq[Tree] =
      primitiveEvaluation.lift.apply(e)
        //.map(_ :+ q"val $objectTerm = if($nullTerm) null else $primitiveTerm")
        .getOrElse {
          logger.warn(s"No rules to generate $e")
          val tree = reify { e }
          q"""
            // Test
            val $objectTerm = $tree.apply(i)
            val $nullTerm = $objectTerm == null
            val $primitiveTerm = $objectTerm.asInstanceOf[${termForType(e.dataType)}]
          """.children
        }

     EvaluatedExpression(code, nullTerm, primitiveTerm, objectTerm)
  }

  protected def getColumn(inputRow: TermName, dataType: DataType, ordinal: Int) = {
    dataType match {
      case dt @ NativeType() => q"$inputRow.${accessorForType(dt)}($ordinal)"
      case _ => q"$inputRow.apply($ordinal).asInstanceOf[${termForType(dataType)}]"
    }
  }

  protected def setColumn(
      destinationRow: TermName,
      dataType: DataType,
      ordinal: Int,
      value: TermName) = {
    dataType match {
      case dt @ NativeType() => q"$destinationRow.${mutatorForType(dt)}($ordinal, $value)"
      case _ => q"$destinationRow.update($ordinal, $value)"
    }
  }

  protected def accessorForType(dt: DataType) = newTermName(s"get${primitiveForType(dt)}")
  protected def mutatorForType(dt: DataType) = newTermName(s"set${primitiveForType(dt)}")

  protected def primitiveForType(dt: DataType) = dt match {
    case IntegerType => "Int"
    case LongType => "Long"
    case ShortType => "Short"
    case ByteType => "Byte"
    case DoubleType => "Double"
    case FloatType => "Float"
    case BooleanType => "Boolean"
    case StringType => "String"
  }

  protected def defaultPrimitive(dt: DataType) = dt match {
    case BooleanType => ru.Literal(Constant(false))
    case FloatType => ru.Literal(Constant(-1.0.toFloat))
    case StringType => ru.Literal(Constant("<uninit>"))
    case _ => ru.Literal(Constant(-1))
  }

  protected def termForType(dt: DataType) = dt match {
    case n: NativeType => n.tag
    case _ => typeTag[Any]
  }
}

object GenerateOrdering extends CodeGenerator {
  import scala.reflect.runtime.{universe => ru}
  import scala.reflect.runtime.universe._

  def apply(ordering: Seq[SortOrder]): Ordering[Row] = {
    val a = newTermName("a")
    val b = newTermName("b")
    val comparisons = ordering.zipWithIndex.map { case (order, i) =>
      val evalA = expressionEvaluator(order.child)
      val evalB = expressionEvaluator(order.child)

      q"""
        i = $a
        ..${evalA.code}
        i = $b
        ..${evalB.code}
        if (${evalA.nullTerm} && ${evalB.nullTerm}) {
          // Nothing
        } else if (${evalA.nullTerm}) {
          return ${if (order.direction == Ascending) q"-1" else q"1"}
        } else if (${evalB.nullTerm}) {
          return ${if (order.direction == Ascending) q"1" else q"-1"}
        } else {
          i = a
          val comp = ${evalA.primitiveTerm} - ${evalB.primitiveTerm}
          if(comp != 0) return comp.toInt
        }
      """
    }

    val q"class $orderingName extends $orderingType { ..$body }" = reify {
      class SpecificOrdering extends Ordering[Row] {
        val o = ordering
      }
    }.tree.children.head

    val code = q"""
      class $orderingName extends $orderingType {
        ..$body
        def compare(a: $rowType, b: $rowType): Int = {
          var i: $rowType = null // Holds current row being evaluated.
          ..$comparisons
          return 0
        }
      }
      new $orderingName()
      """
    toolBox.eval(code).asInstanceOf[Ordering[Row]]
  }
}

// Canonicalize the expressions those those that differ only by names can reuse the same code.
object ExpressionCanonicalizer extends rules.RuleExecutor[Expression] {
  val batches =
    Batch("CleanExpressions", FixedPoint(20), CleanExpressions) :: Nil

  object CleanExpressions extends rules.Rule[Expression] {
    def apply(e: Expression): Expression = e transform {
      case BoundReference(o, a) =>
        BoundReference(o, AttributeReference("a", a.dataType, a.nullable)(exprId = ExprId(0)))
      case Alias(c, _) => c
    }
  }
}

object GenerateCondition extends CodeGenerator {
  import scala.reflect.runtime.{universe => ru}
  import scala.reflect.runtime.universe._

  // TODO: Should be weak references... bounded in size.
  val conditionCache = new collection.mutable.HashMap[Expression, (Row) => Boolean]

  // TODO: Safe to fire up multiple instances of the compiler?
  def apply(condition: Expression): (Row => Boolean) = synchronized {
    val cleanedExpression = ExpressionCanonicalizer(condition)
    conditionCache.getOrElseUpdate(cleanedExpression, createCondition(cleanedExpression))
  }

  def createCondition(condition: Expression): ((Row) => Boolean) = {
    val cEval = expressionEvaluator(condition)

    val code =
      q"""
        (i: $rowType) => {
          ..${cEval.code}
          if(${cEval.nullTerm}) false else ${cEval.primitiveTerm}
        }
      """

    logger.info(s"Generated condition '$condition':\n$code")
    toolBox.eval(code).asInstanceOf[Row => Boolean]
  }
}

abstract class Projection extends (Row => Row)

abstract class MutableProjection extends Projection {
  def currentValue: Row

  /* Updates the target of this projection to a new MutableRow */
  def target(row: MutableRow): MutableProjection
}

object GenerateMutableProjection extends CodeGenerator {
  import scala.reflect.runtime.{universe => ru}
  import scala.reflect.runtime.universe._

  // TODO: Should be weak references... bounded in size.
  val projectionCache = new collection.mutable.HashMap[Seq[Expression], () => MutableProjection]

  def apply(expressions: Seq[Expression], inputSchema: Seq[Attribute]): (() => MutableProjection) =
    apply(expressions.map(BindReferences.bindReference(_, inputSchema)))

  // TODO: Safe to fire up multiple instances of the compiler?
  def apply(expressions: Seq[Expression]): (() => MutableProjection) = synchronized {
    val cleanedExpressions = expressions.map(ExpressionCanonicalizer(_))
    projectionCache.getOrElseUpdate(cleanedExpressions, createProjection(cleanedExpressions))
  }

  val mutableRowName = newTermName("mutableRow")

  def createProjection(expressions: Seq[Expression]): (() => MutableProjection) = {
    val projectionCode = expressions.zipWithIndex.flatMap { case (e, i) =>
      val evaluationCode = expressionEvaluator(e)

      evaluationCode.code :+
      q"""
        if(${evaluationCode.nullTerm})
          mutableRow.setNullAt($i)
        else
          ${setColumn(mutableRowName, e.dataType, i, evaluationCode.primitiveTerm)}
      """
    }

    val code =
    q"""
      () => { new $mutableProjectionType {

        private[this] var $mutableRowName: $mutableRowType =
          new $genericMutableRowType(${expressions.size})

        def target(row: $mutableRowType): $mutableProjectionType = {
          $mutableRowName = row
          this
        }

        /* Provide immutable access to the last projected row. */
        def currentValue: $rowType = mutableRow

        def apply(i: $rowType): $rowType = {
          ..$projectionCode
          mutableRow
        }
      } }
    """

    logger.info(s"code for ${expressions.mkString(",")}:\n$code")
    toolBox.eval(code).asInstanceOf[() => MutableProjection]
  }
}

object GenerateProjection extends CodeGenerator {
  import scala.reflect.runtime.{universe => ru}
  import scala.reflect.runtime.universe._

  // TODO: Should be weak references... bounded in size.
  val projectionCache = new collection.mutable.HashMap[Seq[Expression], Projection]

  def apply(expressions: Seq[Expression], inputSchema: Seq[Attribute]): Projection =
    apply(expressions.map(BindReferences.bindReference(_, inputSchema)))

  // TODO: Safe to fire up multiple instances of the compiler?
  def apply(expressions: Seq[Expression]): Projection = synchronized {
    val cleanedExpressions = expressions.map(ExpressionCanonicalizer(_))
    projectionCache.getOrElseUpdate(cleanedExpressions, createProjection(cleanedExpressions))
  }

  // Make Mutablility optional...
  def createProjection(expressions: Seq[Expression]): Projection = {
    val tupleLength = ru.Literal(Constant(expressions.length))
    val lengthDef = q"final val length = $tupleLength"

    /* TODO: Configurable...
    val nullFunctions =
      q"""
        private final val nullSet = new org.apache.spark.util.collection.BitSet(length)
        final def setNullAt(i: Int) = nullSet.set(i)
        final def isNullAt(i: Int) = nullSet.get(i)
      """
     */

    val nullFunctions =
      q"""
        private[this] var nullBits = new Array[Boolean](${expressions.size})
        final def setNullAt(i: Int) = { nullBits(i) = true }
        final def isNullAt(i: Int) = nullBits(i)
      """.children

    val tupleElements = expressions.zipWithIndex.flatMap {
      case (e, i) =>
        val elementName = newTermName(s"c$i")
        val evaluatedExpression = expressionEvaluator(e)
        val iLit = ru.Literal(Constant(i))

        q"""
        var ${newTermName(s"c$i")}: ${termForType(e.dataType)} = _
        {
          ..${evaluatedExpression.code}
          if(${evaluatedExpression.nullTerm})
            setNullAt($iLit)
          else
            $elementName = ${evaluatedExpression.primitiveTerm}
        }
        """.children : Seq[Tree]
    }

    val iteratorFunction = {
      val allColumns = (0 until expressions.size).map { i =>
        val iLit = ru.Literal(Constant(i))
        q"if(isNullAt($iLit)) { null } else { ${newTermName(s"c$i")} }"
      }
      q"final def iterator = Iterator[Any](..$allColumns)"
    }

    val accessorFailure = q"""scala.sys.error("Invalid ordinal:" + i)"""
    val applyFunction = {
      val cases = (0 until expressions.size).map { i =>
        val ordinal = ru.Literal(Constant(i))
        val elementName = newTermName(s"c$i")
        val iLit = ru.Literal(Constant(i))

        q"if(i == $ordinal) { if(isNullAt($i)) return null else return $elementName }"
      }
      q"final def apply(i: Int): Any = { ..$cases; $accessorFailure }"
    }

    val updateFunction = {
      val cases = expressions.zipWithIndex.map {case (e, i) =>
        val ordinal = ru.Literal(Constant(i))
        val elementName = newTermName(s"c$i")
        val iLit = ru.Literal(Constant(i))

        q"""
          if(i == $ordinal) {
            if(value == null) {
              setNullAt(i)
            } else {
              $elementName = value.asInstanceOf[${termForType(e.dataType)}]
              return
            }
          }"""
      }
      q"final def update(i: Int, value: Any): Unit = { ..$cases; $accessorFailure }"
    }

    val specificAccessorFunctions = NativeType.all.map { dataType =>
      val ifStatements = expressions.zipWithIndex.flatMap {
        case (e, i) if e.dataType == dataType =>
          val elementName = newTermName(s"c$i")
          // TODO: The string of ifs gets pretty inefficient as the row grows in size.
          // TODO: Optional null checks?
          q"if(i == $i) return $elementName" :: Nil
        case _ => Nil
      }

      q"""
      final def ${accessorForType(dataType)}(i: Int):${termForType(dataType)} = {
        ..$ifStatements;
        $accessorFailure
      }"""
    }

    val specificMutatorFunctions = NativeType.all.map { dataType =>
      val ifStatements = expressions.zipWithIndex.flatMap {
        case (e, i) if e.dataType == dataType =>
          val elementName = newTermName(s"c$i")
          // TODO: The string of ifs gets pretty inefficient as the row grows in size.
          // TODO: Optional null checks?
          q"if(i == $i) { $elementName = value; return }" :: Nil
        case _ => Nil
      }

      q"""
      final def ${mutatorForType(dataType)}(i: Int, value: ${termForType(dataType)}): Unit = {
        ..$ifStatements;
        $accessorFailure
      }"""
    }

    val hashValues = expressions.zipWithIndex.map { case (e,i) =>
      val elementName = newTermName(s"c$i")
      val nonNull = e.dataType match {
        case BooleanType => q"if ($elementName) 0 else 1"
        case ByteType | ShortType | IntegerType => q"$elementName.toInt"
        case LongType => q"($elementName ^ ($elementName >>> 32)).toInt"
        case FloatType => q"java.lang.Float.floatToIntBits($elementName)"
        case DoubleType =>
          q"{ val b = java.lang.Double.doubleToLongBits($elementName); (b ^ (b >>>32)).toInt }"
        case _ => q"$elementName.hashCode"
      }
      q"if (isNullAt($i)) 0 else $nonNull"
    }

    val hashUpdates: Seq[Tree] = hashValues.map(v => q"result = 37 * result + $v": Tree)

    val hashCodeFunction =
      q"""
        override def hashCode(): Int = {
          var result: Int = 37
          ..$hashUpdates
          result
        }
      """

    val columnChecks = (0 until expressions.size).map { i =>
      val elementName = newTermName(s"c$i")
      q"if (this.$elementName != specificType.$elementName) return false"
    }

    val equalsFunction =
      q"""
        override def equals(other: Any): Boolean = other match {
          case specificType: SpecificRow =>
            ..$columnChecks
            return true
          case other => super.equals(other)
        }
      """

    val classBody =
      nullFunctions ++ (
      lengthDef +:
      iteratorFunction +:
      applyFunction +:
      updateFunction +:
      equalsFunction +:
      hashCodeFunction +:
      (tupleElements ++ specificAccessorFunctions ++ specificMutatorFunctions))

    val code = q"""
      final class SpecificRow(i: $rowType) extends $mutableRowType {
        ..$classBody

        // Not safe!
        final def copy() = scala.sys.error("Not implemented")

        final def getStringBuilder(ordinal: Int): StringBuilder = ???
      }

      new $projectionType { def apply(r: $rowType) = new SpecificRow(r) }
    """

    logger.warn(s"MutableRow, initExprs: ${expressions.mkString(",")} code:\n${toolBox.typeCheck(code)}")
    toolBox.eval(code).asInstanceOf[Projection]
  }
}