package org.apache.spark.sql

import org.apache.spark.sql.catalyst.analysis.{Analyzer, EmptyFunctionRegistry, SimpleCatalog}
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.catalyst.types._

import scala.language.experimental.macros

import records._

import org.apache.spark.annotation.Experimental
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.{SqlParser, ScalaReflection}

object SQLMacros {
  import scala.reflect.macros._

  case class Schema(dataType: DataType, nullable: Boolean)

  def sqlImpl[A <: Product : c.WeakTypeTag](c: Context)(relation: c.Expr[RDD[A]]) = {
    import c.universe._

    // TODO: Don't copy this function.
    def schemaFor(tpe: `Type`): Schema = tpe match {
      case t if t <:< typeOf[Option[_]] =>
        val TypeRef(_, _, Seq(optType)) = t
        Schema(schemaFor(optType).dataType, nullable = true)
      case t if t <:< typeOf[Product] =>
        val formalTypeArgs = t.typeSymbol.asClass.typeParams
        val TypeRef(_, _, actualTypeArgs) = t
        val params = t.member(nme.CONSTRUCTOR).asMethod.paramss
        Schema(StructType(
          params.head.map { p =>
            val Schema(dataType, nullable) =
              schemaFor(p.typeSignature.substituteTypes(formalTypeArgs, actualTypeArgs))
            StructField(p.name.toString, dataType, nullable)
          }), nullable = true)
      // Need to decide if we actually need a special type here.
      case t if t <:< typeOf[Array[Byte]] => Schema(BinaryType, nullable = true)
      case t if t <:< typeOf[Array[_]] =>
        sys.error(s"Only Array[Byte] supported now, use Seq instead of $t")
      case t if t <:< typeOf[Seq[_]] =>
        val TypeRef(_, _, Seq(elementType)) = t
        Schema(ArrayType(schemaFor(elementType).dataType), nullable = true)
      case t if t <:< typeOf[Map[_,_]] =>
        val TypeRef(_, _, Seq(keyType, valueType)) = t
        Schema(MapType(schemaFor(keyType).dataType, schemaFor(valueType).dataType), nullable = true)
      case t if t <:< typeOf[String] => Schema(StringType, nullable = true)
      case t if t <:< typeOf[BigDecimal] => Schema(DecimalType, nullable = true)
      case t if t <:< typeOf[java.lang.Integer] => Schema(IntegerType, nullable = true)
      case t if t <:< typeOf[java.lang.Long] => Schema(LongType, nullable = true)
      case t if t <:< typeOf[java.lang.Double] => Schema(DoubleType, nullable = true)
      case t if t <:< typeOf[java.lang.Float] => Schema(FloatType, nullable = true)
      case t if t <:< typeOf[java.lang.Short] => Schema(ShortType, nullable = true)
      case t if t <:< typeOf[java.lang.Byte] => Schema(ByteType, nullable = true)
      case t if t <:< typeOf[java.lang.Boolean] => Schema(BooleanType, nullable = true)
      case t if t <:< definitions.IntTpe => Schema(IntegerType, nullable = false)
      case t if t <:< definitions.LongTpe => Schema(LongType, nullable = false)
      case t if t <:< definitions.DoubleTpe => Schema(DoubleType, nullable = false)
      case t if t <:< definitions.FloatTpe => Schema(FloatType, nullable = false)
      case t if t <:< definitions.ShortTpe => Schema(ShortType, nullable = false)
      case t if t <:< definitions.ByteTpe => Schema(ByteType, nullable = false)
      case t if t <:< definitions.BooleanTpe => Schema(BooleanType, nullable = false)
    }

    val q"""
      org.apache.spark.sql.test.TestSQLContext.SqlInterpolator(
        scala.StringContext.apply(..$rawParts))""" = c.prefix.tree

    val parts = rawParts.map(_.toString.stripPrefix("\"").stripSuffix("\""))
    val query = parts(0) + "table1" + parts(1)

    val parser = new SqlParser()
    val logicalPlan = parser(query)
    val catalog = new SimpleCatalog
    val analyzer = new Analyzer(catalog, EmptyFunctionRegistry, false)

    val inputType = weakTypeTag[A]
    val inputSchema = schemaFor(inputType.tpe).dataType.asInstanceOf[StructType].toAttributes
    catalog.registerTable(None, "table1", LocalRelation(inputSchema:_*))

    val analyzedPlan = analyzer(logicalPlan)

    val fields = analyzedPlan.output.zipWithIndex.map {
      case (attr, i) => q"${attr.name} -> row.getString($i)"
    }

    val tree = q"""
      import records.R
      $relation.registerAsTable("table1")
      val result = sql($query)
      // TODO: Avoid double copy
      result.map(row => R(..$fields))
    """

    // TODO: Why do I need this cast?
    c.Expr(tree).asInstanceOf[c.Expr[org.apache.spark.rdd.RDD[records.R{def name: String}]]]
  }
}

trait TypedSQL {
  self: SQLContext =>

  @Experimental
  implicit class SqlInterpolator(val strCtx: StringContext) {
    // TODO: Handle more than one relation
    // TODO: Handle functions...
    def sql[A <: Product](relation: RDD[A]) = macro SQLMacros.sqlImpl[A]
  }
}