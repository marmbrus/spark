package org.apache.spark.sql

import org.apache.spark.sql.catalyst.analysis.{Analyzer, EmptyFunctionRegistry, SimpleCatalog}
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.catalyst.types._

import scala.language.experimental.macros

import records._
import Macros.RecordMacros

import org.apache.spark.annotation.Experimental
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.{SqlParser, ScalaReflection}

object SQLMacros {
  import scala.reflect.macros._

  case class Schema(dataType: DataType, nullable: Boolean)

  def sqlImpl(c: Context)(args: c.Expr[Any]*) = {
    import c.universe._

    // TODO: Don't copy this function from ScalaReflection.
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
    val query = parts(0) + (0 until args.size).map { i =>
      s"table$i" + parts(i + 1)
    }.mkString("")

    val parser = new SqlParser()
    val logicalPlan = parser(query)
    val catalog = new SimpleCatalog
    val analyzer = new Analyzer(catalog, EmptyFunctionRegistry, false)

    val tables = args.zipWithIndex.map { case (arg, i) =>
      val TypeRef(_, _, Seq(schemaType)) = arg.actualType

      val inputSchema = schemaFor(schemaType).dataType.asInstanceOf[StructType].toAttributes
      (s"table$i", LocalRelation(inputSchema:_*))
    }

    tables.foreach(t => catalog.registerTable(None, t._1, t._2))

    val analyzedPlan = analyzer(logicalPlan)

    // TODO: This shouldn't probably be here but somewhere generic
    // which defines the catalyst <-> Scala type mapping
    def toScalaType(dt: DataType) = dt match {
      case IntegerType => definitions.IntTpe
      case LongType => definitions.LongTpe
      case ShortType => definitions.ShortTpe
      case ByteType => definitions.ByteTpe
      case DoubleType => definitions.DoubleTpe
      case FloatType => definitions.FloatTpe
      case BooleanType => definitions.BooleanTpe
      case StringType => definitions.StringClass.toType
    }

    val schema = analyzedPlan.output.map(attr => (attr.name, toScalaType(attr.dataType)))
    val dataImpl = {
      // Generate a case for each field
      val cases = analyzedPlan.output.zipWithIndex.map {
        case (attr, i) =>
          cq"""${attr.name} => row.${newTermName("get" + primitiveForType(attr.dataType))}($i)"""
      }

      // Implement __data using these cases.
      // TODO: Unfortunately, this still boxes. We cannot resolve this
      // since the R abstraction depends on the fully generic __data.
      // The only way to change this is to create __dataLong, etc. on
      // R itself
      q"""
      val res = fieldName match {
        case ..$cases
        case _ => ???
      }
      res.asInstanceOf[T]
      """
    }

    val record: c.Expr[Nothing] = new RecordMacros[c.type](c).record(schema)(tq"Serializable")()(dataImpl)
    val tree = q"""
      ..${args.zipWithIndex.map{ case (r,i) => q"""$r.registerAsTable(${s"table$i"})""" }}
      val result = sql($query)
      result.map(row => $record)
    """

    println(tree)

    c.Expr(tree)
  }

  // TODO: Duplicated from codegen PR...
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
}

trait TypedSQL {
  self: SQLContext =>

  @Experimental
  implicit class SqlInterpolator(val strCtx: StringContext) {
    // TODO: Handle functions...
    def sql(args: Any*): Any = macro SQLMacros.sqlImpl
  }
}