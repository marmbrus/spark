package org.apache.spark.sql

import org.apache.spark.sql.catalyst.analysis.{Analyzer, EmptyFunctionRegistry, SimpleCatalog}
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.catalyst.types._

import scala.language.experimental.macros
import scala.language.existentials

import records._
import Macros.RecordMacros

import org.apache.spark.annotation.Experimental
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.{SqlParser, ScalaReflection}

object SQLMacros {
  import scala.reflect.macros._

  def sqlImpl(c: Context)(args: c.Expr[Any]*) =
    new Macros[c.type](c).sql(args)

  case class Schema(dataType: DataType, nullable: Boolean)

  class Macros[C <: Context](val c: C) extends ScalaReflection {
    val universe: c.universe.type = c.universe

    import c.universe._

    val rowTpe = tq"_root_.org.apache.spark.sql.catalyst.expressions.Row"

    val rMacros = new RecordMacros[c.type](c)

    case class RecSchema(name: String, index: Int,
      cType: DataType, tpe: Type)

    def sql(args: Seq[c.Expr[Any]]) = {

      val q"""
        org.apache.spark.sql.test.TestSQLContext.SqlInterpolator(
          scala.StringContext.apply(..$rawParts))""" = c.prefix.tree

      val parts = rawParts.map(_.toString.stripPrefix("\"").stripSuffix("\""))
      val query = parts(0) + args.indices.map { i => s"table$i" + parts(i + 1) }.mkString("")

      val analyzedPlan = analyzeQuery(query, args.map(_.actualType))

      val fields = analyzedPlan.output.map(attr => (attr.name, attr.dataType))
      val record = genRecord(q"row", fields)

      val tree = q"""
        ..${args.zipWithIndex.map{ case (r,i) => q"""$r.registerAsTable(${s"table$i"})""" }}
        val result = sql($query)
        result.map(row => $record)
      """

      println(tree)

      c.Expr(tree)
    }

    // TODO: Handle nullable fields
    def genRecord(row: Tree, fields: Seq[(String, DataType)]) = {
      case class ImplSchema(name: String, tpe: Type, impl: Tree)

      val implSchemas = for {
        ((name, dataType),i) <- fields.zipWithIndex
      } yield {
        val tpe = c.typeCheck(genGetField(q"null: $rowTpe", i, dataType)).tpe
        val tree = genGetField(row, i, dataType)

        ImplSchema(name, tpe, tree)
      }

      val schema = implSchemas.map(f => (f.name, f.tpe))

      val (spFlds, objFields) = implSchemas.partition(s =>
        rMacros.specializedTypes.contains(s.tpe))

      val spImplsByTpe = {
        val grouped = spFlds.groupBy(_.tpe)
        grouped.mapValues { _.map(s => s.name -> s.impl).toMap }
      }

      val dataObjImpl = {
        val impls = objFields.map(s => s.name -> s.impl).toMap
        val lookupTree = rMacros.genLookup(q"fieldName", impls, mayCache = false)
        q"($lookupTree).asInstanceOf[T]"
      }

      rMacros.specializedRecord(schema)(tq"Serializable")()(dataObjImpl) {
        case tpe if spImplsByTpe.contains(tpe) =>
          rMacros.genLookup(q"fieldName", spImplsByTpe(tpe), mayCache = false)
      }
    }

    /** Generate a tree that retrieves a given field for a given type.
      * Constructs a nested record if necessary
      */
    def genGetField(row: Tree, index: Int, t: DataType): Tree = t match {
      case t: PrimitiveType =>
        val methodName = newTermName("get" + primitiveForType(t))
        q"$row.$methodName($index)"
      case StructType(structFields) =>
        val fields = structFields.map(f => (f.name, f.dataType))
        genRecord(q"$row($index).asInstanceOf[$rowTpe]", fields)
      case _ =>
        c.abort(NoPosition, s"Query returns currently unhandled field type: $t")
    }

    def analyzeQuery(query: String, tableTypes: Seq[Type]) = {
      val parser = new SqlParser()
      val logicalPlan = parser(query)
      val catalog = new SimpleCatalog
      val analyzer = new Analyzer(catalog, EmptyFunctionRegistry, false)

      val tables = tableTypes.zipWithIndex.map { case (tblTpe, i) =>
        val TypeRef(_, _, Seq(schemaType)) = tblTpe

        val inputSchema = schemaFor(schemaType).dataType.asInstanceOf[StructType].toAttributes
        (s"table$i", LocalRelation(inputSchema:_*))
      }

      tables.foreach(t => catalog.registerTable(None, t._1, t._2))

      analyzer(logicalPlan)
    }

  }

  // TODO: Duplicated from codegen PR...
  protected def primitiveForType(dt: PrimitiveType) = dt match {
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
