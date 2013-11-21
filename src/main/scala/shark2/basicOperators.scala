package catalyst
package shark2

import org.apache.hadoop.hive.ql.plan.TableDesc
import org.apache.hadoop.hive.serde2.objectinspector.{PrimitiveObjectInspector, StructObjectInspector}
import shark.execution.HadoopTableReader
import shark.{SharkContext, SharkEnv}

import expressions._
import types._

import collection.JavaConversions._
import org.apache.spark.SparkContext._

case class Project(projectList: Seq[NamedExpression], child: SharkPlan) extends UnaryNode {
  def output = projectList.map(_.toAttribute)

  def execute() = child.execute().map { row =>
    projectList.map(Evaluate(_, Vector(row))).toIndexedSeq
  }
}

case class Filter(condition: Expression, child: SharkPlan) extends UnaryNode {
  def output = child.output
  def execute() = child.execute().filter { row =>
    Evaluate(condition, Vector(row)).asInstanceOf[Boolean]
  }
}

case class Sort(sortExprs: Seq[SortOrder], child: SharkPlan) extends UnaryNode {
  val numPartitions = 1 // TODO: Set with input cardinality

  val directions = sortExprs.map(_.direction).toIndexedSeq
  val dataTypes = sortExprs.map(_.dataType).toIndexedSeq

  class SortKey(val keyValues: IndexedSeq[Any]) extends Ordered[SortKey] with Serializable {
    def compare(other: SortKey): Int = {
      var i = 0
      while(i < keyValues.size) {
        val left = keyValues(i)
        val right = other.keyValues(i)
        val curDirection = directions(i)
        val curDataType = dataTypes(i)

        // TODO: Configure logging...
        // println(s"Comparing $left, $right as $curDataType order $curDirection")

        val comparison =
          if(curDataType == IntegerType)
            if(curDirection == Ascending)
              left.asInstanceOf[Int] compare right.asInstanceOf[Int]
            else
              right.asInstanceOf[Int] compare left.asInstanceOf[Int]
          else if(curDataType == DoubleType)
            if(curDirection == Ascending)
              left.asInstanceOf[Double] compare right.asInstanceOf[Double]
            else
              right.asInstanceOf[Double] compare left.asInstanceOf[Double]
          else
            sys.error(s"Comparison not yet implemented for: $curDataType")

        if(comparison != 0) return comparison
        i += 1
      }
      return 0
    }
  }

  def execute() = attachTree(this, "sort") {
    child.execute().map { row =>
      val input = Vector(row)
      val sortKey = new SortKey(sortExprs.map(s => Evaluate(s.child, input)).toIndexedSeq)

      (sortKey, row)
    }.sortByKey(true, numPartitions).map(_._2)
  }

  def output = child.output
}

case class HiveTableScan(attributes: Seq[Attribute], relation: MetastoreRelation) extends LeafNode {
  val hiveQlTable = new org.apache.hadoop.hive.ql.metadata.Table(relation.table)
  val tableDesc = new TableDesc(
    Class.forName(relation.table.getSd.getSerdeInfo.getSerializationLib).asInstanceOf[Class[org.apache.hadoop.hive.serde2.Deserializer]],
    Class.forName(relation.table.getSd.getInputFormat).asInstanceOf[Class[org.apache.hadoop.mapred.InputFormat[_,_]]],
    Class.forName(relation.table.getSd.getOutputFormat),
    hiveQlTable.getSchema
  )

  @transient
  val hadoopReader = new HadoopTableReader(tableDesc, SharkContext.hiveconf)

  /**
    * The hive object inspector for this table, which can be used to extract values from the
    * serialized row representation.
    */
  @transient
  lazy val objectInspector =
    tableDesc.getDeserializer.getObjectInspector.asInstanceOf[StructObjectInspector]

  /**
   * The hive struct field references that correspond to the attributes to be read from this table.
   */
  @transient
  lazy val refs = attributes.map { a =>
    objectInspector.getAllStructFieldRefs
      .find(_.getFieldName == a.name)
      .getOrElse(sys.error(s"Invalid attribute ${a.name} referenced in table $relation"))
  }

  def execute() = {
    hadoopReader.makeRDDForTable(hiveQlTable).map {
      case struct: org.apache.hadoop.hive.serde2.`lazy`.LazyStruct =>
        refs.map { ref =>
          val data = objectInspector.getStructFieldData(struct, ref)
          ref.getFieldObjectInspector.asInstanceOf[PrimitiveObjectInspector].getPrimitiveJavaObject(data)
        }.toIndexedSeq
    }
  }

  def output = attributes
}

case class InsertIntoHiveTable(tableName: String, child: SharkPlan)
                              (sc: SharkContext) extends UnaryNode {
  def output = child.output
  def execute() = {
    val childRdd = child.execute()
    // TODO: write directly to hive
    val tempDir = java.io.File.createTempFile("data", "tsv")
    tempDir.delete()
    tempDir.mkdir()
    childRdd.map(_.map(_.toString).mkString("\001")).saveAsTextFile(tempDir.getCanonicalPath)
    sc.runSql(s"LOAD DATA LOCAL INPATH '${tempDir.getCanonicalPath}/*' INTO TABLE $tableName")

    // It would be nice to just return the childRdd unchanged so insert operations could be chained,
    // however for now we return an empty list to simplify compatibility checks with hive, which
    // does not return anything for insert operations.
    // TODO: implement hive compatibility as rules.
    sc.makeRDD(Nil, 1)
  }
}

case class LocalRelation(output: Seq[Attribute], data: Seq[IndexedSeq[Any]])
                        (@transient sc: SharkContext) extends LeafNode {
  def execute() = sc.makeRDD(data, 1)
}

