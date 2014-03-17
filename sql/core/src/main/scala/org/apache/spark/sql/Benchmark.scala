package org.apache.spark
package sql
package execution

import catalyst.dsl.expressions._
import catalyst.expressions._
import catalyst.util._

object Benchmark {

  def main(args: Array[String]) {
    val sc = new SparkContext("local[4]", "agg benchmark", new SparkConf())
    val sqlContext = new SqlContext(sc)
    import sqlContext._

    val rdd = sc.parallelize((1 to 100), 100).mapPartitions { iter =>
      var i = 0
      val row = GenerateProjection(Literal(0) :: Nil)(EmptyRow).asInstanceOf[MutableRow]
      new Iterator[Row] {
        def hasNext = i < 1000000
        def next() = {
          i += 1
          row.setInt(0, i % 100)
          row
        }
      }
    }


    val query = SparkLogicalPlan(ExistingRdd('a.int :: Nil, rdd)).groupBy()(Count(1))

    println(benchmark { query.toRdd.collect().foreach(println)})
    println(benchmark { query.toRdd.collect().foreach(println)})
    println(benchmark { query.toRdd.collect().foreach(println)})

    val query2 = SparkLogicalPlan(ExistingRdd('a.int :: Nil, rdd)).groupBy('a)(Count('a))

    println(benchmark { query.toRdd.collect().foreach(println)})
    println(benchmark { query.toRdd.collect().foreach(println)})
    println(benchmark { query.toRdd.collect().foreach(println)})
  }
}