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

package test.org.apache.spark.sql;

import java.io.Serializable;
import java.util.Arrays;


import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import org.junit.*;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.test.TestSQLContext;

public class JavaDatasetSuite implements Serializable {
    private transient JavaSparkContext jsc;
    private transient TestSQLContext context;

    @Before
    public void setUp() {
        // Trigger static initializer of TestData
        SparkContext sc = new SparkContext("local[*]", "testing");
        jsc = new JavaSparkContext(sc);
        context = new TestSQLContext(sc);
        context.loadTestData();
    }

    @After
    public void tearDown() {
        context.sparkContext().stop();
        context = null;
        jsc = null;
    }

    Dataset<Tuple2<Long, Long>> getDS() {
        JavaRDD<String> rdd = jsc.parallelize(Arrays.asList("{\"a\":1,\"b\":2}"));
        DataFrame df = context.read().json(rdd);
        return df.as(ProductEncoder.tuple(Long.class, Long.class));
    }

    static class MapFunction implements Function<Tuple2<Long, Long>, Tuple2<Long, Long>> {
        @Override
        public Tuple2<Long, Long> call(Tuple2<Long, Long> v1) throws Exception {
            return new Tuple2<Long, Long>(v1._1() + 1, v1._2() + 2);
        }
    }

    @Test
    public void testMap() {
        Dataset<Tuple2<Long, Long>> mapped =
                getDS().map(new MapFunction(), ProductEncoder.tuple(Long.class, Long.class));

        mapped.collect();
    }

    @Test
    public void testMapInfer() {
        Dataset<Tuple2<Long, Long>> mapped = getDS().mapInfer(new MapFunction());
        mapped.collect();
    }
}