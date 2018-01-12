package com.example.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class HelloSpark {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("HelloSpark");
        try (JavaSparkContext jsc = new JavaSparkContext(conf)) {
            // do something here
            List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
            // jsc是上述代码已经创建的JavaSparkContext实例
            JavaRDD<Integer> distData = jsc.parallelize(data);
            distData.reduce((a, b) -> a + b);
            System.out.println("--------------------------------------------------------------------");
            distData.map(a -> {
                System.out.println(a);
                return (a + 1);
            }).reduce((a, b) -> (a + b));
            distData.map(a -> a + 1).persist(StorageLevel.MEMORY_ONLY());

            List<Integer> list = distData.collect();
            for (Integer i : list) {
                System.out.println(i);
            }
            System.out.println("--------------------------------------------------------------------");
        }
    }

}
