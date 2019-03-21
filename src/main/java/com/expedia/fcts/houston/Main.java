package com.expedia.fcts.houston;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class Main {

    public static void main(String[] args) {

        List<String> inputData = new ArrayList<>();
        inputData.add("WARN: Tuesday 4 September 0405");
        inputData.add("ERROR: Tuesday 4 September 0408");
        inputData.add("FATAL: Wednesday 5 September 0415");
        inputData.add("ERROR: Friday 7 September 2405");
        inputData.add("WARN: Saturday 8 September 3405");

        // Set logging
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        // Create spark conf
        SparkConf sparkConf = new SparkConf()
                .setAppName("mySparkApp")
                .setMaster("local[*]");

        // Connect to spark
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // load input data into RDD
        sc.parallelize(inputData)
                .mapToPair(logLine -> new Tuple2<>(logLine.split(":")[0], 1))
                .reduceByKey((x, y) -> x+y)
                .foreach(tuple -> System.out.println(tuple._1 + " has count " + tuple._2));

        sc.close();
    }
}
