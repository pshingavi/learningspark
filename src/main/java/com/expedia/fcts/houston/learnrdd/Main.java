package com.expedia.fcts.houston.learnrdd;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class Main {

    public static void main(String[] args) {

        // Set loggin level for apache
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        // Setup spark config
        SparkConf sparkConf = new SparkConf()
                .setAppName("mySparkApp")
                .setMaster("local[*]");
        // Remove setMaster when deploying to real cluster else will keep running everything in the driver node

        // Connect to spark
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        sc.textFile("src/main/resources/input.txt")
                .flatMap(sentence -> Arrays.asList(sentence
                            .replaceAll("[^a-zA-z\\s]", "")
                            .toLowerCase()
                            .trim()
                            .split(" ")
                    ).iterator()
                )
                .filter(word -> !StringUtils.isEmpty(word))
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((x, y) -> x+y)
                // Need below to switch key and value to use sortByKey
                .mapToPair(tuple -> new Tuple2<> (tuple._2, tuple._1))
                .sortByKey(false)
                // If you use foreach here, results might be incorrect. Using coalesce(#num of partition) is also not a great idea. It won't work
                .take(10)
                .forEach(tuple -> System.out.println(tuple._1 + ": " + tuple._2));

        sc.close();
    }
}
