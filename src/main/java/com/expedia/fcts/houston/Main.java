package com.expedia.fcts.houston;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class Main {

    public static void main(String[] args) {

        // Set logging
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        // Create spark conf
        SparkConf sparkConf = new SparkConf()
                .setAppName("mySparkApp")
                .setMaster("local[*]");

        // Connect to spark
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // Read from file
        // Does not load in memory immediately. Tells the worker nodes to load in parts
        JavaRDD<String> initialRDD = sc.textFile("src/main/resources/input.txt");
        initialRDD
                .flatMap(sentence -> Arrays.asList(sentence.split(" ")).iterator())
                .collect().forEach(System.out::println);

        sc.close();
    }
}
