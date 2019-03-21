package com.expedia.fcts.houston;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

public class Main {

    public static void main(String[] args) {

        List<Double> inputData = new ArrayList<Double>();
        inputData.add(34.5);
        inputData.add(3.9);
        inputData.add(3.5);
        inputData.add(4.5);

        // Set logging
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        // Create spark conf
        SparkConf sparkConf = new SparkConf()
                .setAppName("mySparkApp")
                .setMaster("local[*]"); // Run on local with all(*) available cores

        // Connection to spark cluster
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // Load data into spark. JavaRDD communicates Scala RDD
        JavaRDD<Double> myRdd =  sc.parallelize(inputData);
        System.out.println(myRdd.collect());
        sc.close();
    }
}
