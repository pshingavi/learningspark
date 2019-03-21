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

        List<Integer> inputData = new ArrayList<>();
        inputData.add(35);
        inputData.add(12);
        inputData.add(3);
        inputData.add(4);

        // Set logging
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        // Create spark conf
        SparkConf sparkConf = new SparkConf()
                .setAppName("mySparkApp")
                .setMaster("local[*]"); // Run on local with all(*) available cores

        // Connection to spark cluster
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // Load data into spark. JavaRDD communicates Scala RDD
        JavaRDD<Integer> myRdd =  sc.parallelize(inputData);
        System.out.println(myRdd.collect());

        // Mapping function and return different type
        JavaRDD<Double> sqrtRdd = myRdd.map(Math::sqrt);
        //sqrtRdd.foreach(System.out::println);   // This can throw NotSerializableException
        // The function passed has to be serialized to multiple nodes. For single CPU this will work

        // Convert RDD to Java collection using collect() and
        // now the function is not to be serialized since it's running on local
        sqrtRdd.collect().forEach(System.out::println);

        // how many elements in result rdd
        // using just map and reduce
        JavaRDD<Integer> resultRDD = sqrtRdd.map(x -> 1);
        System.out.println("Total elements: " + resultRDD.reduce((x,y) -> x+y));

        // Close the connection to spark
        sc.close();
    }
}
