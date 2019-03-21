package com.expedia.fcts.houston;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

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
                .setMaster("local[*]");

        // Connect to spark
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // Generate RDD from the input data
        JavaRDD<Integer> originalIntegers = sc.parallelize(inputData);

        // Let's say we want to have values together across different rdds
        // original list: (2,4) mapping for sqrt results (1.414, 2)
        // Let's say you want ((2, 1.414), (4,2)) each item is a custom object
        JavaRDD<MySqrtObject> mySqrRdd = originalIntegers.map(x -> new MySqrtObject(x));
        mySqrRdd.collect().forEach(System.out::println);

        // Above can be done by using TupleX. See Tuple2 is used from the scala package.
        // Spark has transitive dependency on scala so we already have the library
        JavaRDD<Tuple2<Integer, Double>> mySqrtTuple = originalIntegers
                .map(x -> new Tuple2<>(x, Math.sqrt(x)));

        mySqrtTuple.collect().forEach(System.out::println);
    }
}
