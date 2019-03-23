package com.expedia.fcts.houston;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class TestingJoins {

    public static void main(String[] args) {

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf sparkConf = new SparkConf()
                .setAppName("joinTestingApp")
                .setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        List<Tuple2<Integer, Integer>> visitsRaw = new ArrayList<>();
        visitsRaw.add(new Tuple2<>(4, 18));
        visitsRaw.add(new Tuple2<>(6, 4));
        visitsRaw.add(new Tuple2<>(10, 9));

        List<Tuple2<Integer, String>> usersRaw = new ArrayList<>();
        usersRaw.add(new Tuple2<>(1, "John"));
        usersRaw.add(new Tuple2<>(2, "Bob"));
        usersRaw.add(new Tuple2<>(3, "Alan"));
        usersRaw.add(new Tuple2<>(4, "Doris"));
        usersRaw.add(new Tuple2<>(5, "Marybelle"));
        usersRaw.add(new Tuple2<>(6, "Raquel"));

        JavaPairRDD<Integer, Integer> visitsRDD = sc.parallelizePairs(visitsRaw);
        JavaPairRDD<Integer, String > usersRDD = sc.parallelizePairs(usersRaw);

        JavaPairRDD<Integer, Tuple2<Integer, String>> resultRDD = visitsRDD.join(usersRDD);
        resultRDD.collect().forEach(System.out::println);

        sc.close();
    }
}
