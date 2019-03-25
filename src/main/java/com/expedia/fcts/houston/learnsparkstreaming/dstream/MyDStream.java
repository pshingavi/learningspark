package com.expedia.fcts.houston.learnsparkstreaming.dstream;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

public class MyDStream {

    public static void main(String[] args) throws InterruptedException {

        // Logger and hadoop dir
        System.setProperty("hadoop.home.dir", "/Users/pshingavi/hadoop");
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.storage").setLevel(Level.WARN);

        // Create SparkConf
        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("myStreamingApp");

        // Set JavaStreamingContext
        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(10));

        JavaReceiverInputDStream<String> resultDStream =  sc.socketTextStream("localhost", 9999);
        JavaDStream<String> result = resultDStream.map(item -> item);
        result.map(rawMessage -> rawMessage.split(",")[0])
                .mapToPair(level -> new Tuple2<>(level, 1))
                .reduceByKey((x, y) -> x+y)
                .print();

        sc.start(); // Start streaming
        sc.awaitTermination();  // Wait termination of jvm

        // Do not close the context since we want to continue with streaming
    }
}
