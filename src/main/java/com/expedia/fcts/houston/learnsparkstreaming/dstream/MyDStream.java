package com.expedia.fcts.houston.learnsparkstreaming.dstream;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class MyDStream {

    public static void main(String[] args) throws InterruptedException {

        // Logger and hadoop dir
        System.setProperty("hadoop.home.dir", "/Users/pshingavi/hadoop");
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        // Create SparkConf
        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("myStreamingApp");

        // Set JavaStreamingContext
        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(30));

        JavaReceiverInputDStream<String> resultDStream =  sc.socketTextStream("localhost", 9999);
        JavaDStream<String> result = resultDStream.map(item -> item);
        result.print();

        sc.start(); // Start streaming
        sc.awaitTermination();  // Wait termination of jvm

        // Do not close the context since we want to continue with streaming
    }
}
