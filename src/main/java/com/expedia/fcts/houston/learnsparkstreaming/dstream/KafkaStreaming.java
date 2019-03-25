package com.expedia.fcts.houston.learnsparkstreaming.dstream;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class KafkaStreaming {

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

        // sc does not have direct kafka connector - Look into spark streaming + kafka integration
        // https://spark.apache.org/docs/latest/streaming-kafka-integration.html
        Collection<String> topics = Arrays.asList("viewrecords");

        Map<String, Object> params = new HashMap<>();
        params.put("bootstrap.servers", "localhost:9092");
        params.put("key.deserializer", StringDeserializer.class);
        params.put("value.deserializer", StringDeserializer.class);
        params.put("group.id", "spark-group-1");
        params.put("auto.offset.reset", "latest");
        params.put("enable.auto.commit", false);

        JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(
                sc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topics, params)
        );

        // Create JavaPairDStrea<String, Value> (course, duration_it_was_watched_dummy)
        JavaPairDStream<Long, String> results = stream
                .mapToPair(kafkaRecord -> new Tuple2<>(kafkaRecord.value(), 5L))
                .reduceByKey((x, y) -> x + y)
                .mapToPair(newDStream -> newDStream.swap())
                //.sortByKey()  // Not provided by DStream. So get the underlying rdd and perform operation on that rdd
                .transformToPair(rdd -> rdd.sortByKey(false));

        results.print();

        sc.start();
        sc.awaitTermination();
    }

}
