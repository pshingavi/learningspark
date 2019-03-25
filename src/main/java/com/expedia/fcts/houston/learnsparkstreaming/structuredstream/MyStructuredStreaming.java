package com.expedia.fcts.houston.learnsparkstreaming.structuredstream;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

public class MyStructuredStreaming {

    public static void main(String[] args) throws StreamingQueryException {

        // Logger and hadoop dir
        System.setProperty("hadoop.home.dir", "/Users/pshingavi/hadoop");
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.storage").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .appName("structuredStreaming")
                .getOrCreate();

        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "viewrecords")
                .load();

        // start some dataframe operation
        df.createOrReplaceTempView("viewing_figures");

        // cast (value as string) required for deserializing value from the ConsumerRecord
        Dataset<Row> results = spark.sql("select cast (value as string) from viewing_figures");
        StreamingQuery streamingQuery = results
                .writeStream()
                .format("console")
                .outputMode(OutputMode.Append())
                .start();

        streamingQuery.awaitTermination();



    }

}
