package com.expedia.fcts.houston;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Scanner;

public class MySparkSQL {

    public static void main(String[] args) {

        // Set logging
        System.setProperty("hadoop.home.dir", "/Users/pshingavi/hadoop");
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        // Spark session for SQL
        SparkSession spark = SparkSession.builder()
                .appName("mySparkSQL")
                .master("local[*]")
                // Location for temp dir used by SparkSQL
                .config("spark.sql.warehouse.dir", "file:///Users/pshingavi/dev/mysparkleaning/tmp")
                .getOrCreate();

        // Load data
        Dataset<Row> dataSet = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");

        Column subjectColumn = dataSet.col("subject");
        Column scoreColumn = dataSet.col("score");

        Dataset<Row> resultDataSet = dataSet.filter(subjectColumn.equalTo("Math")
        .and(scoreColumn.$greater$eq(50)));
        resultDataSet.show();

        // Using Scanner to interrupt and watch the SparkUI
        /*Scanner scanner = new Scanner(System.in);
        scanner.next();*/

        // Close spark connection
        spark.close();
    }
}
