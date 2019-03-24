package com.expedia.fcts.houston;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
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

        Dataset<Row> resultSet = dataSet.filter("subject = 'Math' AND score > 50");
        resultSet.show();

        // Using Scanner to interrupt and watch the SparkUI
        /*Scanner scanner = new Scanner(System.in);
        scanner.next();*/

        // Close spark connection
        spark.close();
    }
}
