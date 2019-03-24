package com.expedia.fcts.houston;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import java.util.Scanner;


public class SparkSQLWriteTest {

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

        // Read file from the location
        Dataset<Row> logRows = spark
                .read()
                .option("header", true)
                .csv("src/main/resources/log/biglog.txt");

        logRows.createOrReplaceTempView("logging_table");

        Dataset<Row> resultSet = spark.sql("select level, date_format(datetime, 'MMMM') as month, " +
                // Apply aggregate function on non group attribute. Use first() here
                // Case value as int else results will be inconsistent and sorted by string value
                "cast(first(date_format(datetime, 'M')) as int) as monthnum, " +
                //" order by month" - This will result in inconsistent since these are alphabets, so added monthnum
                "count(1) as total from logging_table group by level, month order by monthnum"
        );

        resultSet.show(100);

        // Using Scanner to interrupt and watch the SparkUI
        Scanner scanner = new Scanner(System.in);
        scanner.next();

        // Close spark connection
        spark.close();
    }
}
