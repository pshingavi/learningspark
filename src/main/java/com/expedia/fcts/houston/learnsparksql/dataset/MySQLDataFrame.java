package com.expedia.fcts.houston.learnsparksql.dataset;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class MySQLDataFrame {

    public static void main(String[] args) {

        // Logger and hadoop dir
        System.setProperty("hadoop.home.dir", "/Users/pshingavi/hadoop");
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        // Spark sql - Create spark session
        SparkSession spark = SparkSession.builder()
                .appName("myDataFrameAPI")
                .master("local[*]")
                // Location for temp dir used by SparkSQL
                .config("spark.sql.warehouse.dir", "file:///Users/pshingavi/dev/mysparkleaning/tmp")
                .getOrCreate();

        Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/log/biglog.txt");
        /*
        * "select level, date_format(datetime, 'MMMM') as month,
        * cast(first(date_format(datetime, 'M')) as int) as monthnum,
        * count(1) as total from logging_table group by level, month order by monthnum"
        * */
        //dataset = dataset.selectExpr("level", "date_format(datetime, 'MMMM') as month");
        /*      +-----+---------+
                |level|    month|
                +-----+---------+
                |DEBUG| February|
                | WARN|     July|
        */
        dataset = dataset.select(
                col("level"),
                date_format(col("datetime"), "MMMM").as("month"),
                date_format(col("datetime"), "M").as("monthnum").cast(DataTypes.IntegerType)
        );

        Object[] months = new Object[] {"January", "February", "March", "April", "May", "June", "July", "August",
                "September", "October", "November", "December", "Dummy"};
        // Start grouping what your row column will be
        dataset = dataset
                .groupBy(col("level"))
                .pivot("month", Arrays.asList(months))
                .count()
                .na().fill(0);
        dataset.show();
    }
}
