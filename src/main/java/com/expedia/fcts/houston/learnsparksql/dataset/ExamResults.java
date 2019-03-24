package com.expedia.fcts.houston.learnsparksql.dataset;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

public class ExamResults {

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

        Dataset<Row> dataset = spark.read()
                .option("header", true) // Returns DataFrameReader, inferSchema can be expensive since it needs extra pass on the data
                .csv("src/main/resources/exams/students.csv");

        // User defined functions to add column using 'lit' to existing DataFrame - Inline
        dataset = dataset.withColumn("pass_dummy", lit("YES"));   // lit can be dynamically built

        // Use condition to fill the value
        dataset = dataset.withColumn("pass", lit( col("grade").equals("A+") ));
        dataset.show();
        spark.close();
    }
}
