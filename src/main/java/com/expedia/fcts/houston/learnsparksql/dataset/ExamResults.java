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

        // Register UDF
        spark
                .udf()
                // UDF can be lambda function or can be more complex implemented in another class
                .register("hasPassed", (String grade, String subject) -> {
                    if (subject.equals("Biology")) {
                        return grade.equals("A+");
                    }
                    return grade.equals("A+") || grade.equals("B");
                }, DataTypes.BooleanType);

        Dataset<Row> dataset = spark.read()
                .option("header", true) // Returns DataFrameReader, inferSchema can be expensive since it needs extra pass on the data
                .csv("src/main/resources/exams/students.csv");

        // User defined functions using UDF
        dataset = dataset.withColumn("pass", callUDF(
                "hasPassed",
                col("grade"),
                col("subject")
        ));

        dataset.show();
        spark.close();
    }
}
