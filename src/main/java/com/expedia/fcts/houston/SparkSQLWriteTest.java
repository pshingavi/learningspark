package com.expedia.fcts.houston;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;


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

        // Create in-memory list of Row
        List<Row> inMemory = new ArrayList<>();
        inMemory.add(RowFactory.create("WARN", "16 December 2018"));
        inMemory.add(RowFactory.create("FATAL", "16 December 2018"));
        inMemory.add(RowFactory.create("WARN", "16 December 2018"));
        inMemory.add(RowFactory.create("INFO", "16 December 2018"));
        inMemory.add(RowFactory.create("FATAL", "16 December 2018"));

        StructField[] fields = new StructField[] {
                new StructField("level", DataTypes.StringType, false, Metadata.empty()),
                new StructField("datetime", DataTypes.StringType, false, Metadata.empty())
        };
        StructType schema = new StructType(fields);
        Dataset<Row> dataset = spark.createDataFrame(inMemory, schema);

        dataset.createOrReplaceTempView("logging_table");
        Dataset<Row> result = spark.sql("select level, collect_list(datetime) from logging_table group by level");

        result.show();
        // Using Scanner to interrupt and watch the SparkUI
        /*Scanner scanner = new Scanner(System.in);
        scanner.next();*/

        // Close spark connection
        spark.close();
    }
}
