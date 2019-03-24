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
        inMemory.add(RowFactory.create("WARN", "2016-12-31 04:02:11"));
        inMemory.add(RowFactory.create("FATAL", "2016-12-31 04:02:11"));
        inMemory.add(RowFactory.create("WARN", "2016-11-12 04:02:11"));
        inMemory.add(RowFactory.create("INFO", "2016-11-14 04:02:11"));
        inMemory.add(RowFactory.create("FATAL", "2016-11-11 04:02:11"));

        StructField[] fields = new StructField[] {
                new StructField("level", DataTypes.StringType, false, Metadata.empty()),
                new StructField("datetime", DataTypes.StringType, false, Metadata.empty())
        };
        StructType schema = new StructType(fields);
        Dataset<Row> dataset = spark.createDataFrame(inMemory, schema);

        dataset.createOrReplaceTempView("logging_table");
        Dataset<Row> result = spark.sql("select level, date_format(datetime, 'MMMM') as month from logging_table");

        result.show();
        // Using Scanner to interrupt and watch the SparkUI
        /*Scanner scanner = new Scanner(System.in);
        scanner.next();*/

        // Close spark connection
        spark.close();
    }
}
