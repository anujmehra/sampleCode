package Spark.SparkBasicOperations.anuj;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ReadAvroFile {

    public static void main(final String args[]){

        final SparkSession sparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("Java Spark SQL basic example")
            .getOrCreate();

        final Dataset<Row> inputData = sparkSession.read().format("com.databricks.spark.avro").load("E:/aeroMexico/test/Sample1.avro");
        
        inputData.printSchema();
        
        System.out.println(inputData.count());

    }
}
