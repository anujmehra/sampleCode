package Spark.SparkBasicOperations.anuj;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ReadCSVFile {

    public static void main(String args[]){
        final SparkSession sparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("Java Spark SQL basic example")
            .getOrCreate(); 

        //1. Read CSV file

        //final Dataset<Row> inputDataset = sparkSession.read().csv("E:/aeroMexico/CurrencyConvertion.csv");
        final Dataset<Row> inputDataset = sparkSession.read().format("com.databricks.spark.csv").option("delimiter", "\\t") .load("E:/aeroMexico/CurrencyConvertion.csv");
        
        System.out.println(inputDataset.schema());

        inputDataset.show(100);
    }
}
