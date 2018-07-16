package com.sample.sparkcode;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class SparkTransformations {

    private static String firstFileLocation  = "E:/TestData/Bounces1.csv";
    
    private static String secondFileLocation  = "E:/TestData/Bounces2.csv";
    
    
    public static void main(final String args[]){
        final SparkSession sparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("Java Spark SQL basic example")
            .getOrCreate();

        //1. Read a comma separated text file from HDFS
        final Dataset<String> initialFile = sparkSession.read().textFile(firstFileLocation).persist();
        System.out.println("Rows in initial file -->" + initialFile.count());
        
        final Dataset<String> incrementalFile = sparkSession.read().textFile(secondFileLocation).persist();
        System.out.println("Rows in incremental file -->" + incrementalFile.count());
        
        //Combining RDD's
        final JavaRDD<String> commonData = initialFile.javaRDD().intersection(incrementalFile.javaRDD());
        
        final JavaRDD<String> incrementalData = incrementalFile.javaRDD().subtract(commonData);
            
        System.out.println("Different rows in files -->" + incrementalData.count());
        
        System.out.println("-----New/Different Rows are as below-----");
        incrementalData.foreach((row) ->{
            System.out.println(row);
        });
        
        
    }
}
