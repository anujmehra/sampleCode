package Spark.SparkBasicOperations.anuj;

import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class SparkTransformations {

    public static void main(final String args[]){
        final SparkSession sparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("Java Spark SQL basic example")
            .getOrCreate();

        //1. Read a comma separated text file from HDFS
        final Dataset<String> inputTextDataset = sparkSession.read().textFile("E:/aeroMexico/Sample2.txt").persist();

        final Dataset<String> inputTextDataset2 = sparkSession.read().textFile("E:/aeroMexico/Sample2.txt").persist();

        /*final Dataset<Row> response =  inputTextDataset.crossJoin(inputTextDataset2);

        final Dataset<Row> response =  inputTextDataset.toJavaRDD().pa
        response.foreach((row) ->{
            System.out.println(row);
        });

        System.out.println(response.count());*/

        //Combining RDD's
        final JavaRDD<String> rdd1 = inputTextDataset.javaRDD();

        final JavaRDD<String> rdd2 = inputTextDataset2.javaRDD();

        final JavaRDD<String> rdd3 = rdd1.union(rdd2);

        final JavaRDD<String> rdd4 = rdd1.subtract(rdd2);

        final JavaRDD<String> rdd5 = rdd1.intersection(rdd2);
        
        System.out.println("1-->"+ rdd1.count());
        System.out.println("2-->"+ rdd2.count());
        System.out.println("3-->"+ rdd3.count());
        System.out.println("4-->"+ rdd4.count());
        System.out.println("5-->"+ rdd5.count());
        
        //Joining on RDD's
        //Only Paid RDD's can be joined on keys.
        
        JavaPairRDD<Integer,String> pairRDD  = rdd1.mapToPair((row) ->{
           
            return new Tuple2<Integer,String>(1, row);
        });
        
        pairRDD.partitionBy(new org.apache.spark.HashPartitioner(10));
        //pairRDD.partitionBy(new org.apache.spark.RangePartitioner<>(10));
        
    }
}
