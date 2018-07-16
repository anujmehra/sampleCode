package Spark.SparkBasicOperations.anuj;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class MergeTwoDatasets {

    public static void main(String args[]){

        final SparkSession session = SparkSession
            .builder()
            .master("local[*]")
            .appName("Java Spark SQL basic example")
            .getOrCreate();



        // Create a Java version of the Spark Context from the configuration
        final JavaSparkContext sc = new JavaSparkContext(session.sparkContext());

        // Load the input data, which is a text file read from the command line
        final JavaRDD<String> input = sc.textFile("E:/aeroMexico/Sample1.txt");

        System.out.println(input.count());

        //Converting to row type

        final JavaRDD<Row> rowRDD = input.map((row) ->{

            final int length = row.split(",").length;

            final Object[] values = new Object[length];

            for(int i=0;i<length;i++){
                values[i] = row.split(",")[i]; 
            }
            return new GenericRow(values);
        });

        StructField[] fields = new StructField[2];
        fields[0] = new StructField("Key",org.apache.spark.sql.types.DataTypes.StringType , true, Metadata.empty());
        fields[1] = new StructField("Value",org.apache.spark.sql.types.DataTypes.StringType , true, Metadata.empty());
        StructType schema = new StructType(fields);
        
        try{
            Dataset<Row> dataset1 = session.createDataFrame(rowRDD, schema);
            dataset1.createGlobalTempView("dataset1");

            Dataset<Row> dataset2 = session.createDataFrame(rowRDD, schema);
            dataset2.createGlobalTempView("dataset2");

            final Dataset<Row> selectedColumns = session.sql("SELECT a.value  FROM global_temp.dataset1 a");
            
            selectedColumns.printSchema();
            
            Dataset<Row> merged = session.sql("SELECT a.value,b.* FROM global_temp.dataset1 a, global_temp.dataset2 b where a.Key = b.key");

            System.out.println(merged.count());
            System.out.println(merged.first());
            merged.printSchema();

            //merged.write().csv("E:/aeroMexico/mergedData.csv");
            
            Dataset<Row> repartitionedDataset = merged.coalesce(5);
            repartitionedDataset.toJavaRDD().saveAsTextFile("E:/aeroMexico/mergedData200.csv");

            System.out.println("--- done ---");
            //merged.collect();
        }catch(Exception e){
            e.printStackTrace();   
        }

    }
}
