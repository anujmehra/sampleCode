package Spark.SparkBasicOperations.anuj;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class FindUniqueNames {

    public static void main(final String args[]){
        
        final FindUniqueNames obj = new FindUniqueNames();
        
        //final JavaSparkContext javaSparkContext = obj.createJavaSparkContext();
        
        final SparkSession sparkSession = obj.getSparkSession();
        
        
        //1. Read Parquet file
        final Dataset<Row> inputDataset = sparkSession.read().parquet("maprfs:///am/staging_data/1/sabre_cleaning/ResPassenger.parquet");
        try {
            inputDataset.createGlobalTempView("ResPassenger");
        } catch (AnalysisException e) {
            e.printStackTrace();
        }
        
        inputDataset.printSchema();
        
        System.out.println(inputDataset.count());
        
        final Dataset<Row> uniqueFirstNames = inputDataset.sqlContext().sql("SELECT distinct a.respassenger_namefirst  FROM global_temp.ResPassenger a");
        
        //uniqueFirstNames.write().parquet("maprfs:///am/staging_data/1/alankar/ResPassengerFirstNames");
        uniqueFirstNames.coalesce(1).write().format("com.databricks.spark.csv").csv("maprfs:///am/staging_data/1/alankar/ResPassengerDistinctFirstNames.csv");
    }
    
    private JavaSparkContext createJavaSparkContext() {
        try{
            //return new JavaSparkContext("local", "local");
            return new JavaSparkContext();
        }catch(final Exception e){
            e.printStackTrace();
        }
        return null;

    }
    
    private SparkSession getSparkSession(){
        JavaSparkContext javaSparkContext = this.createJavaSparkContext();
        return new SparkSession(javaSparkContext.sc());
    }
}
