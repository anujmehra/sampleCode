package Spark.SparkBasicOperations.anuj;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;


public class TestMultipleHiveTables {
    
    public static void main(final String args[]){
        
        final TestMultipleHiveTables testMultipleHiveTables = new TestMultipleHiveTables();

        System.out.println("-----------Start processing -----------------");
        
        final SparkSession sparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("Java Spark SQL basic example")
            .enableHiveSupport()
            .getOrCreate();
        
        testMultipleHiveTables.process(sparkSession);
       
    }
    
    /**
     * 
     */
    public void process(final SparkSession sparkSession){
        
        final SQLContext sqlContext = new SQLContext(sparkSession);
        
        final Dataset<Row> inputDataset = sqlContext.read().parquet("/am/intermediate_data/churnSampleTest.parquet");

        inputDataset.write().mode(SaveMode.Overwrite).saveAsTable("default.churnSampleTest");
        
       /* System.out.println("----------- Dataset Size -----------");
        System.out.println(cleanedDataset.count());

        double[] mylist = new double[10];
        mylist[0] = 1;
        mylist[1] = 1;
        mylist[2] = 1;
        mylist[3] = 1;
        mylist[4] = 1;
        mylist[5] = 1;
        mylist[6] = 1;
        mylist[7] = 1;
        mylist[8] = 1;
        mylist[9] = 1;
        
        Dataset<Row>[] datasetArr = cleanedDataset.randomSplit(mylist);
        
        for(int i=0;i<10;i++){
            int counter = i+1;
            System.out.println("count---->" + datasetArr[i].count());
            datasetArr[i].write().mode(SaveMode.Overwrite).parquet("/am/intermediate_data/Churn_Prep_" + counter + ".parquet");    
        }*/
        
    }
 
}