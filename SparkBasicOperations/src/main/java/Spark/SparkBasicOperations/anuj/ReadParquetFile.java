package Spark.SparkBasicOperations.anuj;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ReadParquetFile {

    public static void main(String[] args) {

        final SparkSession sparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("Java Spark SQL basic example")
            .getOrCreate(); 

        //1. Read Parquet file
        final Dataset<Row> inputDataset = sparkSession.read().parquet("E:/Aeromexico_Phase2/MappingJobTestInput/sabre_mapping/TktCoupon.parquet");
        
        //final Dataset<Row> inputDataset = sparkSession.read().parquet("E:/Aeromexico_Phase2/MappingJobTestInput/sabre_mapping/ResFlightAirExtra.parquet");

        System.out.println(inputDataset.schema());
        
        inputDataset.show(100);

        //2. Manipulate data present in this parquet file
        final String[] datasetColumns = inputDataset.columns();
       
        for(final String column : datasetColumns){
            System.out.println(column);
        }

        //Operations on Dataset
        inputDataset.printSchema();
        
        inputDataset.select("name").show();
        
        inputDataset.groupBy("age").count().show();
        
        inputDataset.createOrReplaceTempView("sample1");
        
        Dataset<Row> sqlDF = sparkSession.sql("SELECT name,company,age FROM sample1 group by copmany");
        
        sqlDF.show();
        
    }
}
