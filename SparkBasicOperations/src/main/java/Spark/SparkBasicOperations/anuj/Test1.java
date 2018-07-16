package Spark.SparkBasicOperations.anuj;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;


public class Test1 {



    public static void main(final String args[]){

        final SparkSession sparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("Java Spark SQL basic example")
            .getOrCreate();

        final Dataset<Row>  customerIdMaster = sparkSession.read().parquet("/am/my_dummy_path/anuj_customer_id_master.parquet");

        final Dataset<Row> cpMaster = sparkSession.read().parquet("/am/my_dummy_path/anuj_Club_Premiere_Master_Table.parquet").persist(StorageLevel.DISK_ONLY_2());

        try {
            customerIdMaster.createGlobalTempView("custidmaster");
            cpMaster.createGlobalTempView("cpmaster");
        } catch (final AnalysisException e) {
            e.printStackTrace();
        }

        System.out.println(customerIdMaster.count());

        final JavaRDD<Row> cleanedCustomerIdMaster = customerIdMaster.javaRDD().map((row) ->{

            final String ftNumber = row.getAs("respassengerft_frequenttravelernbr");

            boolean isPresent = false;
            if(ftNumber.contains("/")){

                final String[] arr = ftNumber.split("/");

                String ft = "";
                for(final String str : arr){
                    ft = ft.concat(str).concat(",");
                }
                
                ft = ft.substring(0, ft.length()-2);
                final Dataset<Row> fetchedRow = sparkSession.sql("Select * from cpmaster where members_frequenttravelernumber_c in (" + ft.trim() + ")" );

                if(null != fetchedRow){
                    isPresent = true;
                }

            }else{
                final Dataset<Row> fetchedRow = sparkSession.sql("Select * from custidmaster where members_frequenttravelernumber_c = " + ftNumber);

                if(null != fetchedRow){
                    isPresent = true;
                }
            }

            if(isPresent){
                return row;    
            }else{
                return null;
            }
        }).filter(row -> row != null);

        final Dataset<Row> outputDataset = sparkSession.createDataFrame(cleanedCustomerIdMaster, customerIdMaster.schema());
        System.out.println("-- creating parquet ---");
        outputDataset.printSchema();
        outputDataset.write().parquet("maprfs:///am/my_dummy_path/test1_sample.parquet");
    }
    
   
}
