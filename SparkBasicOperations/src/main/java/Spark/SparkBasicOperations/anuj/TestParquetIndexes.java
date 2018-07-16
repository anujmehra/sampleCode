package Spark.SparkBasicOperations.anuj;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.parser.SqlBaseParser.QueryContext;

public class TestParquetIndexes {

    public static void main(final String args[]){

        final SparkSession sparkSession = SparkSession
            .builder()
            .master("local[2]")
            .appName("Java Spark SQL basic example")
            .getOrCreate();

        final Dataset<Row> inputDataset = sparkSession.read().parquet("E:/aeroMexico/Sample1.parquet");

        // Create query context, entry point to working with parquet-index
        //final QueryContext context = new QueryContext(sparkSession);


    }

}
