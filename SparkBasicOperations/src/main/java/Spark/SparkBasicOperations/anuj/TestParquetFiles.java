package Spark.SparkBasicOperations.anuj;

import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class TestParquetFiles {

    public static void main(final String args[]){


        final SparkSession sparkSession = SparkSession
            .builder()
            .master("local[2]")
            .appName("Java Spark SQL basic example")
            .getOrCreate();

       final Dataset<String> inputDataset = sparkSession.read().textFile("E:/aeroMexico/Sample1.txt");

        final StructField[] fields = new StructField[3];
        fields[0] = DataTypes.createStructField("Field1", DataTypes.StringType, true);
        fields[1] = DataTypes.createStructField("Field2", DataTypes.StringType, true);
        fields[2] = DataTypes.createStructField("Field3", DataTypes.StringType, true);

        final StructType schema = new StructType(fields);

        final JavaRDD<Row> output = inputDataset.javaRDD().map((row) -> {
            final String[] values = row.split(",");

            return new GenericRowWithSchema(values, schema);
        });

        final Dataset<Row> outputDataset = sparkSession.createDataFrame(output, schema);

        outputDataset.write().mode(SaveMode.Append).parquet("E:/aeroMexico/Sample1.parquet");

        
      /*  final Dataset<Row> inputDataset2 = sparkSession.read().parquet("E:/aeroMexico/Sample1.parquet");

        inputDataset2.printSchema();
        
        System.out.println(inputDataset2.count());
        
        List<Row> list = inputDataset2.collectAsList();
        
        for(Row r : list){
            System.out.println(r);
        }*/
        
    }

}
