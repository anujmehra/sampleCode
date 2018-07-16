package Spark.SparkBasicOperations.anuj;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class CreateAvroFile {

    public static void main(final String args[]){
        final SparkSession sparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("Java Spark SQL basic example")
            .getOrCreate();


        final Dataset<String> inputTextDataset = sparkSession.read().textFile("E:/aeroMexico/Sample1.txt");

        final StructField[] fields = new StructField[3];

        StructField structField = new StructField("Name",org.apache.spark.sql.types.DataTypes.StringType , true, Metadata.empty());
        fields[0] = structField;

        structField = new StructField("Class",org.apache.spark.sql.types.DataTypes.StringType , true, Metadata.empty());
        fields[1] = structField;
        
        structField = new StructField("Age",org.apache.spark.sql.types.DataTypes.StringType , true, Metadata.empty());
        fields[2] = structField;

        final StructType schema = new StructType(fields);
        
        final JavaRDD<Row> dataWithSchema = inputTextDataset.javaRDD().map((row) ->{
           
            final Object[] values = new Object[3];
            
            final String[] input = row.split(",");
            
            int i=0;
            for(final String val : input){
                values[i] = val;
                i++;
            }
            
            return new GenericRowWithSchema(values, schema);
        });

        final Dataset<Row> finalDataset = sparkSession.createDataFrame(dataWithSchema, schema);
        
        finalDataset.write().format("com.databricks.spark.avro").mode(SaveMode.Overwrite).save("E:/aeroMexico/test/Sample1.avro");
    }
}
