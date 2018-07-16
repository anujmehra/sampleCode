package Spark.SparkBasicOperations.manish;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.oracle.jrockit.jfr.DataType;

public class CreateParquetForImpalaResMaster {

    public static void main(final String args[]){

        final SparkSession sparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("Java Spark SQL basic example")
            .getOrCreate();

        //1. Read a comma separated text file from HDFS

        final String inputPath = "/home/admin/am/master/reservationmaster/Reservation_Master.parquet";
        final Dataset<Row> input = sparkSession.read().parquet(inputPath);
        final JavaRDD<Row> inputRowJavaRDD = input.javaRDD();

        final String outputPath = "/home/admin/am/english_masters/Reservation_Master_english.parquet";


        final StructType inputSchema = input.schema();
        final StructField[] schemaFields = inputSchema.fields();
        
        final StructField[] schemaFieldsWithStringDataType = new StructField[schemaFields.length];
        int counter = 0;
        for(final StructField field : schemaFields){
            if(field.dataType().equals(DataTypes.LongType) 
                || field.dataType().equals(DataTypes.IntegerType) ){
                schemaFieldsWithStringDataType[counter]=DataTypes.createStructField(field.name(), field.dataType(), true);
            }else{
                schemaFieldsWithStringDataType[counter]=DataTypes.createStructField(field.name(), DataTypes.StringType, true);    
            }
            
            counter++;
        }
        final StructType newSchema = DataTypes.createStructType(schemaFieldsWithStringDataType);

        System.out.println(inputSchema);
        System.out.println("*********************************");
        System.out.println(newSchema);
        final JavaRDD<Row> finalRDD = inputRowJavaRDD.map((row) ->{

            final Object[] newValues = new Object[newSchema.fieldNames().length];
            int i=0;
            for(final String fieldName : newSchema.fieldNames()){
                
                newValues[i] = row.getAs(fieldName) == null ? null : row.getAs(fieldName).toString();
                
                i++;
            }
            return new GenericRowWithSchema(newValues, newSchema);
        });

        final Dataset<Row> outputDataset = sparkSession.createDataFrame(finalRDD, newSchema);

        outputDataset.write().parquet(outputPath);
    }


}
