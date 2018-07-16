package Spark.SparkBasicOperations.anuj;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class ParquetToTextFile {

    public static void main(final String args[]){
        final SparkSession sparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("Java Spark SQL basic example")
            .config("spark.sql.warehouse.dir", "/user/hive/warehouse/")
            .enableHiveSupport()
            .getOrCreate();


        final SQLContext sqlContext = new SQLContext(sparkSession);


        final StructField[] fields = new StructField[1];
        final StructField structField = new StructField("row",org.apache.spark.sql.types.DataTypes.StringType , true, Metadata.empty());
        fields[0] = structField;
        final StructType newSchema  = new StructType(fields);



        for(int i=0;i<10;i++){
            final int counter = i+1;
            final Dataset<Row> input = sqlContext.read().parquet("/am/intermediate_data/Churn_Prep_" + counter + ".parquet");

            final StructType schema = input.schema();
            final int numberOfFields = schema.fieldNames().length;    

            final JavaRDD<Row> rdd = input.javaRDD().map((row) -> {

                int j=0;
                final Object[] values = new Object[1];
                final StringBuilder sb = new StringBuilder();
                for(final String fieldName : schema.fieldNames()){
                    j++;
                    if(j < numberOfFields){
                        if(null != row.getAs(fieldName)){
                            sb.append(row.getAs(fieldName).toString()).append(",");    
                        }else{
                            sb.append(",");
                        }
                    }else{
                        if(null != row.getAs(fieldName)){
                            sb.append(row.getAs(fieldName).toString());    
                        }
                    }

                }
                values[0] = sb.toString();
                return new GenericRowWithSchema(values, newSchema);
            });

            sparkSession.createDataFrame(rdd, newSchema).coalesce(1).write().mode(SaveMode.Overwrite).text("/am/intermediate_data/em_churn_input_" + counter + ".txt");
        }

    }
}
