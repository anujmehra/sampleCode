package Spark.SparkBasicOperations.anuj;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;



public class CreateParquetFile {

    public static void main(final String args[]){

        final SparkSession sparkSession = SparkSession
                                    .builder()
                                    .master("local[*]")
                                    .appName("Java Spark SQL basic example")
                                    .getOrCreate();

        //1. Read a comma separated text file from HDFS
        final Dataset<String> inputTextDataset = sparkSession.read().textFile("E:/aeroMexico/Sample2.txt");
            
        System.out.println(inputTextDataset.first());

        //2. Convert this Dataset<String> into Dataset<Row>. 
        
        //For this first create a StructType to define the schema of the row.
        final StructField[] fields = new StructField[5];
        
        StructField structField = new StructField("Name",org.apache.spark.sql.types.DataTypes.StringType , true, Metadata.empty());
        fields[0] = structField;
        
        structField = new StructField("Age",org.apache.spark.sql.types.DataTypes.IntegerType , true, Metadata.empty());
        fields[1] = structField;
        
        structField = new StructField("Company",org.apache.spark.sql.types.DataTypes.StringType , true, Metadata.empty());
        fields[2] = structField;
        
        structField = new StructField("EmailId",org.apache.spark.sql.types.DataTypes.StringType , true, Metadata.empty());
        fields[3] = structField;
        
        structField = new StructField("City",org.apache.spark.sql.types.DataTypes.StringType , true, Metadata.empty());
        fields[4] = structField;
        
        final StructType schema = new StructType(fields);
        
        System.out.println(schema.toStream());
        
        //3. Convert this Dataset<String> into JavaRDD<String>
        final JavaRDD<String> inputTextJavaRDD = inputTextDataset.toJavaRDD();
        
        //4. Convert this JavaRDD<String> into into JavaRDD<Row>
        final JavaRDD<Row> inputRowJavaRDD =  inputTextJavaRDD.map( (eachRow) -> { 

                                                    Object[] finalValues = null;
                                                    
                                                    try {
                                                        //Split every row on the basis of separator
                                                        final String[] values = eachRow.split("," , -1);
                                                    
                                                        finalValues = new Object[values.length];
                                        
                                                       
                                                        Object obj = String.valueOf(values[0]);
                                                        finalValues[0] = obj;
                                                        
                                                        Object obj1 = Integer.parseInt(values[1]);
                                                        finalValues[1] = obj1;
                                                        
                                                        Object obj2 = String.valueOf(values[2]);
                                                        finalValues[2] = obj2;
                                                        
                                                        if(values.length > 3){
                                                            if(null != values[3] 
                                                                && values[3].length() > 0){
                                                                Object obj3 = String.valueOf(values[3]);
                                                                finalValues[3] = obj3;    
                                                            }
                                                        }
                                                        
                                                        if(values.length > 4){
                                                            if(null != values[4] 
                                                                && values[4].length() > 0){
                                                                Object obj4 = String.valueOf(values[4]);
                                                                finalValues[4] = obj4;    
                                                            }
                                                        }
                                                       
                                                        
                                                        
                                                    } catch (final Exception exception) {
                                                        exception.printStackTrace();
                                                }
                                                return finalValues == null ? null : (Row) new GenericRow(finalValues);
    
        });
        
        //5. Convert this JavaRDD<Row> into Dataset<Row>
        final Dataset<Row> rowDataset = sparkSession.createDataFrame(inputRowJavaRDD, schema);
        
        
        //6. Save this Dataset<Row> as parquet file
        rowDataset.write().parquet("E:/aeroMexico/Sample2_output.parquet");
    }
}
