package Spark.SparkBasicOperations.anuj;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;


/**
 * 
 * @author anujmehra
 *
 */
public class CreateMultipleParquetsForImpala {

    
    public static void main(final String args[]) throws IOException{

        final SparkSession sparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("Java Spark SQL basic example")
            .getOrCreate();

        //Read all parquet files preset at a location in HDFS
       
        final String inputPath = args[0];
        final String outputPath = args[1];
        
        /*
        String inputPath = "/am/staging_data/8001/sf_data_extension_cleaning/";
        String outputPath = "/am/qarelease/1801/impala/salesforce/cleaning/";
        
        */
        
        final List<String> fileNames = getFileNames(inputPath);
        
        
        for(final String fileName : fileNames){
            final Dataset<Row> input = sparkSession.read().parquet(inputPath + fileName);
            final JavaRDD<Row> inputRowJavaRDD = input.javaRDD();

            final StructType inputSchema = input.schema();
            final StructField[] schemaFields = inputSchema.fields();
            
            final StructField[] schemaFieldsWithStringDataType = new StructField[schemaFields.length];
            int counter = 0;
            for(final StructField field : schemaFields){
                schemaFieldsWithStringDataType[counter]= DataTypes.createStructField(field.name(), DataTypes.StringType, true);
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

            outputDataset.write().parquet(outputPath + fileName);
        }
        
    }

    public static final int HADOOP_FILE_READ_BUFFER_SIZE = 262144 * 2;
    
    public static transient Configuration conf = null;
    
    public static org.apache.hadoop.conf.Configuration newHadoopConf() {

        final org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
        hadoopConf.set("fs.maprfs.impl", "com.mapr.fs.MapRFileSystem");
        hadoopConf.setInt("io.file.buffer.size", HADOOP_FILE_READ_BUFFER_SIZE);
        return hadoopConf;
    }
    
    private static void init() {

        if (conf == null) {
            conf = newHadoopConf();
        }
    }
    
    public static List<String> getFileNames(final String directoryPath) throws IOException {
        init();
        List<String> fileNames = null;
        if (!StringUtils.isEmpty(directoryPath)) {
            final Path dstPath = new Path(directoryPath);
            fileNames = new ArrayList<>();
            final FileSystem fileSystem = dstPath.getFileSystem(conf);
            final RemoteIterator<LocatedFileStatus> filesIterator = fileSystem.listLocatedStatus(dstPath);
            if (filesIterator != null) {
                while (filesIterator.hasNext()) {
                    fileNames.add(filesIterator.next().getPath().getName());
                }
            }
        }
        return fileNames;
    }
}
