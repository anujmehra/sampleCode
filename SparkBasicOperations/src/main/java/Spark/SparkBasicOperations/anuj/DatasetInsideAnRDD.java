package Spark.SparkBasicOperations.anuj;

import java.io.Serializable;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;

public class DatasetInsideAnRDD implements Serializable{

  
    private static final long serialVersionUID = -8614416557593438040L;

    public static void main(final String args[]) {

        final SparkSession sparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("Java Spark SQL basic example")
            .getOrCreate();

        final Dataset<Row> dataset1 = sparkSession.read().parquet("/am/qarelease/1801/impala/cp/cleaning/Members.parquet");
        dataset1.persist(StorageLevel.DISK_ONLY_2());
        dataset1.createOrReplaceTempView("dataset1");
        System.out.println("-- count 1 ---" + dataset1.count());
        
        final Dataset<Row> dataset2 = sparkSession.read().parquet("/am/qarelease/1801/impala/cp/cleaning/Members.parquet");
        dataset2.createOrReplaceTempView("dataset2");
        System.out.println("-- count 2 ---" + dataset2.count());

        final StructField[] fields = new StructField[1];
        final StructField structField = new StructField("members_passenger_first_name_c",org.apache.spark.sql.types.DataTypes.StringType , true, Metadata.empty());
        fields[0] = structField;
        final StructType schema = new StructType(fields);
        
        Dataset<Tuple2<Row,Row>> respDataset = dataset1.joinWith(dataset2, dataset1.col("members_email_c"));
        
       /* final JavaRDD<Row> responseRDD = dataset2.javaRDD().map((row) ->{

            final String members_email_c = row.getAs("members_email_c");

            final Dataset<Row> response = sparkSession.sql("select members_passenger_first_name_c from dataset1 where members_email_c ='" + members_email_c + "'");
            
            System.out.println("query--> "  + "select members_passenger_first_name_c from global_temp.dataset1 where members_email_c ='" + members_email_c + "'");
            
            final Object[] obj = new Object[1];
            obj[0] = response.head(1);
            
            return new GenericRowWithSchema(obj, schema);
        });*/

        //final Dataset<Row> respDataset = sparkSession.createDataFrame(responseRDD, schema);
        
        respDataset.write().parquet("/am/my_dummy_path/Members.parquet");
    }
}
