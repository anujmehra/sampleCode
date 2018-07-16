package Spark.SparkBasicOperations;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;


public class RowSchema {

	public static void main(String[] args) {
		// creating Spark conf and context
		SparkConf conf = new SparkConf().setMaster("local").setAppName("Get_Row_Schema");
		JavaSparkContext jsc = new JavaSparkContext(conf); 
		
		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
		//Dataset<Row> inputDataset = spark.read().json("D:/Spark-2.1_Hdp_2.7/examples/src/main/resources/people.json");
		Dataset<Row> inputDataset = spark.read().parquet("D:/Spark-2.1_Hdp_2.7/examples/src/main/resources/users.parquet");
		StructType schema = inputDataset.schema();
		String FieldNames[] = schema.fieldNames();
		for(String field: FieldNames){
			System.out.println("fieldName : "+field);
		}
		StructField[] fields = schema.fields();
		List<String> fieldNames = new ArrayList<>();
		for (StructField field: fields ){
			System.out.println("field Name: "+field.name()+" \nand datatype: "+field.dataType());
			fieldNames.add(field.name());
		}
		for(String names: fieldNames){
			System.out.println("names : "+names);
		}
		
	}

}
