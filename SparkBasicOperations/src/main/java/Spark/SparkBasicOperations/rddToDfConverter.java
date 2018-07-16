package Spark.SparkBasicOperations;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
// Import Row.
import org.apache.spark.sql.Row;
// Import RowFactory.
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
// Import factory methods provided by DataTypes.
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
// Import StructType and StructField
import org.apache.spark.sql.types.StructType;

public class rddToDfConverter {

	public static void main(String[] args) {
		
		SparkConf sparkConf = new SparkConf().setAppName("SparkApp").setMaster("local");

		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		// sc is an existing JavaSparkContext.
		SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);

		// Load a text file and convert each line to a JavaBean.
		JavaRDD<String> people = sc.textFile("D:/Spark-2.1_Hdp_2.7/examples/src/main/resources/people.txt");

		// The schema is encoded in a string
		String schemaString = "name age";

		// Generate the schema based on the string of schema
		List<StructField> fields = new ArrayList<StructField>();
		for (String fieldName: schemaString.split(" ")) {
		  fields.add(DataTypes.createStructField(fieldName, DataTypes.StringType, true));
		}
		StructType schema = DataTypes.createStructType(fields);

		// Convert records of the RDD (people) to Rows.
		JavaRDD<Row> rowRDD = people.map(
		  new Function<String, Row>() {
		    public Row call(String record) throws Exception {
		    	System.out.println("inside call method");
		      Object[] fields = record.split(",");
		      //Object rowFields = rddRecordtoRowConverter(fields);
		      //return RowFactory.create(fields[0], fields[1].trim());
		      return RowFactory.create(fields);
		      //System.out.println("returned from rddRecordtoRowConverter with output as "+rowFields);
		      //return myRowFactory.create(rowFields);
		    }

		  });

		// Apply the schema to the RDD.
		Dataset<Row> peopleDataFrame = sqlContext.createDataFrame(rowRDD, schema);
		
		peopleDataFrame.javaRDD().collect().forEach((Row row) -> {
			System.out.println("Name: " + row.getAs("name"));
			System.out.println("Age: " + row.getAs("age"));
		});

		// Register the DataFrame as a table.
		peopleDataFrame.registerTempTable("people");
		System.out.println("pdf Name "+peopleDataFrame.col("name"));
		
		// SQL can be run over RDDs that have been registered as tables.
		Dataset<Row> results = sqlContext.sql("SELECT name FROM people");
		results.show();

		// The results of SQL queries are DataFrames and support all the normal RDD operations.
		// The columns of a row in the result can be accessed by ordinal.
		List<String> names = results.javaRDD().map(new Function<Row, String>() {
		  public String call(Row row) {
		    return "Name: " + row.getString(0);
		  }
		}).collect();
		
		for (String name:names){
			System.out.println(name);
		}
		

	}
	




}

