package Spark.SparkBasicOperations;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;

public class JavaRDDtoHBase {

	public static void main(String[] args) {
		SparkConf sConf = new SparkConf().setAppName("sample").setMaster("local");
		JavaSparkContext jsc = new JavaSparkContext(sConf);
		SQLContext sql = new SQLContext(jsc);
		Dataset<Row> data=sql.read().parquet("D:/Spark-2.1_Hdp_2.7/examples/src/main/resources/users.parquet");
		
		StructType schema=data.schema();
		
		StructField[] fields = schema.fields();
		for (StructField field: fields){
			System.out.println("fieldName: "+field.name()+"\nfieldDataType: "+field.dataType());
		}
		
		// Way 1 to created a pair RDD
		JavaPairRDD<String,Row> PairRDD =data.javaRDD().mapToPair(new PairFunction<Row, String, Row>(){

		
			@Override
			public Tuple2<String, Row> call(Row row) throws Exception {
				String message="from Way 1";
				String rowKey=(String) concatenate((row.getAs(1)),(row.getAs(2)),message);
				return new Tuple2<>(rowKey,row);
			}

						
		});
		
		// Way 2 to created a pair RDD
		JavaPairRDD<String,Row> anotherPairRDD = 
				data
				.javaRDD()
				.mapToPair(row->new Tuple2<>(((String)concatenate(row.getAs(1),row.getAs(1), "Way 2")),row));
		
		anotherPairRDD.collect().forEach( (Tuple2<String,Row> t) -> {
			System.out.println("rowKey: "+t._1);
			System.out.println("Column 1 : "+(t._2).getAs(1));
			System.out.println("Column 2 : "+(t._2).getAs(2));
			});

		JavaRDD<Row> javaRDD=anotherPairRDD.map((Tuple2<String,Row> row) -> {
			System.out.println("rowKey : "+ row._1);
			System.out.println("Column 1 : "+(row._2).getAs(1));
			System.out.println("Column 2 : "+(row._2).getAs(2));
			return row._2;
			} );
		
		Dataset<Row> reCreatedDF=sql.createDataFrame(javaRDD, schema);
		System.out.println("reCreatedDF.show()");
		reCreatedDF.show();
		
	
	}
	
	

	protected static Object concatenate(Object object, Object object2, String message) {
		System.out.println("call from "+message);
		String str = null;
		System.out.println("object 1 "+(String)object);
		//System.out.println("object 2 "+(String)object2);
		str=(String)object;
		//str=str.concat("_").concat((String)object2);
		System.out.println("concat string is "+str);
		return str;
	}


}
