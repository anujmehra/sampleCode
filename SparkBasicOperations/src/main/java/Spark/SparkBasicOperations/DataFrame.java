package Spark.SparkBasicOperations;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import Spark.SparkBasicOperations.People;
import scala.Tuple2;


public class DataFrame {
	
	static int counter=0;

	public static void main(String[] args) throws InterruptedException{
		SparkConf sConf =  new SparkConf().setAppName("DF_1").setMaster("local[2]");
		JavaSparkContext jsc = new JavaSparkContext(sConf);
		SQLContext sql = new SQLContext(jsc);

		//Dataset<Row> dfRead = sql.read().json("D:/Spark-2.1_Hdp_2.7/examples/src/main/resources/people.json");


		//JavaRDD firstRDD = new JavaRDD<>(dfRead.rdd(),People.class);

		List<Integer> list = new ArrayList<>();
		list.addAll(Arrays.asList(1,2,3,4,5,6,7,8,9,10));
		JavaRDD<Integer> pRDD = jsc.parallelize(list);
/*		int sum=pRDD.reduce((a,b)-> a + b);
		System.out.println("Sum : "+sum);*/
		
		//Using variable in such a way here is wrong ... Instead use Accumulators
		pRDD.foreach(elem -> counter =counter + elem);
		
		System.out.println("counter : "+counter);

		/*List<String> peopleArray = new ArrayList<>();
		peopleArray.addAll(Arrays.asList("Raj,12","Arya,11"));
		JavaRDD<People> peopleRDD=jsc.parallelize(peopleArray).map(new Function<String,People>(){

			@Override
			public People call(String peopleDetails) throws Exception {
				String details[]=peopleDetails.split(",");
				People people = new People();
				people.firstName=details[0];
				people.marriedForYears=Integer.parseInt(details[1]);
				return people;
			}

		});*/
		//peopleRDD.saveAsTextFile("D:/Spark_Assignments/outPut/peopleRDD");
	/*	Dataset<Row> peopleDF = sql.createDataFrame(peopleRDD, People.class);
		peopleDF.show();
		*/
	/*	Thread thread = new Thread();
		thread.sleep(5*60*1000);*/
		
		JavaRDD<String> lines=jsc.textFile("D:/Spark_Assignments/inPut/data.txt");
		JavaPairRDD<String,Integer> pairs=lines.mapToPair(s->new Tuple2<String,Integer>(s,1));
		JavaPairRDD<String,Integer> counts= pairs.reduceByKey((a,b)->a+b);
		counts.sortByKey().collect().forEach((Tuple2<String, Integer> t) -> {
			System.out.println("Count for "+t._1+" is "+t._2);
		});
		
		JavaPairRDD<Row, Integer> rdd1=lines.flatMap(new FlatMapFunction<String,String>(){

			@Override
			public Iterator<String> call(String t) throws Exception {
				List<String> list= new ArrayList<>();
				String[] elem=null;
				if (t.contains(" ")){
					elem=t.split(" ");
					for(String e: elem){
						list.add(e);
					}
				}
				Iterator<String> itr=list.iterator();
				return itr;
			}
			
		}).mapToPair(new PairFunction<String, Row,Integer>(){

			@Override
			public Tuple2<Row, Integer> call(String t) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<>(RowFactory.create(t),1);
			}
			 
		});
		
		rdd1.collect().forEach((Tuple2<Row,Integer>t)->{
			System.out.println("Row : "+t._1.getAs(0));
		});
		
		
		
		
		String field="desc";
/*		List<StructField> structFields= new ArrayList<>();
		for(String str: field.split(" ")){
			structFields.add(DataTypes.createStructField(str, DataTypes.StringType, true));
		}
		StructType schema = DataTypes.createStructType(fields);*/
			
		StructType schema2 = new StructType().add(field,DataTypes.StringType);
		
/*		Dataset<Row> df1=sql.createDataFrame(rdd1, schema2);
		df1.show();*/
		
		
		
		

	}

}
