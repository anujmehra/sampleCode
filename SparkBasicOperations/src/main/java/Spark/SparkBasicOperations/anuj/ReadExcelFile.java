package Spark.SparkBasicOperations.anuj;

import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import org.apache.commons.lang3.ArrayUtils;

import scala.collection.JavaConversions;
import scala.collection.mutable.WrappedArray;

public class ReadExcelFile {

    private static final String EXCEL_FORMATTER_PACKAGE = "org.zuinnote.spark.office.excel";

    private static final String LOCALE_PROPERTY = "read.locale.bcp47";

    private static final String LOCALE_VALUE = "us";

    private static final String FORMATTED_VALUE = "rows.formattedValue";
    
    public static final String EXCEL_SEPARATOR = "&&_--%%";
    
    public static void main(String args[]){

        ReadExcelFile obj = new ReadExcelFile();
        
        SparkSession  sparkSession = obj.getSparkSession(); 
        
        Dataset<String> dataSet = null;

        final Dataset<Row> formattedExcel = sparkSession.read().format(EXCEL_FORMATTER_PACKAGE).option(LOCALE_PROPERTY, LOCALE_VALUE).load("E:/aeroMexico/UnsubsMontly.xlsx");
        formattedExcel.printSchema();
        
        final JavaRDD<String> excelRDD = formattedExcel.select(formattedExcel.col(FORMATTED_VALUE)).javaRDD().map(row -> {
            final WrappedArray<String> wrappedArray = row.getAs(1);
            final int totalElements = wrappedArray.length();
            final Object[] elements = new Object[totalElements];
            final List<String> list = JavaConversions.seqAsJavaList(wrappedArray.toList());
            int i = 0;
            for (final String element : list) {
                elements[i++] = element;
            }
            return obj.concatenateUsingSeparator(EXCEL_SEPARATOR, elements);
        });
        
        dataSet = sparkSession.createDataset(excelRDD.rdd(), Encoders.STRING());
        
        
        dataSet.printSchema();
    }

    private String concatenateUsingSeparator(final String separator, final Object... objects) {

        String finalString = null;
        if (!ArrayUtils.isEmpty(objects)) {
            final StringBuilder builder = new StringBuilder();
            int i = 0;
            final int totalFiles = objects.length;
            for (final Object obj : objects) {
                if (null != obj) {
                    builder.append(obj.toString());
                }
                if (i != totalFiles - 1) {
                    builder.append(separator);
                }
                i++;
            }
            finalString = builder.toString();
        }
        return finalString;
    }
    
    private JavaSparkContext createJavaSparkContext() {
        try{
            return new JavaSparkContext("local", "local");
            //return new JavaSparkContext();
        }catch(final Exception e){
            e.printStackTrace();
        }
        return null;

    }

    private SparkSession getSparkSession(){
        JavaSparkContext javaSparkContext = this.createJavaSparkContext();
        return new SparkSession(javaSparkContext.sc());
    }
}
