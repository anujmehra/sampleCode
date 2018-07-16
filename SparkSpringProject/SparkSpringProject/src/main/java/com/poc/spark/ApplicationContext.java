package com.poc.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration("applicationContext")
@ComponentScan(basePackages = "com.poc.spark")
public class ApplicationContext {

    public static final int HADOOP_FILE_READ_BUFFER_SIZE = 262144 * 2;
    
    /**
     * Creating bean for SQLContext.
     * @param sparkSession
     * @return
     */
    @Bean("SQLContext")
    public SQLContext createSQLContext(@Autowired final SparkSession sparkSession) {

        return new SQLContext(sparkSession);
    }

    /**
     * Creating bean for SparkSession.
     * @param sparkContext
     * @return
     */
    @Bean("SparkSession")
    public SparkSession createSparkSession(@Autowired final JavaSparkContext sparkContext) {

        return new SparkSession(sparkContext.sc());
    }
    
    /**
     * Creating bean for JavaSparkContext.
     * @return
     */
    @Bean("sparkContext")
    public JavaSparkContext createJavaSparkContext() {

        try {
            SparkConf conf = new SparkConf();
            conf.set("spark.buffer.size", String.valueOf(HADOOP_FILE_READ_BUFFER_SIZE));
            conf.set("spark.local.dir", "/tmp");
            
            return new JavaSparkContext(conf);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
    
    @Bean("hadoopConfBean")
    public org.apache.hadoop.conf.Configuration createHadoopConfBean() {

        return newHadoopConf();
    }

    public static org.apache.hadoop.conf.Configuration newHadoopConf() {

        final org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
        hadoopConf.set("fs.maprfs.impl", "com.mapr.fs.MapRFileSystem");
        hadoopConf.setInt("io.file.buffer.size", HADOOP_FILE_READ_BUFFER_SIZE);
        return hadoopConf;
    }
}
