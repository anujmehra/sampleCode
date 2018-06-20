package com.am.analytics.job.common.config;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.am.analytics.common.core.constant.BeanConstants;
import com.am.analytics.common.core.utils.Logger;
import com.am.analytics.commons.all.config.AnalyticsCommonAllConfig;
/**
 * Configuration class for 'analytics-job-common' module. This class is responsible to load spring beans present in
 * 'analytics-job-common' module. This module has dependency on 'analytics-commons-all' and 'analytics-job-model' module.
 *
 * @author am
 */
@Configuration("analyticsJobCommonConfig")
@ComponentScan("com.am.analytics.job.common")
@Import(AnalyticsCommonAllConfig.class)
public class AnalyticsJobCommonConfig {

    /** The Constant HADOOP_FILE_READ_BUFFER_SIZE. */
    public static final int HADOOP_FILE_READ_BUFFER_SIZE = 262144 * 2;
    
    /** The Constant LOGGER. */
    private static final Logger LOGGER = Logger.getLogger(AnalyticsJobCommonConfig.class);

    /**
     * Creating Spring bean for {@link JavaSparkContext}.
     *
     * @return javaSparkContext {@link JavaSparkContext}
     */
    @Bean(BeanConstants.JAVA_SPARK_CONTEXT_BEAN)
    public JavaSparkContext createJavaSparkContext() {

        try {
            SparkConf conf = new SparkConf();
            conf.set("spark.buffer.size", String.valueOf(HADOOP_FILE_READ_BUFFER_SIZE));
            //conf.set("spark.hadoop.yarn.application.classpath", "/opt/mapr/hbase/hbase-1.1.8/lib/*,/opt/mapr/lib/*");
            return new JavaSparkContext(conf);
        } catch (Exception e) {
            LOGGER.error(e, "Exception occur while creating java spark context.."+e.getLocalizedMessage());
        }
        return null;

    }


    /**
     * Creating Spring bean for {@link SparkSession}.
     *
     * @param sparkContext the spark context
     * @return sparkSession {@link SparkSession}
     */
    @Bean
    public SparkSession createSparkSession(@Autowired
    final JavaSparkContext sparkContext) {

        return SparkSession
            .builder()
            .sparkContext(sparkContext.sc())
            //.enableHiveSupport()
            .getOrCreate();
    }
    
    /**
     * Creating Spring bean for {@link SQLContext}.
     *
     * @param sparkSession the spark session
     * @return sqlContext {@link SQLContext}
     */
    @Bean
    public SQLContext createSQLContext(@Autowired
    final SparkSession sparkSession) {

        return new SQLContext(sparkSession);
    }

}
