
package com.am.analytics.job.dataaccess.config;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Scope;

import com.am.analytics.common.core.constant.BeanConstants;
import com.am.analytics.job.common.config.AnalyticsJobCommonConfig;

/**
* Configuration class for 'analytics-job-data-access' module.
* This class is responsible to load spring beans present in 'analytics-job-data-access' module.
* 
* This module has dependency on 'analytics-job-common' module.
* @author am
*/
@Configuration("analyticsJobDaoConfig")
@ComponentScan(basePackages = "com.am.analytics.job.dataaccess")
@Import(AnalyticsJobCommonConfig.class)
public class AnalyticsJobDaoConfig {

    /**
     * Gets the h base conf.
     *
     * @return the h base conf
     */
    @Bean(BeanConstants.HBASE_CONFIGURATION)
    @Scope("prototype")
    public static org.apache.hadoop.conf.Configuration getHBaseConf() {

        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.property.clientPort", "5181");
        conf.set("hbase.client.keyvalue.maxsize", "0");
        return conf;
    }
}
