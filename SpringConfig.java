package com.hbase.poc.utils;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration("config")
@ComponentScan(basePackages="com.poc.hbase")
@Import(HBaseConfig.class)
public class SpringConfig {

}
