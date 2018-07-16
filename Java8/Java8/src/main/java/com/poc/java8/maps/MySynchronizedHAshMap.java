package com.poc.java8.maps;

import java.util.HashMap;
import java.util.Map;
import java.util.Collections;



public class MySynchronizedHAshMap {

    public static void main(final String args[]){
    
        //Synchronized HashMap
        final Map<String,String> syncMap = Collections.synchronizedMap(new HashMap<String,String>());
        syncMap.put("key1", "value1");
        syncMap.put("key1", "value2");
        syncMap.put("key3", "value3");
        syncMap.put("key3", "value4");
        
        System.out.println(syncMap.size());
    }
    
}
