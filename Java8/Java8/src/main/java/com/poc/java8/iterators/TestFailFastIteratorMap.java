package com.poc.java8.iterators;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import com.poc.java8.lambda.Person;

public class TestFailFastIteratorMap {

    public static void main(String args[]){
    
        Map<String,Person> map = new HashMap<String,Person>();
        map.put("1",new Person(10, "C1"));
        map.put("2",new Person(20, "p21"));
        map.put("3",new Person(30, "xyz"));
        map.put("4",new Person(80, "A1"));
        map.put("5",new Person(90, "B5"));
        
        Iterator<Entry<String,Person>> itr = map.entrySet().iterator();
        
        while(itr.hasNext()){
            System.out.println(itr.next());
            map.put("6",new Person(70, "B5"));
        }
        
        
    }
    
}
