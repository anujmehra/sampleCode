package com.poc.java8.ds;

import java.util.SortedMap;
import java.util.TreeMap;

import com.poc.java8.lambda.Person;

public class MyTreeMap {

    public static void main(String args[]){
        SortedMap<Person,Person> map = new TreeMap<Person,Person>((Person p1,Person p2) ->{
            return p1.getName().compareTo(p2.getName());
        });
        
      /*  map.put(key, value)
        map.get();*/
    }
}
