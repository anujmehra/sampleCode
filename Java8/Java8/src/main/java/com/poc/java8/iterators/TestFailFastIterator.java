package com.poc.java8.iterators;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.poc.java8.lambda.Person;

public class TestFailFastIterator {

    public static void main(String args[]){
        List<Person> list = new ArrayList<Person>();
        list.add(new Person(10, "C1"));
        list.add(new Person(20, "p21"));
        list.add(new Person(30, "xyz"));
        list.add(new Person(80, "A1"));
        list.add(new Person(70, "B5"));
        
        Iterator<Person> itr = list.iterator();
        
        while(itr.hasNext()){
         
            System.out.println(itr.next().getAge());
        }
        
        System.out.println("--- again--");
        
        Iterator<Person> itr2 = list.iterator();
        while(itr2.hasNext()){
            
            System.out.println(itr2.next().getAge());
            list.add(new Person(90, "C9"));
        }
    }
}
