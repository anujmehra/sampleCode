package com.poc.java8.iterators;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import com.poc.java8.lambda.Person;

public class TestFailFastIteratorSet {

    public static void main(String args[]){
        Set<Person> set = new HashSet<Person>();
        set.add(new Person(10, "C1"));
        set.add(new Person(20, "p21"));
        set.add(new Person(30, "xyz"));
        set.add(new Person(80, "A1"));
        set.add(new Person(90, "B5"));
        
      /*  Iterator<Person> itr = set.iterator();
        
        while(itr.hasNext()){
            System.out.println(itr.next());
            set.add(new Person(100, "B5"));
        }*/
        
      /*  for(Person p : set)
        {
            System.out.println(p.getAge());
            set.add(new Person(100, "B5"));
        }*/
        
        Set<String> set2 = new HashSet<String>();
        set2.add("asd");
        set2.add("qwe");
        
       /* for(int i=0;i<set2.size();i++){
            System.out.println(set2.remove("qwe"));
        }*/
        
        for(String p : set2)
        {
            System.out.println(p);
            set2.add("ededcsac");
        }
    }
}
