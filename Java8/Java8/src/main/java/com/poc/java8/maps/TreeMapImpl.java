package com.poc.java8.maps;

import java.util.Collection;
import java.util.Comparator;
import java.util.Set;
import java.util.TreeMap;

public class TreeMapImpl {

    public static void main(final String args[]){
        final Comparator<Person> comparator = new Comparator<Person>(){

            @Override
            public int compare(final Person o1, final Person o2) {

                return o1.getfName().compareTo(o2.getfName());
            }
        };
        
        //TreeMap<Person,String> tm = new TreeMap<Person, String>(comparator);
        final TreeMap<Person,String> tm = new TreeMap<Person, String>(Comparator.reverseOrder());
        tm.put(new Person("Ram"), "RAM");
        tm.put(new Person("John"), "JOHN");
        tm.put(new Person("Crish"), "CRISH");
        tm.put(new Person("Tom"), "TOM");
       
        final Set<Person> keys = tm.keySet();
        for(final Person key:keys){
            System.out.println(key+" ==> "+tm.get(key));
        }
        
        final Person p = new Person("Ram");
        
        final String str = tm.get(p);
        
        System.out.println("value -->" + str);
        
        final Collection<String> values  = tm.values();
        values.forEach((row) -> System.out.println(row));
    }
}
