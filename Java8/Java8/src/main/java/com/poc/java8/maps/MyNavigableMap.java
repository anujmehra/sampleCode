package com.poc.java8.maps;

import java.util.Comparator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

public class MyNavigableMap {

    public static void main(final String args[]){
        final Comparator<Person> comp = new Comparator<Person>(){

            @Override
            public int compare(final Person o1, final Person o2) {
                return o1.getfName().compareTo(o2.getfName());
            }
        };
        
        final NavigableMap<Person,String> nm = new TreeMap<Person,String>(comp);
        nm.put(new Person("Ram"), "RAM");
        nm.put(new Person("John"), "JOHN");
        nm.put(new Person("Crish"), "CRISH");
        nm.put(new Person("Tom"), "TOM");
        
        final Set<Person> keys = nm.keySet();
        for(final Person key:keys){
            System.out.println(key+" ==> "+nm.get(key));
        }
        
        System.out.println("-------------------");
        
        Map.Entry<Person,String>  lowerMap = nm.lowerEntry(new Person("Ram"));
        System.out.println(lowerMap.getValue());
        System.out.println(lowerMap.getKey());
    }
}
