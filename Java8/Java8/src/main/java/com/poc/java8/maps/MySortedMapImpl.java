package com.poc.java8.maps;

import java.util.Comparator;
import java.util.SortedMap;
import java.util.TreeMap;

public class MySortedMapImpl {

    public static void main(final String args[]){
        
        final Comparator<Person> comp = new Comparator<Person>(){
            @Override
            public int compare(final Person o1, final Person o2) {
               return o1.getfName().compareTo(o2.getfName());
            }
        };
        
        final SortedMap<Person,String> sm = new TreeMap<>(comp);
        
        
    }
    
}
