package com.poc.java8.generics;

import java.util.ArrayList;
import java.util.List;

public class WildCardExample<T> {

    public List<?> getList(){
        return new ArrayList<String>();
    }
    
    
    public T  getData(){
    
        return null;
    }
    
    
    public String getKey(List<?> input){
        return "asd";
    }
    
    
   /* public String getString(T key){
        return "asd";
    }*/
    
   /* public String getStringg(<? extends T> key){
        return "asd";
    }*/
   
}
