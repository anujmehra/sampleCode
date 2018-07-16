package com.poc.java8.optional;

import java.util.Optional;

public class OptionalImpl {

    public String returnValue(){
        return null;
    }
    
    public static void main(String args[]){
        OptionalImpl obj = new OptionalImpl();
        
        Optional<String> str = Optional.ofNullable(obj.returnValue());
        if(str.isPresent()){
            System.out.println(str.get());
        }
        System.out.println(str.orElse("Mehra"));
        
    }
}
