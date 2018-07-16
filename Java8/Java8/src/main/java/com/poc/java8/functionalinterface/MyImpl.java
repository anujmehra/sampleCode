package com.poc.java8.functionalinterface;

import com.poc.java8.functionalinterface.MyFunctionalInterface;

public class MyImpl {

    public void doFunction(){
        MyFunctionalInterface obj = (int a, int b) -> {System.out.println(a +b);};
        obj.myMethod(10, 20);
    }
    
    public static void main(String args[]){
        MyImpl obj = new MyImpl();
        obj.doFunction();
    }
}
