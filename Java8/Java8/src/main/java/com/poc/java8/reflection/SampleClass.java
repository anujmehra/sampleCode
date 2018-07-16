package com.poc.java8.reflection;

public class SampleClass {

    private String firstName;
    
    private int num;
    
    public SampleClass(){
        
    }
    
    public SampleClass(String firstName , int num){
        this.firstName = firstName;
        this.num = num;
    }
    
    public void print(){
        System.out.println(this.firstName);
        System.out.println(this.num);
    }
    
    
}
