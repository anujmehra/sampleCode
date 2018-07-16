package com.poc.java8.threads._1;

public class Impl {

    public static void main(String args[]){
        
        ThreadClass obj = new ThreadClass();
        Thread th1 = new Thread(obj);
        th1.setName("Th-1");
        th1.start();
        
        //ThreadClass obj2 = new ThreadClass();
        Thread th2 = new Thread(obj);
        th2.setName("Th-2");
        th2.start();
    }
}
