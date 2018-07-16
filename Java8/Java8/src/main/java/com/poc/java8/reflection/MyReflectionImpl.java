package com.poc.java8.reflection;

public class MyReflectionImpl {

    public static void main(String args[]){
    
        try {
            Class c = Class.forName("com.poc.java8.reflection.SampleClass");
            
            System.out.println(c.getName());
            System.out.println(c.getModifiers());
            System.out.println(c.getComponentType());
            System.out.println(c.getTypeName());
            
            System.out.println(c.getConstructors()[0]);
            System.out.println(c.getConstructors()[1]);
            
            SampleClass obj = c.getConstructors()[1].newInstance();
            
            
            obj.print();
            
            
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            e.printStackTrace();
        }  
    }
    
}
