package com.poc.java8;

public class MyMain {

    public static void main(final String args[]){
      /*  final MyDTO obj = new MyDTO();
        obj.setName("anuj");
        
        MyDTO obj2 = obj;
        
        obj.setName("mehra");
        
        System.out.println(obj2.getName());*/
        
        String s1 = "anuj";
        
        String s2= s1;
        
        s1 = "mehra";
        
        System.out.println(s2);
        
    }
    
}

