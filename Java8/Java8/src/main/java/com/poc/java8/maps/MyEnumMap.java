package com.poc.java8.maps;

import java.util.AbstractMap;
import java.util.EnumMap;

public class MyEnumMap {

    // create an enum  
    public enum Days {  
        Monday("Monday"), 
        Tuesday("Tuesday"), 
        Wednesday("Wednesday"), 
        Thursday("Thursday");

        private final String day;

        private Days(final String day){
            this.day = day;
        }

        public String getDay() {

            return day;
        }
    }  

    public static void main(String args[]){

        AbstractMap<Days,String> enumMap = new EnumMap<>(Days.class);
        
        enumMap.put(Days.Monday, "1");  
        enumMap.put(Days.Tuesday, "2");  
        enumMap.put(Days.Wednesday, "3");  
        enumMap.put(Days.Thursday, "4");  
        
       
        
    }
}
