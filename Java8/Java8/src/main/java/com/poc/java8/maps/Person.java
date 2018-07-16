package com.poc.java8.maps;

public class Person implements Comparable<Person>{

    private String fName;

    public Person(final String fName){
        this.fName = fName;
    }
    
    public String getfName() {
    
        return fName;
    }

    public void setfName(String fName) {
    
        this.fName = fName;
    }
    
   

    @Override
    public int compareTo(Person o) {

        return o.getfName().compareTo(this.getfName());
    }
    
    /*@Override
    public boolean equals(Object o){
        return false;
    }*/
    
}
