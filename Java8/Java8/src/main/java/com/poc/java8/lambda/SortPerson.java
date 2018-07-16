package com.poc.java8.lambda;

import java.util.ArrayList;
import java.util.List;

public class SortPerson {

    public static void main(final String args[]){

        final Person p1 = new Person(10, "Z1");

        final Person p2 = new Person(20, "Y1");

        final Person p3 = new Person(30, "C1");

        final List<Person> personList = new ArrayList<Person>();
        personList.add(p1); personList.add(p2); personList.add(p3);

        /*Comparator<Person> byName = new Comparator<Person>() {
            @Override
            public int compare(Person o1, Person o2) {
                return o1.getName().compareTo(o2.getName());
            }
        };
        personList.sort(byName);
        
        System.out.println(personList);*/
        
        System.out.println("---Before Sorting---");
        personList.forEach((person)->System.out.println(person.getName()));
        
        personList.sort((Person person1, Person person2) -> {
            return person1.getName().compareTo(person2.getName());
        });
        
        System.out.println("---After Sorting---");
        personList.forEach((person)->System.out.println(person.getName()));

        
        personList.sort((Person person1, Person person2) -> {
            return String.valueOf(person1.getAge()).compareTo(String.valueOf(person2.getAge()));
        });
        
        System.out.println("---After Sorting---");
        personList.forEach((person)->System.out.println(person.getName()));
        
    }
}