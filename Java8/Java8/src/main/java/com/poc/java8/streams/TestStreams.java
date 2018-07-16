package com.poc.java8.streams;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class TestStreams {

    public static void main(final String args[]){
        
        final List<Student> students = new ArrayList<>();
        students.add(new Student("F1", "L1", 10));
        students.add(new Student("F2", "L2", 11));
        students.add(new Student("F3", "L3", 12));
        students.add(new Student("F4", "L4", 13));
        students.add(new Student("F5", "L5", 14));
        
        
        
        final List<Student> newList = students.stream().filter((student) -> student.getAge() > 10).collect(Collectors.toList());
        System.out.println(newList.size());
        
        final Map<Integer, Student> myMap = students.stream().collect(Collectors.toMap(Student::getAge, Function.identity()));
        System.out.println(myMap.size());
        
        
    }
}

