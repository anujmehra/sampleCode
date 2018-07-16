package com.poc.java8.ds;

import java.util.LinkedList;
import java.util.Queue;

public class CreateQueue {

    public static void main(String args[]){
        Queue<String> queue = new LinkedList<String>();
        
        queue.add("1");
        queue.add("2");
        queue.add("3");
        queue.add("4");
        queue.add("5");
        
        System.out.println(queue.poll()); //--> remove top element
        System.out.println(queue.peek()); //Just read top element
        System.out.println(queue.size());
    }
}
