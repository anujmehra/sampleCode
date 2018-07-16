package com.poc.java8;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class ProducerConsumerImpl {

    public static void main(String args[]){
        
        int counter = 1;
        
        Producer p = new Producer(counter);
        Consumer c = new Consumer(counter);
        
        new Thread(p).start();
        new Thread(c).start();
        
    }
}
