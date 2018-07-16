package com.poc.java8.producerconsumer;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class ProducerConsumerImpl {

    public static void main(String args[]){
        
        BlockingQueue<String> shared = new ArrayBlockingQueue(1024);
        
        Producer p = new Producer(shared);
        Consumer c = new Consumer(shared);
        Consumer c2 = new Consumer(shared);
        new Thread(p).start();
        new Thread(c).start();
        new Thread(c2).start();
    }
}
