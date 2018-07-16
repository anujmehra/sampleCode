package com.poc.java8.threads;

import java.util.concurrent.atomic.AtomicInteger;

public class MythreadClass implements Runnable{

    final AtomicInteger counter;
    final String lock;

    public MythreadClass(final AtomicInteger  counter, final String lock){
        this.counter = counter;
        this.lock = lock;
    }

    @Override
    public void run() {
        while(true){
            synchronized(lock){
                System.out.println(Thread.currentThread().getName() + ":" + counter.incrementAndGet());
                lock.notify();
                try {
                    lock.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

}
