package com.poc.java8;

import java.util.concurrent.BlockingQueue;

public class Consumer implements Runnable{

    private final BlockingQueue<String> shared;
    
    public Consumer(final BlockingQueue<String> shared){
        this.shared = shared;
    }
    
    @Override
    public void run() {

       while(true){
           String str;
        try {
            str = shared.take();
            System.out.println( Thread.currentThread().getName() + " : value consumed --> " + str);
        } catch (InterruptedException e1) {
            e1.printStackTrace();
        }
           try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
           
       }
        
    }

}
