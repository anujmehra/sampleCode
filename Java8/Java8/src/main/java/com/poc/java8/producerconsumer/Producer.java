package com.poc.java8.producerconsumer;

import java.util.concurrent.BlockingQueue;

public class Producer implements Runnable{

    private final BlockingQueue<String> shared;

    public Producer(final BlockingQueue<String> shared){
        this.shared = shared;
    }

    @Override
    public void run() {

        int i=1;
        while(true){
            try {
                shared.put(String.valueOf(i));
                System.out.println("value inserted -->" + i);
                i++;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            
        }

    }


}
