package com.poc.java8.threads._1;

import java.util.concurrent.atomic.AtomicInteger;

public class ThreadClass implements Runnable{

    private ClassWithSynchronized obj = new ClassWithSynchronized();
    
   /* @Override
    public void run() {
        ClassWithSynchronized.method3();
    }*/

    AtomicInteger ai = new AtomicInteger(1);
    
    @Override
    public void run() {
        
        if(ai.get() == 1){
            ai.set(2);
            ClassWithSynchronized.method3();
            
        }else{
            ClassWithSynchronized.method4();
        }
        
    }
}
