package com.poc.java8.threads._1;

public class ClassWithSynchronized {

    public synchronized void method1(){
        System.out.println(Thread.currentThread().getName() + "---inside method1()-----");
        
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    
    public synchronized void method2(){
        System.out.println(Thread.currentThread().getName() + "---inside method2()-----");
        
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    
    public static synchronized void method3(){
        System.out.println(Thread.currentThread().getName() + "---inside method3()-----");
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    
    public static synchronized void method4(){
        System.out.println(Thread.currentThread().getName() + "---inside method4()-----");
    }
}
