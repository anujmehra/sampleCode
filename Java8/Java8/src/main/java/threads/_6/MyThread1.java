package threads._6;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class MyThread1 implements Runnable{

    private Lock lock = new ReentrantLock();
    
    @Override
    public void run() {
        
        this.method1();
        
    }

    
    public void method1(){
        lock.lock();
        System.out.println(Thread.currentThread().getName() + "---- inside method 1----");
        this.method2();
    }
    
    public void method2(){
        System.out.println(Thread.currentThread().getName() + "---- inside method 2----");
        lock.unlock();
    }
    
    
}
