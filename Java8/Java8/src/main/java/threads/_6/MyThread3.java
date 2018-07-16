package threads._6;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class MyThread3 implements Runnable{

    //To prevent starvation and implement fairness
    private final Lock lock = new ReentrantLock();

    @Override
    public void run() {

        try {
            this.method1();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
        }
    }

    public void method1() throws InterruptedException{
        if(lock.tryLock(5,TimeUnit.MICROSECONDS)){
            //lock.lock(); --> here never use .lock() method again .. tryLock() has already got the lock
            System.out.println(Thread.currentThread().getName() + "---- inside method 1----");
            this.method2();
        }else{
            this.method3();
        }

    }

    public void method2(){
        System.out.println(Thread.currentThread().getName() + "---- inside method 2----");
        lock.unlock();
    }

    public void method3(){
        System.out.println(Thread.currentThread().getName() + "---- inside method 3 : i didnt got the lock----");
    }
}
