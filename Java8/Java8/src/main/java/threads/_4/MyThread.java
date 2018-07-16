package threads._4;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

public class MyThread implements Runnable{

    private final CyclicBarrier barrier;
    
    public MyThread(final CyclicBarrier barrier){
        this.barrier = barrier;
    }

    @SuppressWarnings("static-access")
    @Override
    public void run() {

        System.out.println("inside thread : " + Thread.currentThread().getName());
        try {
            Thread.currentThread().sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }finally{
            try {
                barrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
            }
        }
        
        System.out.println(Thread.currentThread().getName() + ":" + "--- after barrier await() ---");
    }
    
}
