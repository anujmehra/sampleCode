package threads._4;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

public class CyclicBarrierImpl {

    public static void main(final String args[]){
        final CyclicBarrier barrier = new CyclicBarrier(2);

        final MyThread obj = new MyThread(barrier);

        new Thread(obj).start();
        new Thread(obj).start();
        
        System.out.println("--- Before await() ---");
        try {
            barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
        }
        System.out.println("--- After await() ---");
        
    }
}
