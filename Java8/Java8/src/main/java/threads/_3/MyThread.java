package threads._3;

import java.util.concurrent.CountDownLatch;

public class MyThread implements Runnable{

    private final CountDownLatch latch;

    public MyThread(final CountDownLatch latch){
        this.latch = latch;
    }

    @SuppressWarnings("static-access")
    @Override
    public void run() {

        System.out.println("inside thread : " + Thread.currentThread().getName());
        try {
            Thread.currentThread().sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }finally{
            latch.countDown();
        }
    }

}
