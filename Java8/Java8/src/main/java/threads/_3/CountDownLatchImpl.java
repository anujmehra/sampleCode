package threads._3;

import java.util.concurrent.CountDownLatch;

public class CountDownLatchImpl {

    public static void main(final String args[]){
        final CountDownLatch latch = new CountDownLatch(2);

        final MyThread obj = new MyThread(latch);

        new Thread(obj).start();
        new Thread(obj).start();

        try {
            System.out.println("----- wait start ----");
            latch.await();
            System.out.println("----- wait over ----");
        } catch (final InterruptedException e) {
            e.printStackTrace();
        }
    }
}
