package threads._1._2;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * One thread will print 1, other will print 2, then first 3, then secoond 4 and so on
 *
 */
public class TwoThreadsImpl {

    public static void main(String args[]){
        AtomicInteger ai = new AtomicInteger(0);
        
        Mythread obj = new Mythread(ai);
        
        Thread t1 = new Thread(obj);
        t1.setName("th-1");
        t1.start();

        Thread t2 = new Thread(obj);
        t2.setName("th-2");
        t2.start();
    }
}
