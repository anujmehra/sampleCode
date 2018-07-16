package threads._1._2;

import java.util.concurrent.atomic.AtomicInteger;

public class Mythread implements Runnable{

    private final AtomicInteger ai;
    
    public Mythread(final AtomicInteger ai){
        this.ai =ai;
    }
    
    @SuppressWarnings("static-access")
    @Override
    public void run(){

       while(true){
           
           synchronized(this){
               System.out.println(Thread.currentThread().getName() + ":" + ai.incrementAndGet());
               try {
                Thread.currentThread().sleep(100);
            } catch (InterruptedException e1) {
                e1.printStackTrace();
            }
               this.notify();
               try {
                this.wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
           }
       }
        
    }

}
