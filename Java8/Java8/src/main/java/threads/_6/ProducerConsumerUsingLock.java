package threads._6;

import java.util.Deque;
import java.util.LinkedList;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ProducerConsumerUsingLock {

    public static void main(final String args[]){

        final Deque<String> shared = new LinkedList<String>();
        final int limit = 10;
        final Lock aLock = new ReentrantLock();
        final Condition condVar = aLock.newCondition();

        ProducerConsumerUsingLock obj = new ProducerConsumerUsingLock();

        final ProducerConsumerUsingLock.Producer p = obj.new Producer(shared, limit, aLock ,condVar);

        final ProducerConsumerUsingLock.Consumer c = obj.new Consumer(shared, limit, aLock ,condVar);

        new Thread(p).start();
        new Thread(c).start();
    }

    private class Producer implements Runnable{

        final Deque<String> shared;
        final int limit;
        final Lock aLock;
        final Condition condVar;

        public Producer(final Deque<String> shared, final int limit, final Lock aLock ,final Condition condVar){
            this.shared = shared;
            this.limit = limit;
            this.aLock = aLock;
            this.condVar = condVar;
        }

        @Override
        public void run() {

            aLock.lock();
            try {
                while(true){
                    if (shared.size() == limit) {
                        try {
                            condVar.await();
                        } catch (InterruptedException e) { }
                    }

                    System.out.println("Adding in producer -->" + shared.size() + 1);
                    shared.addLast(String.valueOf(shared.size() + 1));
                    condVar.signalAll();
                    try {
                        Thread.currentThread().sleep(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

            } finally {
                aLock.unlock();
            }

        }
    }


    private class Consumer implements Runnable{

        final Deque<String> shared;
        final int limit;
        final Lock aLock;
        final Condition condVar;

        public Consumer(final Deque<String> shared, final int limit, final Lock aLock ,final Condition condVar ){
            this.shared = shared;
            this.limit = limit;
            this.aLock = aLock;
            this.condVar = condVar;
        }

        @Override
        public void run() {


            try {
                aLock.lock();

                while(true){

                    if (shared.size() == 0) {
                        try {
                            condVar.await();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }else{
                        String str = shared.pollFirst();
                        System.out.println("Consuming in consumer --> " + str);
                        condVar.signalAll();
                    }
                    

                }
            }finally {
                aLock.unlock();
            }

        }//end of run method
    }//end of consumer class
}//end of outer class

