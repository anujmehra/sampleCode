package org.poc.blockingqueue;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class ProducerConsumerImpl {

	public static void main(String args[]){
		BlockingQueue<Integer> input = new LinkedBlockingQueue<Integer>();
		
		Producer p = new Producer(input);
		Consumer c = new Consumer(input);
		
		Thread t1 = new Thread(p);
		Thread t2 = new Thread(c);
		
		t1.start();
		t2.start();
	}
}
