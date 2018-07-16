package org.poc.producerconsumer;

import java.util.ArrayList;
import java.util.List;

public class ProducerConsumerImpl {

	public static void main(String args[]){
		List<Integer> myList = new ArrayList<Integer>();
		int limit = 10;
		final String lock = "lock";
		
		Producer p = new Producer(myList,limit,lock);
		Consumer c = new Consumer(myList, limit,lock);
		Consumer2 c2 = new Consumer2(myList, limit,lock);
		
		Thread t1 = new Thread(p);
		Thread t2 = new Thread(c);
		Thread t3 = new Thread(c2);
		
		t1.start();
		t2.start();
		t3.start();
	}
}
