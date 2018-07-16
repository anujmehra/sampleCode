package org.poc.pc_revise;

import java.util.ArrayList;
import java.util.List;


public class PCImpl {

	public static void main(String args[]){
		List<Integer> myList = new ArrayList<Integer>();
		int limit = 10;
		
		String lock = "LOCK";
	
		Producer p = new Producer(myList,limit,lock);
		Consumer c = new Consumer(myList, 0,lock);
		
		Thread t1 = new Thread(p);
		Thread t2 = new Thread(c);
		
		t1.start();
		t2.start();
	}
}
