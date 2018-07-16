package org.poc.blockingqueue;

import java.util.concurrent.BlockingQueue;

public class Consumer implements Runnable{

	private final BlockingQueue<Integer> myList;

	public Consumer(BlockingQueue<Integer> myList){
		this.myList = myList;
	}

	@Override
	public void run() {

		while(true)
		{
			System.out.println("inside consumer--> " + myList.poll());
			
			
		}//end of synchronized block


	}//end of method run

}//end of Consumer
