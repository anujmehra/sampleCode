package org.poc.blockingqueue;

import java.util.concurrent.BlockingQueue;

public class Producer implements Runnable{

	private final BlockingQueue<Integer> myList;

	public Producer(BlockingQueue<Integer> myList){
		this.myList = myList;
	}

	@Override
	public void run(){

		int counter = 0;
		while(true)
		{
			for(int i=0;i<100;i++){
				try {
					myList.put(i);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			
		}//end of synchronized block



	}

}//end of Producer
