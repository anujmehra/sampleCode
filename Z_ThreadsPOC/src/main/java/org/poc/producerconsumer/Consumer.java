package org.poc.producerconsumer;

import java.util.List;

public class Consumer implements Runnable{

	private final List<Integer> myList;
	private final int limit;
	private final String lock;
	
	public Consumer(List<Integer> myList, int limit, String lock){
		this.myList = myList;
		this.limit = limit;
		this.lock = lock;
	}

	@Override
	public void run() {

		while(true)
		{
			synchronized(lock){
				try{

					if(myList.size() == 0)
					{
						lock.wait();
					}else{
						System.out.println("---Consumer data fetched--" + myList.get(0));
						myList.remove(0);
						Thread.currentThread().sleep(10);
						lock.notify();
					}
				}catch(InterruptedException e){
					e.printStackTrace();
				}
			}

		}//end of synchronized block


	}//end of method run

}//end of Consumer
