package org.poc.producerconsumer;

import java.util.List;

public class Producer implements Runnable{

	private final List<Integer> myList;
	private final int limit;
	private final String lock;
	
	public Producer(List<Integer> myList, int limit, String lock){
		this.myList = myList;
		this.limit = limit;
		this.lock = lock;
	}

	@Override
	public void run(){

		int counter = 0;
		while(true)
		{
			synchronized(lock){
		
				try{

					if(myList.size() == limit)
					{
						lock.wait();
					}else{
						if(myList.size() == 0){
							System.out.println("--producer added --" + counter);
							myList.add(counter);
							Thread.currentThread().sleep(1000);
							counter++;
							lock.notify();
						}else{
							System.out.println("--producer added --" + counter);
							myList.add(counter);
							Thread.currentThread().sleep(1000);
							counter++;
						}
							
					}
				}catch(InterruptedException e){
					e.printStackTrace();
				}
			}

		}//end of synchronized block



	}

}//end of Producer
