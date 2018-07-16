package org.poc.pc_revise;

import java.util.List;

public class Consumer implements Runnable{

	private List<Integer> sharedList;
	private int lowerBound;
	private final String lockObject;
	
	public Consumer(List<Integer> sharedList, int lowerBound, final String lockObject){
		this.sharedList = sharedList;
		this.lowerBound = lowerBound;
		this.lockObject = lockObject;
	}
	
	@Override
	public void run() {
	
		while(true){
			synchronized(lockObject){

				try{

					if(sharedList.size() == lowerBound)
					{
						System.out.println("---Consumer wait() called ---");
						lockObject.wait();
					}else{
						Thread.currentThread().sleep(1000);
						System.out.println("---Consumer data fetched--" + sharedList.get(0));
						sharedList.remove(0);
						lockObject.notify();
					}
				}catch(InterruptedException e){
					e.printStackTrace();
				}
			
			}
		}
	}

}
