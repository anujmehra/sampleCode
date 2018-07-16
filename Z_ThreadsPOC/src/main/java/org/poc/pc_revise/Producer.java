package org.poc.pc_revise;

import java.util.List;

public class Producer implements Runnable{

	private final List<Integer> sharedList;
	private final int upperBound;
	private final String lockObject;
	
	public Producer(final List<Integer> sharedList, final int upperBound, final String lockObject){
		this.sharedList = sharedList;
		this.upperBound = upperBound;
		this.lockObject = lockObject;
	}
	
	@Override
	public void run() {
	
		int counter = 0;
		
		while(true)
		{
			synchronized(lockObject)
			{
				if(sharedList.size() == upperBound){
					try {
						System.out.println("---Producer wait() called ---");
						lockObject.wait();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}else{
					System.out.println("--producer added --" + counter);
					sharedList.add(counter);
					counter++;
					lockObject.notify();
				}
			}//end of synchronized-block
		}//end of while-loop
	}

}
