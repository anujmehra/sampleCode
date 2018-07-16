package org.poc.threads._4;

public class MyThread4 extends Thread{

	private volatile int counter = 0;
	
	public void run() 
	{
		System.out.println("start executing-->" + Thread.currentThread().getName() + "counter-->" + counter);
		counter++;
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println("Completed executing-->" + Thread.currentThread().getName() + "counter-->" + counter);
		
	}

}
