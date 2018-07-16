package org.poc.threads._3;

public class MyThread3 implements Runnable{

	public void run() {
		System.out.println("start executing-->" + Thread.currentThread().getName());
		
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println("Completed executing-->" + Thread.currentThread().getName());
		
	}

}
