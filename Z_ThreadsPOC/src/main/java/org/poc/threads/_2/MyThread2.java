package org.poc.threads._2;

public class MyThread2 implements Runnable{

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
