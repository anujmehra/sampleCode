package org.poc.threads._7;

public class StoppingThread1 {

	public static void main(String args[]){
		final MyThread obj = new MyThread();
		
		final Thread t = new Thread(obj);
		t.start();
		
		try {
			Thread.currentThread().sleep(100);
			
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		t.interrupt();
		
		
	}
}
