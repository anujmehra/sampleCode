package org.poc.threads._7;

public class StoppingThread2 {

	public static void main(String args[]){
		final MyThread2 obj = new MyThread2();
		
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
