package org.poc.threads._6;

public class ThreadImpl {

	public static void main(String args[]){
	
		MyThread myThread = new MyThread();
		
		Thread t1 = new Thread(myThread);
		t1.start();
	
		Thread t2 = new Thread(myThread);
		t2.start();
	}
}
