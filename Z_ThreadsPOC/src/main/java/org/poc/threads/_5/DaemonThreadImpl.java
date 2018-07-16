package org.poc.threads._5;

public class DaemonThreadImpl {

	public static void main(String args[]){
		MyThread myThread = new MyThread();
		
		Thread t1 = new Thread(myThread);
		t1.setDaemon(true);
		
		t1.start();
	}
}
