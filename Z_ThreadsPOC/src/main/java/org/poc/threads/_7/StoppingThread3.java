package org.poc.threads._7;

import org.poc.threads._3.MyThread3;

public class StoppingThread3 {

	public static void main(String args[]){
		MyThread3 obj = new MyThread3();
		
		Thread t = new Thread(obj);
		t.start();
		
		try {
			Thread.sleep(100);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		try {
			obj.wait(100);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		obj.notify();
	}
}
