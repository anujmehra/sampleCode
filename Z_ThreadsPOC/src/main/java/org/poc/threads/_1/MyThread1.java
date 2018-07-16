package org.poc.threads._1;

public class MyThread1 implements Runnable{

	public void run() {
		System.out.println(System.currentTimeMillis() + "-" + Thread.currentThread().getName());
	}

}
