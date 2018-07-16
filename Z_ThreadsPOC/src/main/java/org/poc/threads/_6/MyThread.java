package org.poc.threads._6;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * 
 * @author anujmehra
 *
 */
public class MyThread extends Thread{

	private AtomicInteger ai = new AtomicInteger(0);
	
	@Override
	public void run() {
		System.out.println(Thread.currentThread().getId());
		System.out.println(Thread.currentThread().getId() + " value-->" + ai.get());
		ai.incrementAndGet();
		System.out.println(Thread.currentThread().getId() + " value-->" + ai.get());
	}
	
}
