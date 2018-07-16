package org.poc.threads._7;

public class MyThread implements Runnable{

	
	@Override
	public void run() {

		try {
			while(!Thread.currentThread().isInterrupted()){
				System.out.println("---- Thread is running ----");
			}
			
			System.out.println("---- thread Interrupted ----");
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(e.getLocalizedMessage());
		}
		
		
	}
	
}
