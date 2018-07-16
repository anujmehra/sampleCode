package org.poc.threads._7;

public class MyThread2 implements Runnable{

	@Override
	public void run() {
		
		boolean flag = true;
		
		while(flag){
			System.out.println("--- Staring Thread ---");
			/*try {
				Thread.currentThread().sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}*/
			
			System.out.println("--- After Sleep---");
			
			System.out.println("--- Before Checking interrupted---");
			
			if(Thread.currentThread().interrupted()){
				System.out.println("-- Thread is interrupted --");
				flag = false;
			}else{
				System.out.println("-- Thread is not interrupted --");
			}
			
			System.out.println("--- After Checking interrupted---");
		}
		
	}

}
