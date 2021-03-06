package org.poc.threads._2;

public class ThreadsImpl {

	public static void main(String args[])
	{
		try{
			MyThread2 obj = new MyThread2();
			
			Thread t1 = new Thread(obj);
			t1.setName("t1");
			
			Thread t2 = new Thread(obj);
			t2.setName("t2");
			
			Thread t3 = new Thread(obj);
			t3.setName("t3");
			
			Thread t4 = new Thread(obj);
			t4.setName("t4");
			
			Thread t5 = new Thread(obj);
			t5.setName("t5");
			
			t1.start();
			t1.join();
			
			t2.start();
			t2.join();
			
			t3.start();
			t3.join();
			
			t4.start();
			t4.join();
			
			t5.start();
			t5.join();
		}catch(Exception e){
			e.printStackTrace();
		}
		
	}
	
}
