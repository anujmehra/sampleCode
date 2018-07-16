package org.poc.threads._1;

public class ThreadsImpl {

	public static void main(String args[])
	{
		
		try{
			MyThread1 obj = new MyThread1();
			
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
			
			
			t2.start();
			
			
			t3.start();
			
			
			t4.start();
			
			
			t5.start();
			
		}catch(Exception e){
			e.printStackTrace();
		}
		
	}
	
}
