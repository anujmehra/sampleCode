package org.poc.threads._3;

public class ThreadsImpl {

	public static void main(String args[])
	{

		try{
			MyThread3 obj = new MyThread3();

			Thread t1 = new Thread(obj, "t1");

			Thread t2 = new Thread(obj,"t2");

			Thread t3 = new Thread(obj,"t3");

			Thread t4 = new Thread(obj,"t4");

			Thread t5 = new Thread(obj,"t5");

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


			Thread t6 = new Thread(obj, "t6");

			Thread t7 = new Thread(obj,"t7");

			Thread t8 = new Thread(obj,"t8");

			Thread t9 = new Thread(obj,"t9");

			t6.start();
			t7.start();
			t8.start();
			t9.start();


		}catch(Exception e){
			e.printStackTrace();
		}

	}

}
