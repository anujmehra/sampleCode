package org.poc.locks._1;

public class ClassToBeLocked {
	
	public synchronized void method1(){
		System.out.println("---inside method1---");
	}
	
	public synchronized void method2(){
		System.out.println("---inside method2---");
	}
	
	public synchronized void method3(){
		System.out.println("---inside method3---");
	}
	
	public synchronized void method4(){
		System.out.println("---inside method4---");
	}

	
	
}
