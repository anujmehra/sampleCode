package org.poc.locks._1;

public class LockImpl {

	public static void main(String args[]){
		ClassToBeLocked obj = new ClassToBeLocked();
		
		obj.method1();
		obj.method2();
		obj.method3();
		obj.method4();
	}
	
}
