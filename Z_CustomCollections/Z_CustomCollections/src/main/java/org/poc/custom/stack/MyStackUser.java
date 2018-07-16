package org.poc.custom.stack;

public class MyStackUser {

	public static void main(String args[]){
		MyStack<String> obj = new MyStack<String>();
		
		obj.push("1");
		obj.push("2");
		obj.push("3");
		obj.push("4");
		obj.push("5");
		
		for(String str: obj.getValues()){
			System.out.println(str);
		}
		
		obj.pop();
		obj.pop();
		
		for(String str: obj.getValues()){
			System.out.println(str);
		}
	}
	
}
