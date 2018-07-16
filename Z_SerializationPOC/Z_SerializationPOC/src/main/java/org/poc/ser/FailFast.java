package org.poc.ser;

import java.util.ArrayList;
import java.util.List;

public class FailFast {

	
	public static void main(String... args){
	
		final List<MyObj> myList = new ArrayList<MyObj>();
		MyObj obj = new MyObj();
		obj.setName("asd");
		
		myList.add(obj);
		
		for(MyObj str: myList){
			System.out.println(str.getName());
			
			str.setName("fsdgs");
			System.out.println(str.getName());
			
			MyObj obj2 = new MyObj();
			obj2.setName("c dx cds");
			myList.add(obj2);
			System.out.println(str.getName());
		}
		
	}
	
}
