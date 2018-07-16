package org.poc.custom.queue;

import java.util.ArrayList;
import java.util.List;

//FIFO
public class MyQueue<T> {

	private List<T> queueElements;
	
	public MyQueue(){
		queueElements = new ArrayList<T>();
	}
	
	public void push(T t){
		queueElements.add(t);
	}
	
	public void pop(){
		queueElements.remove(0);
	}
	
	public List<T> getValues(){
		return queueElements;
	}
	
}
