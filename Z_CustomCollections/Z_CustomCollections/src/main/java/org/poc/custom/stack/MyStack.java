package org.poc.custom.stack;

import java.util.ArrayList;
import java.util.List;

//LIFO
public class MyStack<T> {

	private int lastElementIndex = -1;
	
	private List<T> stackElements;
	
	public MyStack(){
		stackElements = new ArrayList<T>();
	}
	
	public void push(T t){
		lastElementIndex++;
		stackElements.add(t);
	}
	
	public void pop(){
		stackElements.remove(lastElementIndex);
		lastElementIndex--;
	}
	
	public List<T> getValues(){
		return stackElements;
	}
}
