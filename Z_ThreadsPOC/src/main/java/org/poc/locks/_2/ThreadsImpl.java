package org.poc.locks._2;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ThreadsImpl {

	private ReadWriteLock lock = new ReentrantReadWriteLock(Boolean.TRUE);
	
	private Lock readLock = lock.readLock();
	
	private Lock writeLock = lock.writeLock();
	
	public void readFromHashmap(){
		
	}
	
	public void writeIntoHashmap(){
		
	}
	
}
