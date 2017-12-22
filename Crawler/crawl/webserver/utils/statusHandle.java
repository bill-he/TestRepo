package edu.upenn.cis.cis455.webserver.utils;

import java.util.ArrayList;
import java.util.List;

import edu.upenn.cis.cis455.webserver.workerThread;

public class statusHandle {

	List<workerThread> threadPool = null;
	
	public statusHandle( List<workerThread> tPool  ) {
		threadPool = tPool;
	}
	
	public String getAllThreadStatus(){
		
		StringBuffer status = new StringBuffer();
		
		for( workerThread t : threadPool){
			status.append("<p>Thread " + t.getThreadId() + ": " + t.getThreadState()+"</p>");
		}
		return status.toString();
	}
	
}
