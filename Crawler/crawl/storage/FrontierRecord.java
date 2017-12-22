package edu.upenn.cis.cis455.storage;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

@Entity
public class FrontierRecord {
	
	public Queue<String> hostNames 
		= new LinkedList<String>();
	
	public Queue<URLParser> frontierQueue =
		new LinkedList<URLParser>();
	
	@PrimaryKey
	private String urlHostName;
	
	public String getUrlString() {
		return urlHostName;
	}
	
	public void setHostNames(Queue<String> hostNamesSet) {
		System.out.println("Storing frontier URLs.");
		hostNames = new LinkedList<String>();
		for (String hostNamesToStore : hostNamesSet) {
			System.out.println(hostNamesToStore);
			hostNames.add(hostNamesToStore);
		}
	}
	
	public Queue<String> getHostNames() {
		urlHostName = "hostNames";
		System.out.println("hostName Size: " + hostNames.size());
		return hostNames;
	}
	
	public void setHostName(String url) {
		urlHostName = url;
	}
	
	public void addURL(URLParser urlParser) {
		frontierQueue.add(urlParser);
	}
	
	public void addURL(String urlString) {
		frontierQueue.add(new URLParser(urlString));
	}
	
	public URLParser getNextURL() {
		return frontierQueue.poll(); // TODO
	}
	
	public boolean isEmpty() {
		return frontierQueue.isEmpty();
	}
	
	public Queue<URLParser> getQueue() {
		return frontierQueue;
	}
	
	public void setInt(int number) {
		hostNames = new LinkedList<String>();
		hostNames.add(String.valueOf(number));
	}
	
	public int getInt() {
		if (hostNames.isEmpty()) {
			return 0;
		} else {
			String value = hostNames.peek();
			return Integer.parseInt(value);
		}
	}
}
