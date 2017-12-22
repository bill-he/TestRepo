package edu.upenn.cis.cis455.storage;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;

import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.model.Entity;

import edu.upenn.cis.cis455.crawler.FrontierQueue;

@Entity
public class FrontierQueueDA {
	
	PrimaryIndex<String,FrontierRecord> recordIndex;
	
	public FrontierQueueDA(String path) {
		EntityStore store = DBWrapper.getEntityStore(path);
		
		recordIndex = store.getPrimaryIndex(
				String.class, FrontierRecord.class);
	}
	
	public void addURL(String urlString) {
		URLParser parsedURL = new URLParser(urlString);
		if (!recordIndex.contains(parsedURL.getHostName())) {
			FrontierRecord record = new FrontierRecord();
			record.setHostName(parsedURL.getHostName());
			record.addURL(urlString);
			recordIndex.put(record);
		} else {
			FrontierRecord record = recordIndex.get(parsedURL.getHostName());
			record.addURL(urlString);
			record.setHostName(parsedURL.getHostName());
		}
	}
	
	public Queue<String> getHostNames() {
		if (!recordIndex.contains("hostNames")) {
			FrontierRecord record = new FrontierRecord();
			record.setHostName("hostNames");
			recordIndex.put(record);
			return new LinkedList<String>();
		} else {
			FrontierRecord record = recordIndex.get("hostNames");
			return record.getHostNames();
		}
	}
	
	public int getNumberOfStoredURLs() {
		if (!recordIndex.contains("NumberOfStoredURLs")) {
			return 0;
		} else {
			FrontierRecord record = recordIndex.get("NumberOfStoredURLs");
			return record.getInt();
		}
	}
	
	public void setNumberOfStoredURLs(int number) {
		if (!recordIndex.contains("NumberOfStoredURLs")) {
			FrontierRecord record = new FrontierRecord();
			record.setHostName("NumberOfStoredURLs");
			record.setInt(number);
			recordIndex.put(record);
		} else {
			FrontierRecord record = recordIndex.get("NumberOfStoredURLs");
			record.setInt(number);
			recordIndex.put(record);
		}
	}
	
	public URLParser getNextUrlFromHostName(String hostName) {
		FrontierRecord record = recordIndex.get(hostName);
		if (record != null) {
			return record.getNextURL();
		}
		return null;
	}
	
	public void storeURLs(FrontierQueue frontierQueue) {
		System.out.println("String URLs for the frontierQueue.");
		HashMap<String, Queue<URLParser>> queueMap = frontierQueue.queueMap();
		int count = 0;
		
		for (String hostName: frontierQueue.hostNames()) {
			if (hostName == null) continue;
			System.out.println("Hostname to Store: " + hostName);
			if (!queueMap.containsKey(hostName)) continue;
			FrontierRecord record = null;
			if (recordIndex.contains(hostName)) {
				record = recordIndex.get(hostName);
			} else {
				record = new FrontierRecord();
				record.setHostName(hostName);
			}
			
			for (URLParser parsedURL : queueMap.get(hostName)) {
				//System.out.println(count++ + "     URL storing under hostname: " + parsedURL.contructURL());
				record.addURL(parsedURL.contructURL());
			}
			recordIndex.put(record);
		}
		
		FrontierRecord record = recordIndex.get("hostNames");
		record.setHostNames(frontierQueue.hostNames());
		recordIndex.put(record);
	}
	
	public boolean isCurrentHostNameEmpty(String hostName) {
		FrontierRecord record = recordIndex.get(hostName);
		if (record == null) {
			record = new FrontierRecord();
			record.setHostName(hostName);
			recordIndex.put(record);
			return true;
		} else {
			return record.isEmpty();
		}
	}
	
	public Queue<URLParser> getQueueByHostName(String hostName) {
		FrontierRecord record = recordIndex.get(hostName);
		return record.getQueue();
	}
} 

