package edu.upenn.cis.cis455.storage;

import java.util.HashSet;
import java.util.Set;

import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.model.Entity;

@Entity
public class CrawledURLDA {
	
	PrimaryIndex<String, CrawledURLRecord> recordIndex;
	
	public CrawledURLDA(String path) {
		EntityStore store = DBWrapper.getEntityStore(path);
		
		recordIndex = store.getPrimaryIndex(
				String.class, CrawledURLRecord.class);
	}
	
	public HashSet<String> getCrawledURLs() {
		if (recordIndex.contains("crawledURLs")) {
			CrawledURLRecord crawledUrlRecord = recordIndex.get("crawledURLs");
			return crawledUrlRecord.getURLs();
		} else {
			CrawledURLRecord crawledUrlRecord = new CrawledURLRecord();
			crawledUrlRecord.setFuction("crawledURLs");
			return new HashSet<String>();
		}
	}
	
	public void setCrawledURLs(HashSet<String> urlsToSet) {
		CrawledURLRecord crawledUrlRecord = null;
		if (!recordIndex.contains("crawledURLS")) {
			crawledUrlRecord = new CrawledURLRecord();
			crawledUrlRecord.setFuction("crawledURLs");
		} else {
			crawledUrlRecord = recordIndex.get("crawledURLs");
		}
		crawledUrlRecord.setURL(urlsToSet);
		recordIndex.put(crawledUrlRecord);
	}
	
	public HashSet<String> getURLs(String function) {
		if (recordIndex.contains(function)) {
			CrawledURLRecord crawledUrlRecord = recordIndex.get(function);
			return crawledUrlRecord.getURLs();
		} else {
			CrawledURLRecord crawledUrlRecord = new CrawledURLRecord();
			crawledUrlRecord.setFuction(function);
			return new HashSet<String>();
		}
	}
	
	public void setURLs(String function, HashSet<String> urlsToSet) {
		CrawledURLRecord crawledUrlRecord = null;
		if (!recordIndex.contains(function)) {
			crawledUrlRecord = new CrawledURLRecord();
			crawledUrlRecord.setFuction(function);
		} else {
			crawledUrlRecord = recordIndex.get(function);
		}
		crawledUrlRecord.setURL(urlsToSet);
		recordIndex.put(crawledUrlRecord);
	}
	
	public int getCrawledURLsCount() {
		return getCrawledURLs().size();
	}
	
	public HashSet<String> getDoneHostNames() {
		return getURLs("DoneHostNames");
	}
	
	public void setDoneHostNames(HashSet<String> doneURLs) {
		setURLs("DoneHostNames", doneURLs);
	}
	
	public HashSet<String> getDoneRobotsTxt() {
		return getURLs("DoneRobotsTxt");
	}
	
	public void setDoneRobotsTxt(HashSet<String> doneURLs) {
		setURLs("DoneRobotsTxt", doneURLs);
	}
	
	public HashSet<String> getError() {
		return getURLs("Error");
	}
	
	public void setError(HashSet<String> doneURLs) {
		setURLs("Error", doneURLs);
	}
} 

