package edu.upenn.cis.cis455.storage;

import java.util.HashSet;
import java.util.Set;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

@Entity
public class CrawledURLRecord {
	
	private HashSet<String> urls = new HashSet<String>();
	
	@PrimaryKey
	private String primaryKey;
	
	public void setFuction(String key) {
		primaryKey = key;
	}
	
	public void setURL(HashSet<String> urlsToStore) {
		urls.addAll(urlsToStore);
	}
	
	public HashSet<String> getURLs() {
		return urls;
	}
}
