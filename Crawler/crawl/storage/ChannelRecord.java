package edu.upenn.cis.cis455.storage;

import java.util.HashSet;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

@Entity
public class ChannelRecord {
	private String xpath;
	private String ChannelAuthor;
	private HashSet<URLRecord> documents;
	
	@PrimaryKey
	private String pKey;
	
	public void setNewChannel(String channelName, String xpath, String author) {
		this.pKey = channelName;
		this.xpath = xpath;
		this.ChannelAuthor = author; 
	}
	
	public String getXPath() {
		return xpath;
	}
	
	public void setNewXPath(String newXPath) {
		xpath = newXPath;
	}
	
	public String getName() {
		return pKey;
	}
	
	public String getAuthor() {
		return ChannelAuthor;
	}
	
	public HashSet<URLRecord> getDocuments() {
		if (documents == null) documents = new HashSet<URLRecord>();
		return documents;
	}
	
	public void clearDocuments() {
		documents = new HashSet<URLRecord>();
	}
	
	public void addDocument(URLRecord urlRecord) {
		if (urlRecord == null) return;
		if (documents == null) documents = new HashSet<URLRecord>();
		boolean toAdd = true;
		for (URLRecord records : documents) {
			if (records.getUrlString().equals(urlRecord.getUrlString())) {
				toAdd = false;
				break;
			}
		}
		if (toAdd) documents.add(urlRecord);
	}

}
