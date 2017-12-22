package edu.upenn.cis.cis455.storage;

import java.util.Date;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

@Entity
public class URLRecord {
	
	private Date lastAccessed;
	private String content;
	private int contentLength;
	
	@PrimaryKey
	private String urlInfo;
	
	public String getContent() {
		return content;
	}
	
	public int getContentLength() {
		return contentLength;
	}
	
	public String getUrlString() {
		return urlInfo;
	}
	
	public Date getLastAccessed() {
		return lastAccessed;
	}
	
	public void setUrl(String url) {
		urlInfo = url;
	}
	
	public void setContent(String content) {
		this.content = content;
		this.contentLength = content.length();
	}
	
	public boolean hasBeenModified(Date timeLastModified) {
		if (timeLastModified == null || lastAccessed == null) {
			return true;
		}
		return timeLastModified.after(lastAccessed);
	}
	
	public void setLastedAccessed(Date timeLastChecked) {
		if (timeLastChecked != null) {
			lastAccessed = timeLastChecked;
		}
	}
}
