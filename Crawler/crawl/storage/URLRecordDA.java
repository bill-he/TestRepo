package edu.upenn.cis.cis455.storage;

import java.util.Date;
import java.util.Map;

import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.model.Entity;

@Entity
public class URLRecordDA {
	
	PrimaryIndex<String,URLRecord> recordIndex;
	
	public URLRecordDA(String path) {
		EntityStore store = DBWrapper.getEntityStore(path);
		
		recordIndex = store.getPrimaryIndex(
				String.class, URLRecord.class);
	}
	
	public boolean containsRecord(String urlInfo) {
		return recordIndex.contains(urlInfo);
	}
	
	public String getContent(String urlInfo) {
		if (!containsRecord(urlInfo)) return null;
		return recordIndex.get(urlInfo).getContent();
	}
	
	public String getContentIfNotModified(
			String urlInfo, Date lastModifiedOnline) {
		if (!containsRecord(urlInfo)) return null;
		if (recordIndex.get(urlInfo).hasBeenModified(lastModifiedOnline)) 
			return null;
		return recordIndex.get(urlInfo).getContent();
	}
	
	public void putNewRecord(String urlInfo, String content) {
		URLRecord record = new URLRecord();
		record.setUrl(urlInfo);
		record.setLastedAccessed(new Date());
		record.setContent(content);
		recordIndex.put(record);
	}
	
	public boolean setLastChecked(String urlInfo, Date lastChecked) {
		if (urlInfo == null) return false;
		URLRecord record = recordIndex.get(urlInfo);
		if (record == null) {
			record = new URLRecord();
			record.setUrl(urlInfo);
		}
		record.setLastedAccessed(lastChecked);
		recordIndex.putNoReturn(record);
		return true;
	}
	
	public boolean setNewContent(String urlInfo, String content) {
		if (urlInfo == null) return false;
		URLRecord record = recordIndex.get(urlInfo);
		if (record == null) {
			record = new URLRecord();
			record.setUrl(urlInfo);
		}
		record.setContent(content);
		record.setLastedAccessed(new Date());
		recordIndex.putNoReturn(record);
		return true;
	}
	
	public URLRecord getRecord(String urlInfo) {
		return recordIndex.get(urlInfo);
	}
	
	public Map<String, URLRecord> getMap() {
		return recordIndex.map();
	}
	
} 
