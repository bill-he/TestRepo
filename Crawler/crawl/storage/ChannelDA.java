package edu.upenn.cis.cis455.storage;

import java.util.HashSet;
import java.util.Map;

import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.model.Entity;

@Entity
public class ChannelDA {
	
	PrimaryIndex<String, ChannelRecord> channelRecordx;
	
	/**
	 * Creates a Data Access for the XPath Channels.
	 * @param location relative to the project_base
	 */
	
	public ChannelDA(String location) {
		EntityStore store = DBWrapper.getEntityStore(location);
		
		channelRecordx = store.getPrimaryIndex(
				String.class, ChannelRecord.class);
	}
	
	/**
	 * See is a certain channel (name) exists
	 * @param Channel name
	 * @return boolean
	 */
	
	public boolean containsChannel(String channelName) {
		return channelRecordx.contains(channelName);
	}
	
	/**
	 * Get the record of the channel.
	 * @param Channel name
	 * @return null if channel doesn't exist
	 */
	public ChannelRecord getChannel(String channelName) {
		return channelRecordx.get(channelName);
	}
	
	/**
	 * Change the XPath of an existing channel.
	 * @param Channel name, newXPath
	 * @return true is successful, otherwise false
	 */
	
	public boolean changeChannelXPath(String channelName, String newXPath) {
		if (containsChannel(channelName)) return false;
		ChannelRecord record = channelRecordx.get(channelName);
		record.setNewXPath(newXPath);
		channelRecordx.putNoReturn(record);
		return true;
	}
	
	/**
	 * Clear the documents from the channel
	 * @param Channel name
	 * @return true is successful, otherwise false
	 */
	
	public boolean clearChannel(String channelName) {
		if (containsChannel(channelName)) return false;
		ChannelRecord record = channelRecordx.get(channelName);
		record.clearDocuments();
		channelRecordx.putNoReturn(record);
		return true;
	}
	
	/**
	 * Clear the documents from the channel
	 * @param Channel name
	 * @return returns null if channel doesn't exist
	 */
	
	public String getChannelAuthor(String channelName) {
		if (containsChannel(channelName)) return null;
		ChannelRecord record = channelRecordx.get(channelName);
		return record.getAuthor();
	}
	
	/**
	 * Create new channel
	 * @param Channel name, XPath, User's name (strings)
	 * @return returns null if channel doesn't exist
	 */
	
	public void createNewChannel(String channelName, String xpath, String user) {
		ChannelRecord record = new ChannelRecord();
		record.setNewChannel(channelName, xpath,user);
		channelRecordx.putNoReturn(record);
	}

	/**
	 * Create map of all the channels
	 * @return Map<Channel Name, ChannelRecord>;
	 */
	
	public Map<String, ChannelRecord> getChannelMap() {
		return channelRecordx.map();
	}
	
	/**
	 * Get channel documents
	 * @param Channel name
	 * @return null if channel doesn't exist,
	 * empty set if it has no documents
	 */
	
	public HashSet<URLRecord> getChannelDocuments(String channelName) {
		if (containsChannel(channelName)) return null;
		ChannelRecord record = channelRecordx.get(channelName);
		return record.getDocuments();
	}
	
	/**
	 * Get channel xpath
	 * @param Channel name
	 * @return returns null if channel doesn't exist
	 */
	
	public String getXPath(String channelName) {
		if (containsChannel(channelName)) return null;
		ChannelRecord record = channelRecordx.get(channelName);
		return record.getXPath();
	}
	
	/**
	 * Put document
	 * @param Channel name
	 * @return returns false if unsuccessful
	 */
	
	public boolean putDocument(String channelName, URLRecord urlRecord) {
		if (containsChannel(channelName)) return false;
		ChannelRecord record = channelRecordx.get(channelName);
		record.addDocument(urlRecord);
		channelRecordx.putNoReturn(record);
		return true;
	}
	
	public boolean containsDocument(String channelName, URLRecord urlRecord) {
		ChannelRecord record = channelRecordx.get(channelName);
		for(URLRecord urlRec :record.getDocuments()) {
			if (urlRec.getUrlString().equals(urlRecord.getUrlString()))
				return true;
		}
		return false;
	}
	
	public String[] getXPathArray() {
		Map<String, ChannelRecord> channelMap = getChannelMap();
		String[] pathArray = new String[channelMap.size()];
		int count = 0;
		for (ChannelRecord records : channelMap.values()) {
			pathArray[count++] = records.getXPath();
		}
		return pathArray;
	}
	
} 