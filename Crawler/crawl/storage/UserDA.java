package edu.upenn.cis.cis455.storage;

import java.util.HashSet;

import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.model.Entity;


@Entity
public class UserDA {
	
	PrimaryIndex<String, UserRecord> UserRecordIdx;
	
	public UserDA(String location) {
		EntityStore store = DBWrapper.getEntityStore(location);
		
		UserRecordIdx = store.getPrimaryIndex(
				String.class, UserRecord.class);
	}
	
	public boolean containsUser(String user) {
		return UserRecordIdx.contains(user);
	}
	
	public void createNewUser(String user, String pass) {
		UserRecord record = new UserRecord();
		record.setNewUser(user, pass);
		UserRecordIdx.putNoReturn(record);
	}
	
	public String getUserPassword(String user) {
		UserRecord record = UserRecordIdx.get(user);
		return record.getPassword();
	}
	
	public void subscribeToChannel(String user, String channel) {
		UserRecord record = UserRecordIdx.get(user);
		record.subscribeToChannel(channel);
		UserRecordIdx.putNoReturn(record);
	}
	
	public HashSet<String> getUserSubscribedChannel(String user) {
		UserRecord record = UserRecordIdx.get(user);
		return record.getSubscribedChannels();
	}
} 