package edu.upenn.cis.cis455.storage;

import java.util.HashSet;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

@Entity
public class UserRecord {

	private String password;
	private HashSet<String> channels;
	
	@PrimaryKey
	private String pKey;
	
	public void setNewUser(String user, String pass) {
		pKey = user;
		password = pass;
	}
	
	public String getPassword() {
		return password;
	}
	
	public void subscribeToChannel(String channel) {
		if (channel == null) return;
		if (channels == null) channels = new HashSet<String>();
		channels.add(channel);
	}
	
	public HashSet<String> getSubscribedChannels() {
		if (channels == null) channels = new HashSet<String>();
		return channels;
	}
}
