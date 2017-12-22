package edu.upenn.cis.cis455.storage;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;

import javax.net.ssl.HttpsURLConnection;

/**
 * Keeps track of restricted accesses by crawler
 * @author billhe
 *
 */

public class RobotsTxtInfo {
	
	public HashMap<String,ArrayList<String>> disallowedLinks;
	public HashMap<String,ArrayList<String>> allowedLinks;
	
	public HashMap<String,Integer> crawlDelays;
	public ArrayList<String> sitemapLinks;
	public ArrayList<String> userAgents;

	private String robotURL;
	
	public RobotsTxtInfo(){
	}
	
	public RobotsTxtInfo(String robotURL){
		this.robotURL = robotURL;
		
		// user + links
		disallowedLinks = new HashMap<String,ArrayList<String>>();
		allowedLinks = new HashMap<String,ArrayList<String>>();
		
		// user + delay
		crawlDelays = new HashMap<String,Integer>();
		sitemapLinks = new ArrayList<String>();
		
		// user
		userAgents = new ArrayList<String>();
		
		populate();
	}
	
	private void populate() {
		try {
			System.out.println(robotURL +": Robot Downloading");
			URL url = new URL(robotURL);
			
			BufferedReader inStream = null;
			int code = 0;
			if (robotURL.startsWith("http://")) {
				HttpURLConnection connection = (HttpURLConnection)url.openConnection();
				connection.setRequestMethod("GET");
				connection.connect();

				code = connection.getResponseCode();
				inStream = new BufferedReader(new InputStreamReader(
					connection.getInputStream()));
			} else if (robotURL.startsWith("https://")) {
				HttpsURLConnection connection = (HttpsURLConnection)url.openConnection();
				connection.setRequestMethod("GET");
				connection.connect();

				code = connection.getResponseCode();
				inStream = new BufferedReader(new InputStreamReader(
						connection.getInputStream()));
			} else {
				disallowedLinks = new HashMap<String,ArrayList<String>>();
				allowedLinks = new HashMap<String,ArrayList<String>>();
				crawlDelays = new HashMap<String,Integer>();
				sitemapLinks = new ArrayList<String>();
				userAgents = new ArrayList<String>();
				return;
			}
			
			if (code != 200) {
				disallowedLinks = new HashMap<String,ArrayList<String>>();
				allowedLinks = new HashMap<String,ArrayList<String>>();
				crawlDelays = new HashMap<String,Integer>();
				sitemapLinks = new ArrayList<String>();
				userAgents = new ArrayList<String>();
				return;
			}
			
	        String readIn = ".";
	        String[] split;
	        String agent = null;
	        String path;
			
	        while ((readIn=inStream.readLine()) != null) {
	        	int index = readIn.indexOf("#");
	        	if (index != -1) {
	        		readIn.substring(0, index);
	        	}
	        	
	        	if (readIn.startsWith("User-agent:")) {
	        		split = readIn.split(":");
	        		agent = split[1].trim();
	        		if (!userAgents.contains(agent)) {
	        			userAgents.add(agent);
	        		}
	        	} else if (readIn.startsWith("Allow: ")) {
	        		if (agent == null) continue;
	        		split = readIn.split(":");
	        		path = split[1].trim();
	        		addAllowedLink(agent, path);
	        	} else if (readIn.startsWith("Disallow: ")) {
	        		if (agent == null) continue;
	        		split = readIn.split(":");
	        		path = split[1].trim();
	        		addDisallowedLink(agent, path);
	        	} else if (readIn.startsWith("Crawl-delay: ")) {
	        		if (agent == null) continue;
	        		split = readIn.split(":");
	        		path = split[1].trim();
	        		crawlDelays.put(agent, Integer.parseInt(path));
	        	} else if (readIn.startsWith("Sitemap: ")) {
	        		if (agent == null) continue;
	        		split = readIn.split(": ");
	        		path = split[1].trim();
	        		addSitemapLink(path);
	        	}
	        }
	        return;
		} catch (Exception e) {
			disallowedLinks = new HashMap<String,ArrayList<String>>();
			allowedLinks = new HashMap<String,ArrayList<String>>();
			crawlDelays = new HashMap<String,Integer>();
			sitemapLinks = new ArrayList<String>();
			userAgents = new ArrayList<String>();
			return;
		}
		
	}
	
	public void addDisallowedLink(String key, String value){
		if(!disallowedLinks.containsKey(key)){
			ArrayList<String> temp = new ArrayList<String>();
			temp.add(value);
			disallowedLinks.put(key, temp);
		}
		else{
			ArrayList<String> temp = disallowedLinks.get(key);
			if(temp == null)
				temp = new ArrayList<String>();
			temp.add(value);
			disallowedLinks.put(key, temp);
		}
	}
	
	public void addAllowedLink(String key, String value){
		if(!allowedLinks.containsKey(key)){
			ArrayList<String> temp = new ArrayList<String>();
			temp.add(value);
			allowedLinks.put(key, temp);
		}
		else{
			ArrayList<String> temp = allowedLinks.get(key);
			if(temp == null)
				temp = new ArrayList<String>();
			temp.add(value);
			allowedLinks.put(key, temp);
		}
	}
	
	public void addCrawlDelay(String key, Integer value){
		crawlDelays.put(key, value);
	}
	
	public void addSitemapLink(String val){
		sitemapLinks.add(val);
	}
	
	public void addUserAgent(String key){
		userAgents.add(key);
	}
	
	public boolean containsUserAgent(String key){
		return userAgents.contains(key);
	}
	
	public ArrayList<String> getDisallowedLinks(String key){
		return disallowedLinks.get(key);
	}
	
	public ArrayList<String> getAllowedLinks(String key){
		return allowedLinks.get(key);
	}
	
	public int getCrawlDelay(String key){
		return crawlDelays.get(key);
	}
	
	public ArrayList<String> getSiteMap() {
		return sitemapLinks;
	}
	
	public void print(){
		for(String userAgent:userAgents){
			System.out.println("User-Agent: "+userAgent);
			ArrayList<String> dlinks = disallowedLinks.get(userAgent);
			if(dlinks != null)
				for(String dl:dlinks)
					System.out.println("Disallow: "+dl);
			ArrayList<String> alinks = allowedLinks.get(userAgent);
			if(alinks != null)
					for(String al:alinks)
						System.out.println("Allow: "+al);
			if(crawlDelays.containsKey(userAgent))
				System.out.println("Crawl-Delay: "+crawlDelays.get(userAgent));
			System.out.println();
		}
		if(sitemapLinks.size() > 0){
			System.out.println("# SiteMap Links");
			for(String sitemap:sitemapLinks)
				System.out.println(sitemap);
		}
	}
	
	public boolean crawlContainAgent(String key){
		return crawlDelays.containsKey(key);
	}
}
