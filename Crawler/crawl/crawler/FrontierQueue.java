package edu.upenn.cis.cis455.crawler;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;

import edu.upenn.cis.cis455.storage.FrontierQueueDA;
import edu.upenn.cis.cis455.storage.URLParser;

public class FrontierQueue {

	
	private int NumberCrawledPerSwitch = 5;
	private int count = 0;
	private String hostName;
	
	private Queue<String> frontierQueueHostName =
		new LinkedList<String>();
		
	private HashMap<String, Queue<URLParser>> frontierQueueMap =
		new HashMap<String, Queue<URLParser>>();
		
	// front load all the information
	public FrontierQueue(FrontierQueueDA frontierQueueDA, HashSet<String> doneHostName) throws InterruptedException {
		int countURLs = 0; // counter to see the length of the frontierQueue
		try {
			
			// Queue of allHostNames in that order
			Queue<String> allHostNames = frontierQueueDA.getHostNames();
			System.out.println("Number of HostNames in Queue: " + allHostNames.size());
			
//			int index = XPathCrawler.index;
//			int start = allHostNames.size()/5 * (index-1);
//			int end = allHostNames.size()/5 * (index);
//			
//			System.out.println("Start: " + start);
//			System.out.println("End: " + end);
//			
//			Thread.sleep(2000);
//			
//			int count = -1; 
			// For each of the host name
			for(String hostName : allHostNames) {
//				count++;
//				if (count < start || count > end) continue;
				if (hostName == null) continue;
				if (hostName.equals("wikipedia")) continue; // blacklisted
				if (hostName.equals("nga")) continue; // blacklisted
				if (doneHostName.contains(hostName)) continue; // blacklisted
				
				// sanitize the hostnames
        		String[] linkSegments = hostName.split("\n");
        		if (linkSegments.length > 2) {
        			hostName = linkSegments[0] + linkSegments[2];
        		} else if (linkSegments.length > 1) {
        			hostName = linkSegments[0];
        		}
        		
        		if (hostName == null) continue;
        		hostName = hostName.replace("\r", "");
        		if (hostName == null) continue;
				System.out.println("HostName: " + hostName);
				
				// get the queue of urls to crawl under this Hostname
				HashSet<String> URLs = new HashSet<String>();
				double count1 = 100.0;
				int totalCount = frontierQueueDA.getQueueByHostName(hostName).size();
				for (URLParser parsedURL : frontierQueueDA.getQueueByHostName(hostName)) {
					try {
						if (Math.random() < count1/totalCount) {
							String urlString = parsedURL.contructURL();
							URLs.add(urlString.trim());
						}
					} catch (Exception e) {
						
					}
				}

				// if URLs set is not empty
				if (!URLs.isEmpty()) {
					// add HostName to Queue
	        		if (!frontierQueueHostName.contains(hostName)) {
	        			frontierQueueHostName.add(hostName);
						appendToFile("HostNames", hostName);
	        		}
					Queue<URLParser> urlsParsed = new LinkedList<URLParser>();
	        		if (frontierQueueMap.containsKey(hostName)) {
	        			urlsParsed = frontierQueueMap.get(hostName);
	        		}
					for (String URLToAdd : URLs) {
						if (urlsParsed.size() > 100) break;
						urlsParsed.add(new URLParser(URLToAdd));
						System.out.println("     " + URLToAdd);
//						appendToFile("FrontierQueue", URLToAdd);
						countURLs++;
					}
					frontierQueueMap.put(hostName, urlsParsed);
				}
			}
		} catch (Exception ex) {
			ex.printStackTrace();
			System.out.println("Error when loading FrontierQueue in the beginning");
			Thread.sleep(3000);
		}
		System.out.println("Total number in FrontierQueue: " + countURLs);
	}
	
	// see if frontierQueue is empty
	public boolean isEmpty() {
		System.out.println("frontierQueue is empty");
		return frontierQueueHostName.isEmpty();
	}
	
	// add URLs in front (URLs that are currently being crawled)
	public void addInFront(URLParser parsedURL) {
		if (!frontierQueueHostName.contains(parsedURL.getStorageName())) {
			frontierQueueHostName.add(parsedURL.getStorageName());
			Queue<URLParser> queue = new LinkedList<URLParser>();
			queue.add(parsedURL);
			frontierQueueMap.put(parsedURL.getStorageName(), queue);
		} else {
			Queue<URLParser> queue = frontierQueueMap.get(parsedURL.getStorageName());
			LinkedList<URLParser> list = (LinkedList<URLParser>) queue;
			list.addFirst(parsedURL);
			frontierQueueMap.put(parsedURL.getStorageName(), list);
		}
	}
	
	// add URL to the back of the queues
	public void add(URLParser parsedURL) {
		if (!frontierQueueHostName.contains(parsedURL.getStorageName())) {
			frontierQueueHostName.add(parsedURL.getStorageName());
			Queue<URLParser> queue = new LinkedList<URLParser>();
			queue.add(parsedURL);
			frontierQueueMap.put(parsedURL.getStorageName(), queue);
		} else {
			Queue<URLParser> queue = frontierQueueMap.get(parsedURL.getStorageName());
			double spaceLeft = 500 - queue.size();
			if (Math.random() < spaceLeft/300) {
				queue.add(parsedURL);
			}
			frontierQueueMap.put(parsedURL.getStorageName(), queue);
		}
	}
	
	// get the frontierQueueMap
	public HashMap<String, Queue<URLParser>> queueMap() {
		return frontierQueueMap;
	}
	
	// get the list of hostNames
	public Queue<String> hostNames() {
		return frontierQueueHostName;
	}
	
	// get the nextURL
	public URLParser getNextURL() throws InterruptedException {
		synchronized (this) {
			while (frontierQueueHostName.isEmpty()) {
				count = 0;
				this.wait();
			}
			
			hostName = frontierQueueHostName.peek();
//			hostName = "wikipedia";
			
			if (++count >= NumberCrawledPerSwitch) {
				frontierQueueHostName.add(hostName);
				frontierQueueHostName.poll();
				hostName = frontierQueueHostName.peek();
				count = 0;
			}
			
			Queue<URLParser> queue = frontierQueueMap.get(hostName);
			
			while (queue.isEmpty()) {
				frontierQueueMap.remove(hostName);
				frontierQueueHostName.poll();
				hostName = frontierQueueHostName.peek();
				queue = frontierQueueMap.get(hostName);
			}
			
			URLParser parsedURL = queue.poll();
			
			if (queue.isEmpty()) {
				frontierQueueMap.remove(hostName);
				frontierQueueHostName.poll();
				hostName = frontierQueueHostName.peek();
			} else {
				frontierQueueMap.put(hostName, queue);
			}
			return parsedURL;
		}
	}
	
//	@SuppressWarnings("unused")
	private void appendToFile(String fileName, String addition) {
//		if (true)
//			return;
		BufferedWriter bw = null;
		
		try {
	         // APPEND MODE SET HERE
	         bw = new BufferedWriter(new FileWriter(fileName, true));
	         bw.write(addition);
	         bw.newLine();
	         bw.flush();
         } catch (IOException ioe) {
        	 ioe.printStackTrace();
    	 } finally {               
			 try {
				bw.close();
			 } catch (IOException e) {
				e.printStackTrace();
			 }
		 }
	}
}
