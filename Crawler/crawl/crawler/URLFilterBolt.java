package edu.upenn.cis.cis455.crawler;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;

import edu.upenn.cis.cis455.storage.CrawledURLDA;
import edu.upenn.cis.cis455.storage.DBWrapper;
import edu.upenn.cis.cis455.storage.FrontierQueueDA;
import edu.upenn.cis.cis455.storage.URLParser;
import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.bolt.IRichBolt;
import edu.upenn.cis.stormlite.bolt.OutputCollector;
import edu.upenn.cis.stormlite.routers.IStreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;

public class URLFilterBolt implements IRichBolt {
	
	Fields myFields = new Fields();
    String executorId = UUID.randomUUID().toString();
    
	private HashSet<String> crawledURLs;
	private HashSet<String> crawlingURLs;
	private FrontierQueue frontierQueue;
	private HashSet<String> rejectedURLs;

    public URLFilterBolt() {
    	frontierQueue = XPathCrawler.FrontierQueue;
    	crawledURLs = XPathCrawler.crawledURLs;
    	crawlingURLs = XPathCrawler.crawlingURLs;
    	rejectedURLs = XPathCrawler.rejectedURLs;
    }
    
	@Override
	public void cleanup() {
		// SAVE QUEUE STATE
    	System.out.println("Cleaningup URLFilter");
    	frontierQueue = null;
    	crawledURLs = null;
    	crawlingURLs = null;
    	rejectedURLs = null;
	}

	@Override
	@SuppressWarnings("unchecked")
	public void execute(Tuple input) {
		
		// get URL and Outgoing URLs
		String URLString = input.getStringByField("url");
		LinkedList<String> URLsToAdd = (LinkedList<String>) input.getObjectByField("newURLs");
		System.out.println("Filtering: " + URLString);
		
		// add the current crawled link to crawledURL
		synchronized(crawledURLs) {
			crawledURLs.add(URLString);
		}
		
		// remove the current crawled link from crawling
		synchronized(crawlingURLs) {
			crawlingURLs.remove(URLString);
		}
		
		// add the URLs needed to add to the frontierQueue
		if (URLsToAdd != null) {
			int count = 50;
			for (String URLToAdd : URLsToAdd) {
				if (URLToAdd == null) continue;
				URLParser parsedURL = new URLParser(URLToAdd);
				String hostName = parsedURL.getHostName();
				
				// don't add if I don't want to crawl this URL anymore
				synchronized (XPathCrawler.doneHostNames) {
					if (XPathCrawler.doneHostNames.contains(parsedURL.getStorageName()))
						continue;
				}
				
				// don't add if it's already crawled
				synchronized(crawledURLs) {
					if (crawledURLs.contains(URLToAdd)) {
		        		continue;
		        	}
				}
				
				// don't add if it is currently being crawled
				synchronized(crawlingURLs) {
					if (crawlingURLs.contains(URLToAdd)) {
		        		continue;
		        	}
				}
				
				// don't add if it's an image
				if (URLToAdd.endsWith(".jpg") || URLToAdd.endsWith(".png") || URLToAdd.endsWith(".gif") || 
						URLToAdd.endsWith(".pdf")) {
					System.out.println("Rejected due to restrictions: " + URLToAdd);
					synchronized(rejectedURLs) {
						rejectedURLs.add(URLToAdd);
					}
					continue;
				}
				
				// don't add if starts with a foreign language
				if (URLToAdd.startsWith("en.") || URLToAdd.startsWith("ja.") || URLToAdd.startsWith("es.") ||
					URLToAdd.startsWith("ru.") || URLToAdd.startsWith("de.") || URLToAdd.startsWith("fr.") ||
					URLToAdd.startsWith("it.") || URLToAdd.startsWith("zh.") || URLToAdd.startsWith("pt.") ||
					URLToAdd.startsWith("pl.") || URLToAdd.startsWith("nl.") || URLToAdd.startsWith("ceb.") ||
					URLToAdd.startsWith("sv.") || URLToAdd.startsWith("vi.") || URLToAdd.startsWith("war.") ||
					URLToAdd.startsWith("ar.") || URLToAdd.startsWith("az.") || URLToAdd.startsWith("bg.") ||
					URLToAdd.startsWith("zh-min-nan.") || URLToAdd.startsWith("be.") || URLToAdd.startsWith("ca.") ||
					URLToAdd.startsWith("cs.") || URLToAdd.startsWith("da.") || URLToAdd.startsWith("et.") ||
					URLToAdd.startsWith("el.") || URLToAdd.startsWith("eo.") || URLToAdd.startsWith("eu.") ||
					URLToAdd.startsWith("fa.") || URLToAdd.startsWith("gl.") || URLToAdd.startsWith("ko.") ||
					URLToAdd.startsWith("hy.") || URLToAdd.startsWith("hi.") || URLToAdd.startsWith("hr.") ||
					URLToAdd.startsWith("id.") || URLToAdd.startsWith("he.") || URLToAdd.startsWith("ka.") ||
					URLToAdd.startsWith("la.") || URLToAdd.startsWith("lt.") || URLToAdd.startsWith("hu.") ||
					URLToAdd.startsWith("ms.") || URLToAdd.startsWith("min.") || URLToAdd.startsWith("no.") ||
					URLToAdd.startsWith("nn.") || URLToAdd.startsWith("ce.") || URLToAdd.startsWith("uz.") ||
					URLToAdd.startsWith("kk.") || URLToAdd.startsWith("ro.") || URLToAdd.startsWith("sk.") ||
					URLToAdd.startsWith("sl.") || URLToAdd.startsWith("sr.") || URLToAdd.startsWith("sh.") ||
					URLToAdd.startsWith("fi.") || URLToAdd.startsWith("th.") || URLToAdd.startsWith("tr.") ||
					URLToAdd.startsWith("lb.") || URLToAdd.startsWith("ur.") || URLToAdd.startsWith("vo.") ||
					URLToAdd.startsWith("af.") || URLToAdd.startsWith("als.") || URLToAdd.startsWith("am.") ||
					URLToAdd.startsWith("an.") || URLToAdd.startsWith("ast.") || URLToAdd.startsWith("bn.") ||
					URLToAdd.startsWith("map-bms.") || URLToAdd.startsWith("ba.") || URLToAdd.startsWith("be-tarask.") ||
					URLToAdd.startsWith("bpy.") || URLToAdd.startsWith("bar.") || URLToAdd.startsWith("bs.") ||
					URLToAdd.startsWith("br.") || URLToAdd.startsWith("cv.") || URLToAdd.startsWith("cy.") ||
					URLToAdd.startsWith("fo.") || URLToAdd.startsWith("fy.") || URLToAdd.startsWith("ga.") ||
					URLToAdd.startsWith("gd.") || URLToAdd.startsWith("gu.") || URLToAdd.startsWith("hsb.") ||
					URLToAdd.startsWith("io.") || URLToAdd.startsWith("ia.") || URLToAdd.startsWith("os.") ||
					URLToAdd.startsWith("is.") || URLToAdd.startsWith("jv.") || URLToAdd.startsWith("kn.") ||
					URLToAdd.startsWith("ht.") || URLToAdd.startsWith("ku.") || URLToAdd.startsWith("ckb.") ||
					URLToAdd.startsWith("ky.") || URLToAdd.startsWith("mrj.") || URLToAdd.startsWith("lv.") 
					|| URLToAdd.startsWith("el.") || URLToAdd.startsWith("simple.") || URLToAdd.contains("mailto:")) {
					System.out.println("Rejected due to restrictions: " + URLToAdd);
					synchronized(rejectedURLs) {
						rejectedURLs.add(URLToAdd);
					}
					continue;
				}
				
				// don't add if it is a foreign language
				if (URLToAdd.contains("?lang=")) {
					if (!URLToAdd.contains("?lang=en")) {
						System.out.println("Rejected due to restrictions: " + URLToAdd);
						synchronized(rejectedURLs) {
							rejectedURLs.add(URLToAdd);
						}
						continue;
					}
				}
				
				// don't add if it doesn't end with one of these
				if (hostName.endsWith(".com") || hostName.endsWith(".org") || hostName.endsWith(".net") ||
				    hostName.endsWith(".int") || hostName.endsWith(".edu") || hostName.endsWith(".gov") || 
				    hostName.endsWith(".mil") || hostName.endsWith(".au") || hostName.endsWith(".ca") ||
				    hostName.endsWith(".eu") || hostName.endsWith(".hk") || hostName.endsWith(".ky") ||
				    hostName.endsWith(".nz") || hostName.endsWith(".sg") || hostName.endsWith(".uk") ||
				    hostName.endsWith(".uk") || hostName.endsWith(".um") || hostName.endsWith(".us") ||
				    hostName.endsWith(".fm") || hostName.endsWith(".com") || hostName.endsWith(".com") ||
				    hostName.endsWith(".tv") || hostName.endsWith(".io") || hostName.endsWith(".me") ||
				    hostName.endsWith(".co")) {
					
				} else {
					if (!URLToAdd.contains("english")) {
						System.out.println("Rejected due to restrictions: " + URLToAdd);
						synchronized(rejectedURLs) {
							rejectedURLs.add(URLToAdd);
						}
						continue;
					}
				}
				
				// otherwise add to frontierQueue
				synchronized (frontierQueue) {
					frontierQueue.add(parsedURL);
            		frontierQueue.notifyAll();
    				if (count-- < 0) break;
				}
			}
		}
	}
	
	@Override
	public void prepare(Map<String, String> stormConf, TopologyContext context, OutputCollector collector) {
		// Do nothing
	}

	@Override
	public String getExecutorId() {
		return executorId;
	}

	@Override
	public void setRouter(IStreamRouter router) {
		// Do nothing
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(myFields);
	}

	@Override
	public Fields getSchema() {
		return myFields;
	}

}

