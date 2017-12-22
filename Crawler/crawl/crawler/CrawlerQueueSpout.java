package edu.upenn.cis.cis455.crawler;

import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.UUID;

import edu.upenn.cis.cis455.storage.URLParser;
import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.routers.IStreamRouter;
import edu.upenn.cis.stormlite.spout.IRichSpout;
import edu.upenn.cis.stormlite.spout.SpoutOutputCollector;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Values;

public class CrawlerQueueSpout implements IRichSpout {
	
	int NumberCrawledPerSwitch = 10;
    String executorId = UUID.randomUUID().toString();
    
	SpoutOutputCollector collector;
	
	private FrontierQueue FrontierQueue;
	
	private HashSet<String> crawlingURLs =
			new HashSet<String>();
	
	private HashSet<String> crawledURLs =
			new HashSet<String>();
	
	int count = 0;
	private String currentHostName = "";
	
	
	

    public CrawlerQueueSpout() {
    	System.out.println("Starting spout");
    	FrontierQueue = XPathCrawler.FrontierQueue;
    	crawlingURLs = XPathCrawler.crawlingURLs;
    	crawledURLs = XPathCrawler.crawledURLs;
    }
    
    @SuppressWarnings("rawtypes")
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        
//    	System.out.println("Spout: open");
    }

    /**
     * Shut down the spout
     */
    @Override
    public void close() {
    	System.out.println("Closing Spout");
    	FrontierQueue = null;
    	crawlingURLs = null;
    	crawledURLs = null;
    }

    /**
     * Grabs 
     */
    @Override
    public void nextTuple() {
		try {
			String urlToEmit = FrontierQueue.getNextURL().contructURL();
			
			boolean toCrawl = true;
			
			// if already crawled current url
			synchronized(crawledURLs) {
				if (crawledURLs.contains(urlToEmit)) {
					toCrawl = false;
				}
			}
			
			// if already crawling current url
			synchronized(crawlingURLs) {
				if (crawlingURLs.contains(urlToEmit)) {
					toCrawl = false;
				} else {
					crawlingURLs.add(urlToEmit);
				}
			}
			
			if (toCrawl) {
				this.collector.emit(new Values<Object>(urlToEmit));
				Date date = new Date();
				System.out.println(date.toString() + " Crawling: " + urlToEmit);
			}
	        Thread.yield();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("url"));
    	System.out.println("Spout: declareOutputFields");
    }

	@Override
	public String getExecutorId() {
    	System.out.println("Spout: getExecutorId");
		return executorId;
	}

	@Override
	public void setRouter(IStreamRouter router) {
    	System.out.println("Spout: setRouter");
		this.collector.setRouter(router);
	}
}
