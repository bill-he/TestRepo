package edu.upenn.cis.cis455.crawler;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;

import javax.net.ssl.HttpsURLConnection;

import edu.upenn.cis.cis455.storage.RobotsTxtInfo;
import edu.upenn.cis.cis455.storage.URLParser;
import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.bolt.IRichBolt;
import edu.upenn.cis.stormlite.bolt.OutputCollector;
import edu.upenn.cis.stormlite.routers.IStreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis.stormlite.tuple.Values;


public class CrawlerBolt implements IRichBolt {
	
	Fields schema = new Fields("url", "HTMLdocument");
	private OutputCollector collector;
	String executorId = UUID.randomUUID().toString();
	
	private Object lock = new Object();
	private Object robotsTxtLock;
	
	public static String htmlFile = "";
	
	private double maxSize;   
	private FrontierQueue frontierQueue;
	private HashSet<String> crawledURLs;
	private HashSet<String> errorURLs;
	private HashSet<String> robotTxt;
	private HashSet<String> crawlingURLs;
	
	private HashMap<String, Long> hostNameDateLastAccessedMap;
	private HashMap<String, Double> hostNameDelaysMap;
	private HashMap<String, RobotsTxtInfo> robotTxtMap;
	
    
    public CrawlerBolt() {
    	frontierQueue = XPathCrawler.FrontierQueue;
    	maxSize = XPathCrawler.maxSize;
    	crawledURLs = XPathCrawler.crawledURLs;
    	hostNameDateLastAccessedMap = XPathCrawler.hostNameDateLastAccessedMap;
    	hostNameDelaysMap = XPathCrawler.hostNameDelaysMap;
    	robotTxtMap = XPathCrawler.robotTxtMap;
    	errorURLs =  XPathCrawler.errorURLs;
    	robotTxt = XPathCrawler.robotTxt; 
    	crawlingURLs = XPathCrawler.crawlingURLs;
    	robotsTxtLock = XPathCrawler.robotsTxtLock;
    }
    
    @Override
    public void prepare(Map<String,String> stormConf, 
    		TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }
    
    private void flushHeader(OutputStream outStream, 
    		HashMap<String, String> requestHeaders, 
    		String data) throws IOException {

    	String host = requestHeaders.get("Host");
    	requestHeaders.remove("Host");
        data += "Host: "+ host +"\r\n";
        for (String header : requestHeaders.keySet()){
            data += header+": "+ requestHeaders.get(header) +"\r\n";
        }
        data += "\r\n";

        outStream.write(data.getBytes());
        outStream.flush();
    }
    
    private boolean flushHeaderWithDelay(OutputStream outStream, 
    		HashMap<String, String> requestHeaders, 
    		String data, String hostName) throws Exception {
	
    	if (hostNameDelaysMap.containsKey(hostName)) {
    		Double delay = hostNameDelaysMap.get(hostName);
    		System.out.println("delay: " + delay);

    		if (delay > 100) {
    			return false;
    		}
    		long lastSentRequest = hostNameDateLastAccessedMap.get(hostName);
    		
			long time = (long) (lastSentRequest + delay*1000 - System.currentTimeMillis());
    		System.out.println("waitTime: " + time);
    		while (lastSentRequest + delay*1000 - System.currentTimeMillis() > 0)
    			Thread.sleep(100);
			
    		hostNameDateLastAccessedMap.put(hostName, System.currentTimeMillis());
            flushHeader(outStream, requestHeaders, data);
    	} else {
    		flushHeader(outStream, requestHeaders, data);
    	}
    	return true;
    }
    
    private String[] getHeaderValue(String line, String header) {
    	if (line == null) return null;
        if (line.contains(header)) {
        	String[] length = line.split(": ");
        	String valueString = length[1].trim();
        	String[] values = valueString.split(";");
        	for(int x=0; x<values.length; x++) {
        		values[x] = values[x].trim();
        	}
        	return values;
        }
        return null;
    }
    
    private RobotsTxtInfo getRobotsTxt(URLParser urlInfo) {
    	URLParser newUrlInfo = 
    			new URLParser(urlInfo.getHostName(),
    					"/robots.txt",urlInfo.getPortNo(), urlInfo.getIsHTTPS());
    	
    	if (!robotTxtMap.containsKey(newUrlInfo.getStorageName())) {
    		RobotsTxtInfo robotFile = new RobotsTxtInfo(newUrlInfo.contructURL());
    		robotTxtMap.put(newUrlInfo.getStorageName(), robotFile);
    		System.out.println("RobotFile Hostname added: " + newUrlInfo.getStorageName().trim());
    		
        	String robot = "*";
        	if (robotFile.containsUserAgent("cis455crawler")) {
        		robot = "cis455crawler";
        	}
        	double delay = 0;
        	if (robotFile.crawlContainAgent(robot)) {
        		delay = robotFile.getCrawlDelay(robot);
        		hostNameDelaysMap.put(urlInfo.getStorageName(), delay);
        		hostNameDateLastAccessedMap.put(urlInfo.getStorageName(), (long) (System.currentTimeMillis() - delay*1000));
        	}
        	
        	synchronized (frontierQueue) {
        		if (!robotTxt.contains(newUrlInfo.getStorageName())) {
        			
	        		for (String url : robotFile.getSiteMap()) {
	        			url = sanitizeURL(url);
	        			URLParser urlParsedSitemap = new URLParser(url.trim());
		        		String siteMapURL = urlParsedSitemap.contructURL();	  
		        		synchronized (crawledURLs) {
			    			if (!crawledURLs.contains(siteMapURL)) {
			        			frontierQueue.add(urlParsedSitemap);
			        			frontierQueue.notifyAll();
			        			System.out.println("Added Sitemap for (" + newUrlInfo.getStorageName().trim() + "): " + siteMapURL);
			        		}
		        		}
	        		}
        		}
        		robotTxt.add(newUrlInfo.getStorageName().trim());
			}
        	return robotFile;
    	}
    	
    	return robotTxtMap.get(newUrlInfo.getStorageName().trim());
    }
    
    private boolean doesRobotAllow(RobotsTxtInfo robotsTxt, URLParser urlInfo) {
    	boolean doesRobotAllow = true;
    	if (robotsTxt == null) return doesRobotAllow;
    	String robot = "*";
    	String pathName = urlInfo.getFilePath();
    	if (robotsTxt.containsUserAgent("cis455crawler")) {
    		robot = "cis455crawler";
    	}
    	
    	if (robotsTxt.getDisallowedLinks(robot) != null)
	    	for (String path : robotsTxt.getDisallowedLinks(robot)) {
	    		if (pathName.startsWith(path)) {
	    			doesRobotAllow = false;
	    			break;
	    		}
	    	}
    	
    	if (robotsTxt.getAllowedLinks(robot) != null)
	    	for (String path : robotsTxt.getAllowedLinks(robot)) {
	    		if (pathName.startsWith(path)) {
	    			return true;
	    		}
	    	}
    	if (!doesRobotAllow) System.out.println("RobotxTxt doesn't allow " + urlInfo.contructURL());
    	return doesRobotAllow;
    }

    @Override
    public void execute(Tuple input) {
        String urlString = input.getStringByField("url");
    	try {
	        
	        CrawlerThread crawlerTask = new CrawlerThread(urlString);
	        
	        Thread taskThread = new Thread(crawlerTask);
	        taskThread.start();
	        long startTime = (new Date()).getTime();
	        long lastUpdate = startTime + 17000;
	        String html = "";
	        
	        Thread.sleep(30);
	        boolean toContinue = true;
	        
	        while (true && toContinue) {
        		String value = crawlerTask.getValue();
        		toContinue = crawlerTask.toContinue;
	        	if (value == null) {
	        		collector.emit(new Values<Object>(urlString, null));
    				System.out.println(urlString + ": Thread returned null");
	        		try { taskThread.interrupt(); } catch (Exception ec) {}
	        		return;
	        	}
	        	if (new Date().getTime() - startTime > 17000) {
    				if (value.equals("")) {
        				System.out.println(urlString + ": Thread Reached time limit of 20 seconds, value is still empty");
    					throw new Exception();
    				} else {
        				System.out.println(urlString + ": Thread Reached time limit of 20 seconds");
    				}
	        		collector.emit(new Values<Object>(urlString, value));
	        		try { crawlerTask.stop(); } catch (Exception ec) {}
	        		toContinue = false;
	        		Thread.sleep(1500);
	        	} else if (crawlerTask.isDone()) {
    				System.out.println(urlString + ": Thread Reached End");
	        		collector.emit(new Values<Object>(urlString, value));
	        		return;
	        	}
	        	
        		if (!html.equals(value)) {
        			html = new String (value);
        			lastUpdate = (new Date()).getTime();
        		} else {
        			if (new Date().getTime() - lastUpdate > 3000) {
        				System.out.println(urlString + "Not writing anymore");
    	        		try { crawlerTask.stop(); } catch (Exception ec) {}
    	        		toContinue = false;
    	        		Thread.sleep(1500);
    	        		collector.emit(new Values<Object>(urlString, value));
    	        		return;
        			}
        		}
        		Thread.sleep(300);
	        }
    		String value = crawlerTask.getValue();
    		collector.emit(new Values<Object>(urlString, value));
    	} catch (Exception ex) {
			collector.emit(new Values<Object>(urlString, null));
			synchronized (errorURLs) {
				errorURLs.add(urlString);
			}
    	}
    }
    
	private String sanitizeURL(String input) {
		String[] linkSegments = input.split("\n");
		if (linkSegments.length > 2) {
			input = linkSegments[0] + linkSegments[2];
		} else if (linkSegments.length > 1) {
			input = linkSegments[0];
		}
		input = input.replace("\r", "");
		return input.trim();
	}
    
    @Override
    public void cleanup() {
    	System.out.println("Cleaningup CrawlerBolt");
    	frontierQueue = null;
    	crawledURLs = null;
    	hostNameDateLastAccessedMap = null;
    	hostNameDelaysMap = null;
    	robotTxtMap = null;
    	errorURLs = null;
    	robotTxt = null; 
    	crawlingURLs = null;
    	robotsTxtLock = null;
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(schema);
    }
    
	@Override
	public String getExecutorId() {
		return executorId;
	}
	
	@Override
	public void setRouter(IStreamRouter router) {
		this.collector.setRouter(router);
	}
	
	@Override
	public Fields getSchema() {
		return schema;
	}
	
	private class CrawlerThread implements Runnable {
		private volatile String htmlFile = "";
		private String urlString;
		private volatile boolean toContinue = true;

		public CrawlerThread (String name) {
			urlString = new String(name);
		}
		
		public void stop() {
			toContinue = false;
		}
		
	    @Override
	    public void run() {
	    	try {
	    		
	    		// setup
		        URLParser urlInfo = new URLParser(urlString);
		        System.out.println(new Date() +  executorId + " . Crawling: " + urlString);
		        
				String hostName = urlInfo.getHostName();
				String pathName = urlInfo.getFilePath();
				int portNumber = urlInfo.getPortNo();
				boolean isHTTPS = urlInfo.getIsHTTPS();
		    	boolean doesRobotAllow = true;
		    	synchronized(robotsTxtLock) {
		    		
			    	RobotsTxtInfo robotsTxt = getRobotsTxt(urlInfo);
			    	doesRobotAllow = doesRobotAllow(robotsTxt, urlInfo);
			    	// setup ends1
			    	
		    		// robot file don't allow
			    	if (!doesRobotAllow) { 
			    		System.out.println(urlInfo.contructURL() + ": Robots.txt Restricted. Not downloading");
			    		htmlFile = null;
			    	}
		    	}
		    	
		    	while (doesRobotAllow) {
		    		// SETUP for HTTP(S)
					HashMap<String, String> requestHeaders = 
						new HashMap<String, String>();
					SimpleDateFormat dateFormat = 
						new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zzz");
					dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
					
					BufferedReader inStream = null;
					OutputStream outStream = null;
					Socket socket = null;
					HttpsURLConnection urlConnection = null;
					if (isHTTPS) {
						URL url = new URL(urlString);
						urlConnection =
							(HttpsURLConnection) url.openConnection();
						urlConnection.setDoOutput(true);
						outStream = urlConnection.getOutputStream();
						try {
							inStream = new BufferedReader(new InputStreamReader(
									urlConnection.getInputStream()));
						} catch (FileNotFoundException ex) {
							System.out.println(urlString + " is is File that is not found.");
						}
					} else {
						socket = new Socket(hostName, portNumber);
						outStream = socket.getOutputStream();
				        inStream = new BufferedReader(new InputStreamReader(
				        	socket.getInputStream()));
					}
					// SET UP COMPLETE
					
					// send the headRequest
					String data = "HEAD" + " "+ pathName +" HTTP/1.1\r\n";
					requestHeaders.clear();
					requestHeaders.put("User-Agent", "cis455crawler");
					requestHeaders.put("Host", hostName + ":" + portNumber);
					
			        if (!flushHeaderWithDelay(outStream, requestHeaders, data, urlInfo.getStorageName())) {
			        	htmlFile = null;
			        	break;
			        }
			        
			        String header = ".";
			        int contentLength = -1;
			        String contentType = "";
			        Date lastModified = null;
			        String[] values;
			        String responseCode;
			        
			        if (isHTTPS) {
			        	responseCode = Integer.toString(urlConnection.getResponseCode());
			        } else {
			        	String response = inStream.readLine();
		//	        	System.out.println(urlString + ": " + response); //TODO
				        String[] responseParam = response.split(" ");
				        responseCode = responseParam[1];
			        }
			        
			        System.out.println("StatusCode: " + responseCode);
			        
			        if (!responseCode.equals("200") && !responseCode.equals("301") 
			        		&& !responseCode.equals("302") && !responseCode.equals("303")) {
			        	//crawledURLs.add(urlString);
			        	htmlFile = null;
		    			System.out.println("Response Code: " + responseCode + " not 200 or 301 " + urlString);
			        	break;
			        }
		        	
			        if (isHTTPS) {
			        	contentLength = urlConnection.getContentLength();
			        	urlConnection.getHeaderField("content-length");
			        	//System.out.println("contentLength:" + contentLength);
			        	
			        	contentType = urlConnection.getContentType();
			        	//System.out.println("contentType:" + contentType);
			        	if (urlConnection.getLastModified() != 0) {
			        		lastModified = new Date(urlConnection.getLastModified());
			        		//lastModified = dateFormat.parse(lastModified.toString());
			        		//System.out.println("Last Modified: " + lastModified);
			        	}
			        	
			        	// language
			        	String temp;
			        	if ((temp = urlConnection.getHeaderField("Content-Language")) != null) {
			        		if (!temp.contains("en")) {
					        	htmlFile = null;
				    			System.out.println("Language not English " + urlInfo);
					        	break;
				        	}
			        	}
			        	
			        	if ((temp = urlConnection.getHeaderField("Location")) != null && 
			        			(responseCode.equals("301") || responseCode.equals("303"))) {
			        		synchronized (frontierQueue) {
			        			URLParser parsedURL = new URLParser(temp.trim());
	        	        		String siteMapURL = parsedURL.contructURL();	  
	        	    			if (!crawledURLs.contains(siteMapURL)) {
	        	        			frontierQueue.add(parsedURL);
	        	        			frontierQueue.notifyAll();
	        	        			System.out.println("Added Redirect1: " + siteMapURL);
	        	        		}
		        			}
		                	
							System.out.println(urlString + ": Not Downloading (Redirect)");
							System.out.println("Redirected Location: " + temp.trim());
				        	htmlFile = null;
				        	break;
			        	}
			        } else {
			        	String location = null;
		            	boolean acceptableLang = true;
			        	while ((header=inStream.readLine()) != null) {  
//				            System.out.println(header); //TODO
			        		values = getHeaderValue(header, "Content-Language: ");
				            if (values != null) {
				            	acceptableLang = false;
				            	for (String val : values) {
				            		System.out.println("Content-Language: " + val);
				            		if (val.contains("en")) {
				            			acceptableLang = true;
					            		System.out.println("Language hit English!");
				            		}
				            		break;
				            	}
				            }
			        		values = getHeaderValue(header, "Location: ");
				            if (values != null) 
				            	location = values[0];
				            values = getHeaderValue(header, "Content-Length: ");
				            if (values != null) 
				            	contentLength = Integer.parseInt(values[0]);
				            values = getHeaderValue(header, "Content-Type: ");
				            if (values != null) contentType = values[0];
				            values = getHeaderValue(header, "Last-Modified: ");
				            if (values != null)  lastModified = dateFormat.parse(values[0]);
				            if (header.trim().equals("")) break;
				        }

			        	// language is not english
		            	if (!acceptableLang) {
			    			System.out.println("Language not English: " + urlInfo.contructURL());
		            		htmlFile = null;
		            		break;
		            	}
			        	
			        	if ((responseCode.equals("301") || responseCode.equals("302") || responseCode.equals("303")) && location != null) {
			        		// will have handled in URLFilter TODO
				        	//crawledURLs.add(urlString);

			        		synchronized (frontierQueue) {
			        			URLParser parsedURL = new URLParser(location.trim());
	        	        		String urlToPut = parsedURL.contructURL();	  
	        	    			if (!crawledURLs.contains(urlToPut)) {
	        	        			frontierQueue.add(parsedURL);
	        	        			frontierQueue.notifyAll();
	        	        			System.out.println("Added Redirect2: " + urlToPut);
	        	        		}
		        			}
							System.out.println(urlString + ": Not Downloading (Redirect)");
							System.out.println("Redirected Location: " + location.trim());
				        	htmlFile = null;
				        	break;
				        }
			        }
			        
			        // contentType
			        boolean isValidType = false;
			        if (contentType != null) {
			        	
			        	if (contentType.contains("text/html") ||
		        			contentType.contains("text/xml") ||
		        			contentType.contains("application/xml") ||
		        			contentType.contains("+xml")) {
			        		isValidType = true;
				        } else {
				        	System.out.println("contentType: " + contentType);
				        }
			        }
			        
			        if (!isValidType) {
						System.out.println(urlString + ": Not Downloading (Not Valid Type)");
						// will have handled in URLFilter TODO
			        	//crawledURLs.add(urlString);
			        	htmlFile = null;
			        	break;
			        }
			        
			        // contentLength
			        if (contentLength > maxSize*1028*1028) {
						System.out.println(urlString + ": Not Downloading (Exceeded Max Size)");
						// will have handled in URLFilter TODO
			        	//crawledURLs.add(urlString);
			        	htmlFile = null;
			        	break;
			        }
			        
			        inStream.close();
			        outStream.close();
			        if (isHTTPS) urlConnection.disconnect();
			        else socket.close();
					
					if (isHTTPS) {
						URL url = new URL(urlString);
						urlConnection =
							(HttpsURLConnection) url.openConnection();
						urlConnection.setDoOutput(true);
						urlConnection.setReadTimeout(14000);
						outStream = urlConnection.getOutputStream();
						
						inStream = new BufferedReader(new InputStreamReader(
							urlConnection.getInputStream()));
					} else {
						InetAddress address = InetAddress.getByName(hostName); 
						socket = new Socket(address.getHostAddress(), portNumber);
						socket.setSoTimeout(14000);
				        outStream = new DataOutputStream(socket.getOutputStream());
				        inStream = new BufferedReader(new InputStreamReader(
				        	socket.getInputStream()));
					}
					
					
					// send the GET Request
			        data = "GET "+ pathName + " HTTP/1.1\r\n";
					requestHeaders.clear();
					requestHeaders.put("User-Agent", "cis455crawler");
					requestHeaders.put("Host", hostName + ":" + portNumber);
			        // requestHeaders.put("Accept", "text/html");
					
					flushHeaderWithDelay(outStream, requestHeaders, data, hostName);
					
					if (!isHTTPS) {
						header = "";
						while ((header=inStream.readLine()) != null) { 
		//					System.out.println(header); 
				            if (header.trim().equals("")) break;
				        }
					}
					
//					CloseSocketThread closeSocket = new CloseSocketThread(inStream, outStream,
//						isHTTPS, urlConnection, socket);
//					Thread closeSocketThread = new Thread(closeSocket);
//					closeSocketThread.start();
					
					if (contentLength > 0) {
						System.out.println(urlString + ": Downloading1");
						System.out.println("CONTENT LENGTH: " + contentLength);
						if (urlString.contains(".xml")) {
							contentLength = Math.min(contentLength, 100000);
						} else {
							contentLength = Math.min(contentLength, 600000);
						}
							
				        byte[] bytes = new byte[contentLength];
				        try {
			        		htmlFile = "";
					        for(int i=0; i<contentLength; i++) {
//					        	System.out.println(i + ":" + contentLength);
	 			        		bytes[i] = (byte) inStream.read();
	 			        		if ((i+4)%2000 == 0) {
	 			        			htmlFile = new String(bytes);
	 			        		}
					        	if (!toContinue) throw new SocketTimeoutException();
					        }
				        	
			        	} catch (SocketTimeoutException ex) {
			        		ex.printStackTrace();
			        		System.out.println("SocketTimeoutError");
			        	}
				        htmlFile = new String(bytes);
					} else {
						System.out.println(urlString + ": Downloading2");
				        String readIn = ".";
				        int indicator = 0;
				        
				        int count = 10000;
				        try {
			        			htmlFile = "";
					        while ((readIn=inStream.readLine()) != null) {
					        	readIn = readIn.trim();
					        	htmlFile += readIn.trim() + "\r\n";
//			        			System.out.println(readIn);
		
					        	if (readIn.trim().equals("")) {
					        		indicator++;
					        	} else {
					        		indicator = 0;
					        	}
					        	
					        	if (indicator > 20) {
					        		break;
					        	}
					        	if (htmlFile.length() > maxSize * 1024 * 2014) break;
					        	if (!inStream.ready()) break;
					        	if (urlString.contains(".xml")) {
					        		if (count-- < 0) break;
					        	}
					        	if (!toContinue) throw new SocketTimeoutException();
					        }
				        } catch (SocketTimeoutException ex) {
				        	System.out.println("SocketTimedOut");
				        }
					}
		
			        inStream.close();
			        outStream.close();
			        if (isHTTPS) urlConnection.disconnect();
			        else socket.close();	
			        
			        break;
			    }
			    
			    //collector.emit(new Values<Object>(urlString, htmlFile));
	    	
	    	} catch (Exception ex) {
	    		ex.printStackTrace();
				System.out.println("Error on: " + urlString);
				htmlFile = null;
				synchronized (errorURLs) {
					errorURLs.add(urlString);
				}
	    	}
	    	done = true;
	    }

	    private volatile boolean done = false;
	    
	    public String getValue() {
	        return htmlFile;
	    }
	    
	    public boolean isDone() {
	        return done;
	    }
	}
	
	private class CloseSocketThread implements Runnable {
		private BufferedReader inStream;
		private OutputStream outStream;
		private boolean isHTTPS;
		private HttpsURLConnection urlConnection;
		private Socket socket;

		public CloseSocketThread (BufferedReader inStream, OutputStream outStream,
				boolean isHTTPS, HttpsURLConnection urlConnection, Socket socket) {
			this.inStream = inStream;
			this.outStream = outStream;
			this.isHTTPS = isHTTPS;
			this.urlConnection = urlConnection;
			this.socket = socket;
		}

		@Override
		public void run() {
			try {
				Thread.sleep(1300);
				inStream.close();
		        outStream.close();
		        if (isHTTPS) urlConnection.disconnect();
		        else socket.close();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.out.println("THREAD CloseSocketThread ENDED!!!");
				
		}
	}
	
}
