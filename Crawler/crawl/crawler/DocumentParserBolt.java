package edu.upenn.cis.cis455.crawler;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.UUID;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import edu.upenn.cis.cis455.storage.URLParser;
import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.bolt.IRichBolt;
import edu.upenn.cis.stormlite.bolt.OutputCollector;
import edu.upenn.cis.stormlite.routers.IStreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis.stormlite.tuple.Values;

/**
 * A trivial bolt that simply outputs its input stream to the
 * console
 * 
 * @author zives
 *
 */

public class DocumentParserBolt implements IRichBolt {
	
	Fields schema = new Fields("url", "newURLs");
    private OutputCollector collector;
    public static int bucketSize;
    private String bucketName = "mantis";
	
	private Object uploadFileLock;

    /**
     * To make it easier to debug: we have a unique ID for each
     * instance of the PrintBolt, aka each "executor"
     */
    String executorId = UUID.randomUUID().toString();
    
    public DocumentParserBolt() {
    	
    	bucketSize = XPathCrawler.bucketSize;
    	
    	uploadFileLock = XPathCrawler.uploadFileLock;
    	String directory = XPathCrawler.dbdDirectory;
    	String[] directorySplit = directory.split("/");
    	if (directorySplit.length > 1) {
    		directory = directorySplit[1];
    	}
    	bucketName = "mantis" + directory;
    	
    	// create bucket if it doesn't exist already
    	if (!XPathCrawler.s3.s3.doesBucketExist(bucketName)) {
    		XPathCrawler.s3.s3.createBucket(bucketName);
    	}
    	
    }

	@Override
	public void cleanup() {
    	System.out.println("Cleaningup DocumentParser");
    	uploadFileLock = null;
	}

	@Override
	public void execute(Tuple input) {
		synchronized(uploadFileLock) {
			try {
				
				// load URL and htmlDocument
				String URLString = input.getStringByField("url");
				System.out.println("DocumentParserBolt: " + URLString);
				String htmlDocument = input.getStringByField("HTMLdocument");
				//System.out.println("Document: " + htmlDocument); // TODO
				
				URLParser urlInfo = new URLParser(URLString);
				String hostName = urlInfo.getHostName();
				int portNumber = urlInfo.getPortNo();
				boolean isHTTPS = urlInfo.getIsHTTPS();
	
			    
			    LinkedList<String> frontierQueue = 
			    	new LinkedList<String>();
			    
			    // null url, finish it
			    if (htmlDocument == null) {
			    	System.out.println(URLString + " not stored because htmlFile == null.");
			    	collector.emit(new Values<Object>(URLString, null));
			    	return;
			    } else if (htmlDocument.equals("")) {
			    	System.out.println(URLString + " not stored because htmlFile == \"\"");
			    	collector.emit(new Values<Object>(URLString, null));
			    	return;
			    }
			    
			    // Extract xml links
			    if (urlInfo.getFilePath().contains(".xml")) {
			    	DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			    	DocumentBuilder db = dbf.newDocumentBuilder(); 
			    	org.w3c.dom.Document doc = null;
			    	
			    	try {
			    		doc = db.parse(new InputSource(new StringReader(htmlDocument)));
			    		NodeList element = doc.getElementsByTagName("loc");
				    	synchronized (frontierQueue) {
				    		int size = Math.min(50, element.getLength());
					    	for (int x=0; x<size; x++ ) {
					    		String URL = element.item(x).getFirstChild().getNodeValue();
				    			URL = sanitizeURL(URL);
					    		frontierQueue.add(URL);
					    	}
				    	}
				    	
				    	// if the xml breaks, this is what we do
			    	} catch (Exception ex) {
			    		System.out.println("DocumentParser xml broken, extracting manually.");
				    	HashSet<String> siteMapSet = new HashSet<String>();
				    	int count = 40;
				    	while (htmlDocument.indexOf("<loc>") != -1 && count-- > 0) {
				    		htmlDocument = htmlDocument.substring(htmlDocument.indexOf("<loc>") + 5);
				    		int endIndex = htmlDocument.indexOf("</loc>");
				    		if (endIndex == -1) break;
				    		String url = htmlDocument.substring(0, endIndex);
				    		siteMapSet.add(url.trim());
				    		System.out.println("Added: " + url.trim() + " after error.");
				    		htmlDocument = htmlDocument.substring(0, htmlDocument.length()-1);
				    		htmlDocument = htmlDocument.trim();
				    	}
				    	synchronized (frontierQueue) {
					    	for (String URL : siteMapSet) {
					    		try {
					    			URL = sanitizeURL(URL);
					    			new URLParser(URL);
					    			if (!frontierQueue.contains(URL))
					    				frontierQueue.add(URL);
					    		} catch (Exception ex22) {
					    			
					    		}
					    	}
				    	}
			    	}
			    } else {
				    String objectName = bucketSize++ + "";
				    String addition = "";
				    if (bucketSize > 99000) {
				    	addition = "-2";
				    } else if (bucketSize > 180000) {
				    	addition = "-3";
				    }
				    XPathCrawler.s3.putObject(bucketName, objectName, htmlDocument);
			        appendToFile("IdUrl", objectName + " " + urlInfo.contructURL());
			        
			    	Document doc = Jsoup.parse(htmlDocument);
			    	
				    Elements links = doc.select("a[href]");
				    
			        for (Element link : links) {
			        	String newURLInfo = null;
			        	if (link.attr("abs:href").trim().isEmpty()) {
			        		
			        		String pathName = link.attr("HREF").trim();
			        		
			        		pathName = sanitizeURL(pathName);
			        		if (pathName == null) continue;
			        		
			        		if (pathName.startsWith("//")) {
			        			newURLInfo = (new URLParser(pathName.substring(2), "", portNumber, isHTTPS)).contructURL();
			        		} else {
			        			newURLInfo = (new URLParser(hostName, pathName, portNumber, isHTTPS)).contructURL();
			        		}
			        	} else {
			        		// different hostname
			        		String absUrl = link.attr("abs:href");
			        		newURLInfo = sanitizeURL(absUrl);
			        	}
			        	frontierQueue.add(newURLInfo);
			        }
			        
			        // replace characters with %-numbers
		        	for(String URL : frontierQueue) {
		        		if (URL.contains("mailto:")) continue;
		        		URL = URL.replaceAll("'", "%27");
		        		URL = URL.replaceAll(" ", "%20");
			        	appendToFile("OutgoinURLs", urlInfo.contructURL() + " " +  URL);
			        }
			    }
			    
			    collector.emit(new Values<Object>(URLString, frontierQueue));
			} catch (Exception e) {
				e.printStackTrace();
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
		try {
			input = input.split("#")[0];
		} catch (Exception ec) {
			return null;
		}
		return input.trim();
	}

	@Override
	public void prepare(Map<String, String> stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
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
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(schema);
	}

	@Override
	public Fields getSchema() {
		return schema;
	}
	
	private void appendToFile(String fileName, String addition) {
		BufferedWriter bw = null;
		
		try {
	         // APPEND MODE SET HERE
	         bw = new BufferedWriter(new FileWriter(fileName + "_" + bucketName, true));
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
	
	private static void setLINKS(String url1, String url2)
	{
		Connection conn;
		Statement mystat = null;
		ResultSet myrs = null;
		String hostName = "mydbinstance.ciynpeqwoevt.us-east-1.rds.amazonaws.com:1521";
		String user = "chenleshang";
		String password = "chenleshang";
		String database = "ORCL"; 

		try 
		{
			Class.forName("oracle.jdbc.driver.OracleDriver");
			conn = DriverManager.getConnection("jdbc:oracle:thin:@//" + hostName
					+ "/" + database, user, password);
			mystat = conn.createStatement();

			String insert = "INSERT INTO LINKS(URL1, URL2) VALUES ('" + url1 + "', '" + url2+ "')";
			System.out.println(insert);
			myrs = mystat.executeQuery(insert);

			myrs.close();
			conn.close();
		} 
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
}

