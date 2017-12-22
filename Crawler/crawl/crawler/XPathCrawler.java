package edu.upenn.cis.cis455.crawler;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Scanner;

import edu.upenn.cis.cis455.storage.CrawledURLDA;
import edu.upenn.cis.cis455.storage.DBWrapper;
import edu.upenn.cis.cis455.storage.FrontierQueueDA;
import edu.upenn.cis.cis455.storage.RobotsTxtInfo;
import edu.upenn.cis.cis455.storage.URLParser;
import edu.upenn.cis.cis455.storage.URLRecordDA;
import edu.upenn.cis.stormlite.Config;
import edu.upenn.cis.stormlite.LocalCluster;
import edu.upenn.cis.stormlite.TopologyBuilder;

public class XPathCrawler {
	public static String dbdDirectory;
	public static double maxSize;
	public static int numFile;
	public static URLRecordDA URLRecordDA;
	public static FrontierQueueDA frontierQueueDA;
	public static CrawledURLDA crawledURLDA;
	public static boolean showCrawling = false;
	public static int index;
	
	private static int instance = 0;
	
	public static Object uploadFileLock = new Object();
	public static Object robotsTxtLock = new Object();
	
	// container for crawledURLs (both rejectd and stored)
	public static HashSet<String> crawledURLs =
		new HashSet<String>();
	
	//(not implemented)
	public static HashSet<String> rejectedURLs =
			new HashSet<String>();
	
	// urls currently crawling
	public static HashSet<String> crawlingURLs =
		new HashSet<String>();
	
	// hostnames that are done
	public static HashSet<String> doneHostNames =
		new HashSet<String>();
	
	// urls that have error (not implemented)
	public static HashSet<String> errorURLs =
		new HashSet<String>();
	
	// queue for urls to crawl next
	public static FrontierQueue FrontierQueue;
	
	public static LocalCluster cluster;
	
	// for respecting the robot files
	public static HashMap<String, Long> hostNameDateLastAccessedMap =
		new HashMap<String, Long>();

	public static HashMap<String, Double> hostNameDelaysMap =
		new HashMap<String, Double>();

	public static HashMap<String, RobotsTxtInfo> robotTxtMap =
		new HashMap<String, RobotsTxtInfo>();
	
	// hostname's who's sitemap has been added
	public static HashSet<String> robotTxt =
		new HashSet<String>();
	
	private static final String CRAWLER_QUEUE_SPOUT = "CRAWLER_QUEUE_SPOUT";
    private static final String CRAWLER_BOLT = "CRAWLER_BOLT";
    private static final String DOC_PARSER_BOLT = "DOC_PARSER_BOLT";
    private static final String URL_FILTER_BOLT = "URL_FILTER_BOLT";
    
    public static Thread createCrawlerThread;
    public static int bucketSize = 0;
    public static AWS3 s3 = new AWS3();

	public static void main(String[] args) throws Exception {
		
    	if (!XPathCrawler.s3.s3.doesBucketExist("mantiscrawledurls")) {
    		XPathCrawler.s3.s3.createBucket("mantiscrawledurls");
    	}
		
		// print our names
    	printName(args);
    	
    	// initialize everything, and import urls
        init(args);
        
        // start StormLite
        createCluster();
        
        // listen for command to end 
        System.out.println("Enter <STOP>");
        String stopCommand = ".";
        while (!stopCommand.equals("1")) {
	        while (!stopCommand.equals("1")) {
	        	Thread.sleep(500);
	            Scanner scanner = new Scanner(System.in);
	            stopCommand = scanner.nextLine();
	        }
        }
        
        // kill the StromLife
        synchronized (uploadFileLock) {
	        cluster.killTopology("Crawler" + (instance-1));
	        cluster.shutdown();
        }

        // reinitiate
		frontierQueueDA = new FrontierQueueDA(dbdDirectory);
		crawledURLDA = new CrawledURLDA(dbdDirectory);
        
        // store crawled things
        synchronized (crawledURLs) {
        	crawledURLDA.setCrawledURLs(crawledURLs);
        	crawledURLDA.setDoneHostNames(doneHostNames);
        	crawledURLDA.setDoneRobotsTxt(robotTxt);
        }
        
        synchronized (FrontierQueue) {
//	        synchronized (crawlingURLs) {
//	        	for (String url : crawlingURLs) {
//	        		if (url == null) continue;
//	        		try {
//	            	FrontierQueue.addInFront(new URLParser(url));
//	        		} catch (Exception x) {}
//	        	}
//	        }
        	// store the current frontierQueue
	        frontierQueueDA.storeURLs(FrontierQueue);
	        
	        // store the number of storedURLs
	        frontierQueueDA.setNumberOfStoredURLs(DocumentParserBolt.bucketSize);
	        System.out.println("Stored Links: " + DocumentParserBolt.bucketSize);
        }
        
        System.out.println("Crawled Links: " + crawledURLDA.getCrawledURLsCount());
        
        
    	String directory = XPathCrawler.dbdDirectory;
    	File file = new File("IdUrl_mantis" + directory);
    	String[] directorySplit = directory.split("/");
    	if (directorySplit.length > 1) {
    		directory = directorySplit[1];
    	}
    	String bucketName = "mantis" + directory;
        XPathCrawler.s3.putObject("mantiscrawledurls", directory, file);
        
        DBWrapper.closeEnvDB();
        System.out.println("EXITING");
        System.exit(0);
    }
	
	private static void createCluster() {
		
        // creating the spouts and bolts
        CrawlerQueueSpout crawlerQueueSpout = new CrawlerQueueSpout();
        CrawlerBolt crawlerBolt = new CrawlerBolt();
        DocumentParserBolt docParserBolt = new DocumentParserBolt();
        URLFilterBolt urlFilterBolt = new URLFilterBolt();
        TopologyBuilder builder = new TopologyBuilder();

        // Only one source ("spout") for the URLs
        builder.setSpout(CRAWLER_QUEUE_SPOUT + instance, crawlerQueueSpout, 1);
        // bolts
        builder.setBolt(CRAWLER_BOLT + instance, crawlerBolt, 1).shuffleGrouping(CRAWLER_QUEUE_SPOUT + instance);
        builder.setBolt(DOC_PARSER_BOLT + instance, docParserBolt, 1).shuffleGrouping(CRAWLER_BOLT + instance);
        builder.setBolt(URL_FILTER_BOLT + instance, urlFilterBolt, 1).shuffleGrouping(DOC_PARSER_BOLT + instance);

        cluster = new LocalCluster();
       
        Config config = new Config();
        cluster.submitTopology("Crawler" + instance++, config, builder.createTopology());
	}
	
	private static void init(String[] args) throws InterruptedException {
    	
		dbdDirectory = parseBDBDir(args[1]);
		maxSize = parseMaxSize(args[2]);
		numFile = parseFileNumXML(args);
		frontierQueueDA = new FrontierQueueDA(dbdDirectory);
		crawledURLDA = new CrawledURLDA(dbdDirectory);
		Object localLock = new Object();

    	String indexString = dbdDirectory.substring(dbdDirectory.length()-1);
    	index = Integer.parseInt(indexString);
		
		// load all the crawledURLs
		synchronized (localLock) {
			System.out.println("Print Crawled: ");
//			crawledURLs.addAll(readCrawledURLsFromFile());
			for (String url : crawledURLDA.getCrawledURLs()) {
				URLParser parsedURL = new URLParser(url);
				if (parsedURL.getStorageName().equals("wikipedia")) continue;
				crawledURLs.add(url);
				System.out.println("Already crawled: " + url);
			}
			
			System.out.println("Already Crawled URL: " + crawledURLs.size());
		}
		
		synchronized (localLock) {
			doneHostNames.addAll(crawledURLDA.getDoneHostNames());
			System.out.println("Print Done HostNames: ");
			for (String doneHostName : doneHostNames) {
				System.out.println("Done HostNames: " + doneHostName);
			}
			System.out.println("Done HostName Size: " + doneHostNames.size());
		}
		
		synchronized (localLock) {
			
			HashSet<String> doneRobotFiles = crawledURLDA.getDoneRobotsTxt();
			robotTxt.addAll(crawledURLDA.getDoneRobotsTxt());
			System.out.println("Print Added RobotFiles: ");
			for (String robotTxtHostName : doneRobotFiles) {
        		String[] linkSegments = robotTxtHostName.split("\n");
        		if (linkSegments.length > 2) {
        			robotTxtHostName = linkSegments[0] + linkSegments[2];
        		} else if (linkSegments.length > 1) {
        			robotTxtHostName = linkSegments[0];
        		}
        		robotTxtHostName = robotTxtHostName.replace("\r", "");
				robotTxt.add(robotTxtHostName);
				System.out.println("Added Robot: " + robotTxtHostName);
			}
		}
		
		synchronized (localLock) {
			FrontierQueue = new FrontierQueue(frontierQueueDA, doneHostNames);
		}
		
		bucketSize = frontierQueueDA.getNumberOfStoredURLs();

        System.out.println("Already stored number of URLs: " + bucketSize);
		
		synchronized (localLock) {
			if (FrontierQueue.isEmpty()) {
				File file = new File("resources/seed.txt");
				try (BufferedReader br = new BufferedReader(new FileReader(file))) {
				    String line;
				    while ((line = br.readLine()) != null) {
				    	URLParser urlParser = new URLParser("http://www." + line.trim());
			    		FrontierQueue.add(urlParser);
						System.out.println("XPathCrawler. FrontierQueue Empty. Loading Argument: " + urlParser.getHostName());
				    }
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		DBWrapper.closeEnvDB();
	}
	
	@SuppressWarnings("unused")
	private static String parseBDBDir(String arg) {
		try {
			File file = new File(arg);
			if (file == null) throw new Exception();
		} catch (Exception ex) {
			System.out.println(
					"args[1] dbd Directory: invalid. try 'tmp/database/lihe'.");
			System.exit(-1);
		}
		return arg;
	}
	
	private static float parseMaxSize(String arg) {
		float maxSize = 0;
		try {
			maxSize = (Float.valueOf(arg)).floatValue();
		} catch (Exception ex) {
			System.out.println(
					"args[2] maxSize: invalid. Input only integer or double");
			System.exit(-1);
		}
		return maxSize;
	}
	
	private static int parseFileNumXML(String[] args) {
		int numFile = -1;
		if (args.length > 3) {
			try {
				numFile = Integer.parseInt(args[3]);
			} catch (Exception ex) {
				System.out.println("args[3] numFile: invalid. Input only integer");
				System.exit(-1);
			}
		}
		return numFile;
	}
	
    private static void printName(String[] args) {
	    if (args.length < 3) {
			System.out.println("Li Le He <lihe@seas.upenn.edu>");
			System.out.println("Mikael Mantis <mantism@seas.upenn.edu>");
			System.out.println("Leshang Chen <leshangc@seas.upenn.edu>");
			System.out.println("Steven Hwang <stevenhw@seas.upenn.edu>");
	    	System.exit(0);
	    }
    }
    
    private static void displayTextInputStream(InputStream input) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        while (true) {
            String line = reader.readLine();
            if (line == null) break;

            System.out.println("    " + line);
        }
        System.out.println();
    }
    
    private static HashSet<String> readCrawledURLsFromFile() {
    	HashSet<String> URLs = new HashSet<String>();
    	String url = ".";
    	
        File file = new File("IdUrl_mantisdec14night10pm");
	    try {

	        Scanner sc = new Scanner(file);

	        while (sc.hasNextLine()) {
	            url = sc.nextLine();
	            if (url.trim().equals("")) break;
	            url = url.split(" ")[1];
	            URLs.add(url);
	            System.out.println("Printed from File: " + url);
	        }
	        sc.close();
	    } catch (FileNotFoundException e) {
	        e.printStackTrace();
	    }
	    return URLs;
	}
}
