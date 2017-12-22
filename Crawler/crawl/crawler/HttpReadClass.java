package edu.upenn.cis.cis455.crawler;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.concurrent.Callable;

public class HttpReadClass implements Runnable {
 
	private String htmlFile;
	private BufferedReader inStream;
	
	private boolean shutdown = false;
	
	public HttpReadClass (BufferedReader reader) {
		htmlFile = CrawlerBolt.htmlFile;
		inStream = reader;
	}
	
	@Override
	public void run() {
		String readIn = "";
        try {
			while ((readIn=inStream.readLine()) != "") {
				synchronized (htmlFile) {
					htmlFile += readIn + "\r\n";
					//CrawlerBolt.showCrawling = true;
				}
				System.out.println(readIn);
				
				if (htmlFile.length() > 1 * 1024 * 1024) break;
			}
		} catch (IOException e) {
			// put the errorous url somewhere TODO
			e.printStackTrace();
		}
	}
}
