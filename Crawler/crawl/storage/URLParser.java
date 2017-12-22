package edu.upenn.cis.cis455.storage;

import com.sleepycat.persist.model.KeyField;
import com.sleepycat.persist.model.Persistent;

@Persistent
public class URLParser implements Comparable<URLParser> {
	@KeyField(1)
	private String hostName = "";
	@KeyField(2)
	private String filePath ="";	
	@KeyField(3)
	private int portNo = 80;
	@KeyField(4)
	boolean isHTTPS = false;
	
	/**
	 * Constructor called with raw URL as input - parses URL to obtain host name and file path
	 */
	URLParser () {
		
	}
	
	public URLParser(String docURL) {
		docURL = docURL.toLowerCase();
		docURL = docURL.split("#")[0];
		
		// 'https://www.facebook.com/' == null or == ""
		if(docURL == null || docURL.equals(""))
			return;
		
		// trim the white space
		docURL = docURL.trim();
		
		// if 'https://www.facebook.com/' start with 'http://' or 'https://'
		if(!(docURL.startsWith("http://") || docURL.startsWith("https://")) 
				|| docURL.length() < 9)
			return;
		
		// Stripping off 'http://' or 'https://'
		if (docURL.startsWith("http://")) docURL = docURL.substring(7);
		if (docURL.startsWith("https://")) {
			docURL = docURL.substring(8);
			isHTTPS = true;
		}
		
		//If starting with 'www.' , stripping that off too
//		if(docURL.startsWith("www."))
//			docURL = docURL.substring(4);
		
		// finding index of the first '/' in "facebook.com/" [16]
		int i = 0;
		while(i < docURL.length()){
			char c = docURL.charAt(i);
			if(c == '/')
				break;
			i++;
		}
		
		// address = "facebook.com"
		String address = docURL.substring(0,i);
		
		// setting file path:
		// if nothing after '.com' then filePath = "/"
		if(i == docURL.length())
			filePath = "/";
		// else substring that and filePath = after '.com'
		else
			filePath = docURL.substring(i); //starts with '/'
		
		// no address "www.facebook.com"
		if(address.equals("/") || address.equals(""))
			return;
		
		// doesn't have ':' in 'www.facebook.com' or 'localhost:8080'
		// get hostname and port number
		if(address.indexOf(':') != -1){
			String[] comp = address.split(":",2);
			hostName = comp[0].trim();
			try{
				portNo = Integer.parseInt(comp[1].trim());
			}catch(NumberFormatException nfe){
				portNo = isHTTPS ? 443 : 80;
			}
		} else {
			hostName = address;
			portNo = isHTTPS ? 443 : 80;
		}
	}
	
	public URLParser(String hostName, String filePath){
		this.hostName = hostName;
		this.filePath = filePath;
		this.portNo = 80;
		this.isHTTPS = false;
	}
	
	public URLParser(String hostName,int portNo,String filePath){
		this.hostName = hostName;
		this.portNo = portNo;
		this.filePath = filePath;
		this.isHTTPS = false;
	}
	
	public URLParser(String hostName, String filePath,
			int portNo, boolean isHTTPS){
		this.hostName = hostName;
		this.portNo = portNo;
		this.filePath = filePath;
		this.isHTTPS = isHTTPS;
	}
	
	public String getHostName(){
		return hostName;
	}
	
	public String getStorageName(){
		String[] localNames = hostName.split("\\.");
		if (localNames.length > 1) {
			String mid = localNames[localNames.length - 2];
			if ((mid.equals(".com") || mid.equals(".org") || mid.equals(".net") ||
					mid.equals(".int") || mid.equals(".edu") || mid.equals(".gov") || 
					mid.equals(".mil") || mid.equals(".au") || mid.equals(".ca") ||
					mid.equals(".eu") || mid.equals(".hk") || mid.equals(".ky") ||
					mid.equals(".nz") || mid.equals(".sg") || mid.equals(".uk") ||
					mid.equals(".uk") || mid.equals(".um") || mid.equals(".us") || mid.equals(".co"))) {
				if (localNames.length >= 3) {
					System.out.println(localNames[localNames.length - 3]);
					return localNames[localNames.length - 3];
				}
			}
			return mid;
		}
		return hostName;
	}
	
	public void setHostName(String s){
		hostName = s;
	}
	
	public int getPortNo(){
		return portNo;
	}
	
	public void setPortNo(int p){
		portNo = p;
	}
	
	public String getFilePath(){
		return filePath;
	}
	
	public void setFilePath(String fp){
		filePath = fp;
	}
	
	public boolean getIsHTTPS(){
		return isHTTPS;
	}
	
	public String contructURL() {
		String url;
		if (isHTTPS) {
			url = "https://";
		} else {
			url = "http://";
		}
		
		url += hostName;
		//url += ":" + portNo;
		if (!hostName.endsWith("/") && !filePath.startsWith("/")) {
			url += "/";
		}
		url += filePath;
		
		return url;
	}
	
	public int compareTo(URLParser o) {
		if (!this.hostName.equals(o.hostName)) {
			return -1;
		} else if (!this.filePath.equals(o.filePath)) {
			return -1;
		} else if (this.portNo != o.portNo) {
			return -1;
		} else if (!(this.isHTTPS == o.isHTTPS)) {
			return -1;
		}
		return 0;
	}
}
