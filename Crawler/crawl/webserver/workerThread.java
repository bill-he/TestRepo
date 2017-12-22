package edu.upenn.cis.cis455.webserver;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;

import org.apache.log4j.Logger;


import edu.upenn.cis.cis455.webserver.myServer.ShutdownControl;
import edu.upenn.cis.cis455.webserver.utils.HttpRequestParser;
import edu.upenn.cis.cis455.webserver.utils.HttpResponseUtils;
import edu.upenn.cis.cis455.webserver.utils.statusHandle;
import edu.upenn.cis.cis455.webserver.myServer.errorWriter;


public class workerThread extends Thread {
	
	Logger log = Logger.getLogger(workerThread.class);

	
	// Instance variables
	String state = "";
	String root = "";
	int t_id;
	


	ShutdownControl shutdownCtrl = null;
	boolean personalShutdownFlag = false;
	Lock shutdownLock;
	Socket requestSocket = null;
	myBlockingQueue reqQ;
	statusHandle statusHandle = null;
	HashMap<String, HttpServlet> servletMap;
	HashMap<String, String> servletURLMap;
	static HashMap<String, myHttpServletSession> sessionMap;
	errorWriter errorwriter;
	// End Instance variables

	
	// for communicating Persistent Connection intentions or connection close intensions
	class ConnectionPreference {

		private boolean close;


		public void setConnectionPreference(boolean keepalive){
			close = keepalive;
		}

		public boolean getConnectionPreference(){
			return close;
		}

	}

	private class StringComparator implements Comparator<String>{

		@Override
		public int compare(String s1, String s2) {
			return s1.compareTo(s2);
		}
		
	}
		
	
	public workerThread(int i, String s, myBlockingQueue q, String rootdir, ShutdownControl shutd){
		t_id = i;
		state = s;
		reqQ = q;
		root = rootdir;
		shutdownCtrl = shutd;
		shutdownLock = new ReentrantLock();
		//log.debug("Initialized workerThread with |id: " + t_id + " |state: " + state + "|requestQ_ref: " + reqQ.hashCode());
	}
	
	public int getThreadId() {
		return t_id;
	}
	
	//for logging pretty-printing Thread
	private String threadMessage(String msg){
		StringBuffer m = new StringBuffer();
		m.append("Thread " + t_id + ": ");
		m.append(msg);
		return m.toString();
	}
	
	
	// for grabbing the thread's current state for status in "Control" page
	public String getThreadState(){
		
		String currentstate_snap = "";
		synchronized(state){
			currentstate_snap = state;
		}
		
		return currentstate_snap;
		
	}
	
	/** 
	 * Set the statusHandle class for this thread. statusHandle just contains the obj refs
	 *   of all other threads in the thread pool so that this thread can query the status of
	 *   all other Threads when a control page is requested.
	 **/
	public void setStatusHandle( statusHandle sh ){
		statusHandle = sh;
	}
	

	/** 
	 * Set the setServlets Map for this thread. servlets just contains the class literals
	 *   when a servlet is ready to be invoked
	 **/
	public void setServlets( HashMap<String, HttpServlet> servlets  ){
		servletMap = servlets; 
	}
	
	/** 
	 * Set the servletURLMappings for this thread. mappings just contains the url patterns
	 *   for the servlets so that when processing the queries, we can use the regex provided
	 *   in the web.xml to match request to Servlet.
	 **/
	public void setServletURLMappings( HashMap<String, String> mappings  ){
		servletURLMap = mappings; 
	}
	
	/** 
	 * Set the sessionMap for this thread. sessionMappings just contains the key-value pairs
	 *   for the uuids to session objects. A thread could create a session and store it on the 
	 *   "server" and subsequently access it per cookie-indicated string.
	 **/
	public void setSessionMap( HashMap<String, myHttpServletSession> sessionMappings  ){
		sessionMap = sessionMappings; 
	}
	
	/** 
	 * Set the errorlog writer for this thread. 
	 **/
	public void setErrorWriter( errorWriter error){
		errorwriter = error;
	}
	
	
	// Check if the URI specified by the request string was a URI
	private int checkifURL(String filename){
		
		String lower = filename.toLowerCase();
		
		if( lower.startsWith("http://")){
			return 1;
		}
		else if(lower.startsWith("https://") ){
			return 2;
		}
		return 0;
		
	}
	
	
	// If given a Absolute path in the form of a URL, return the portion just after the host and port
	//   number ("from / root")
	private String parseURLforResource(String filename, int http){
		
		String httpstripped = "";
		
		if(http == 1){
			httpstripped = filename.substring("http://".length());
			log.debug(threadMessage("http:// : " + httpstripped));
		}
		else if (http == 2){
			httpstripped = filename.substring("https://".length());
			log.debug(threadMessage("https:// : " + httpstripped));
		}
		
		
		// we want to keep the slash after the port number 
		String hostportstripped = httpstripped.substring(httpstripped.indexOf("/"));
		log.debug(threadMessage("url stripped : " + hostportstripped));
		
		return hostportstripped;
		
	}
	
	
	public String getControlPageText(){
		
		StringBuffer controlPage = new StringBuffer();
		File f = new File("resources/controlpage.html");
		//InputStream inputStream = workerThread.class.getResourceAsStream("resources/controlpage.html");
		
		
		String line;
		try {
			BufferedReader reader = new BufferedReader(new FileReader(f));
			
			while( (line = reader.readLine()) != null ){
				controlPage.append(line);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			errorwriter.writeErrorLog(e.toString());
			e.printStackTrace();
		}
		
		
		// input Thread statuses in response.
		String statuses = statusHandle.getAllThreadStatus();
		
		String temp = controlPage.toString();
		String controlWithStatus = temp.replace("<!-- Include Thread Status HERE -->", statuses);
		
		String errortext = errorwriter.readErrorLog();
		controlWithStatus = controlWithStatus.replace("<!-- Include Error Log HERE -->", errortext);
		
		
		return controlWithStatus;
		
	}
	
	// Simple check to see if the filepath contains any up-directory references, which would mean
	//   user is attempting to access outside the root directory.
	public boolean validatePath(Path filePath){
		
		//Path filePath = Paths.get(file).toAbsolutePath();
		
		log.debug("Absolute path of file/directory: " + filePath.toString());
		log.debug("Absolute path of root: " + root);
		
		if( filePath.startsWith(root) ){
			return true;
		}
		return false;
		
	}
	
	
	// Method for server to propogate shutdown to threads by set the personal_shutdownflag that 
	// each thread can check after handling a request.
	public void setShutdown(){
		shutdownLock.lock();
		personalShutdownFlag = true;
		shutdownLock.unlock();
	}
	
	
	
	// get the suffix for the file to determine the type
	private String getMimeType(String file){
		
		int dotIndex = file.lastIndexOf('.');
		if(dotIndex == -1){
			return null;
		}
		String ext = file.substring(dotIndex+1, file.length()).toLowerCase(); 
		
		log.debug(threadMessage("ext found: " + ext));
		
		
		if(ext.compareTo("html") == 0){
			return "text/html";
		}
		else if(ext.compareTo("css") == 0){
			return "text/css";
		}
		else if(ext.compareTo("txt") == 0){
			return "text/plain";
		}
		else if(ext.compareTo("jpg") == 0 || ext.compareTo("jpeg") == 0){
			return "image/jpeg";
		}
		else if(ext.compareTo("gif") == 0){
			return "image/gif";
		}
		else if(ext.compareTo("png") == 0){
			return "image/png";
		}
		else{
			return null;
		}
		
	}
	
	
	private static void sendBinaryData(FileInputStream fis, DataOutputStream outStream)
	        throws Exception
	{
	    // Construct a 1K buffer to hold bytes on their way to the socket.
	    byte[] buffer = new byte[1024];
	    int bytes = 0;

	    // Copy requested file into the socket's output stream.
	    while ((bytes = fis.read(buffer)) != -1)// read() returns minus one, indicating that the end of the file
	    {
	        outStream.write(buffer, 0, bytes);
	    }
	}

	
	private boolean checkifStillValid(myHttpServletSession session) throws IllegalStateException{

		log.debug("Checking if session is valid..");
		// check if a maxinactiveInterval is set... >0 means never timeout.
		if(session.isValid() == false){
			return false;
		}

		// can through illegalstateException if already invalidated manually
		if(session.getMaxInactiveInterval() <= 0){  
			return true;
		}

		log.debug("Session has a timeout...");


		Date aliveUntil = new Date(session.getLastAccessedTime() + session.getMaxInactiveInterval()*1000); 
		Date now = Calendar.getInstance().getTime();


		SimpleDateFormat dateFormat = new SimpleDateFormat(
				"EEE dd MMM yyyy hh:mm:ss zzz", Locale.US);


		log.debug(" lastaccessTime:  " +dateFormat.format(new Date(session.getLastAccessedTime())));
		log.debug(" inactive interval: " + session.getMaxInactiveInterval());
		log.debug(" ALIVE until: " + dateFormat.format(aliveUntil));
		log.debug(" NOW: " + dateFormat.format(now));

		if(now.before(aliveUntil) == true ){
			log.debug("return true");
			return true;
		}
		log.debug("return false");
		return false;


	}
	
	
	
	
	/****
	 *   MS2 code using servlets
	 * 
	 * ****/
	
	public void processRequest( Socket requestSocket, ConnectionPreference cp  ) 
			throws SocketTimeoutException, IOException, ServletException
	{
		
		HttpRequestParser parser = new HttpRequestParser();
		
		//parser.setServletMaps(servletMap, servletURLMap, sessionMap );
		parser.setServletMaps(servletMap, servletURLMap);
		
		
		// Try to parse all the information from the HTTP request
		int code = 0;
		
		try{
			log.debug("attempting to extract");
			code = parser.extract(requestSocket);
		}catch (Exception e){
			log.debug("EXTRACTIONFIAILED!");
			errorwriter.writeErrorLog(e.toString());
			code = 400;
			log.debug("400 BAD Request ");
			errorwriter.writeErrorLog("400 BAD request");
			String response = HttpResponseUtils.writeErrorResponse(code, "HTTP/1.1", cp.getConnectionPreference());
			PrintWriter out = new PrintWriter(new OutputStreamWriter(requestSocket.getOutputStream()));
			out.write(response);
			out.flush();
			return;
		}
		
		synchronized(state){
			log.debug("SET STATE!!");
			state = "Working with /" + parser.resource_uri;
			log.debug("state: /" + state);
		}
		
		if(code == 0  ){ // succeeded
			
			log.debug("Parsing Succeeded ");
			
		}else if( code == 400 ){ //bad request
			log.debug("400 BAD Request ");
			errorwriter.writeErrorLog("400 BAD request");
			String response = HttpResponseUtils.writeErrorResponse(code, "HTTP/1.1", cp.getConnectionPreference());
			PrintWriter out = new PrintWriter(new OutputStreamWriter(requestSocket.getOutputStream()));
			out.write(response);
			out.flush();
			cp.setConnectionPreference(false);
			return;
			
		} else if( code == 501){
			log.debug("501 Unsupported Method ");
			errorwriter.writeErrorLog("501 Unsupported Method");
			String response = HttpResponseUtils.writeErrorResponse(code, parser.http_protocol, cp.getConnectionPreference());
			PrintWriter out = new PrintWriter(new OutputStreamWriter(requestSocket.getOutputStream()));
			out.write(response);
			out.flush();
			
			cp.setConnectionPreference(false);
			return;
		} else if( code == -1){ // socket timed out on reading
			
			log.debug("-1 Parser failed on timed out socket ");
			
			errorwriter.writeErrorLog("Socket timedout while parsing");
			//requestSocket.close();
			cp.setConnectionPreference(false);
			return;
		} else if( code == -2){ // IO exception on reading
			
			log.debug("-2 I/O exception ");
			
			errorwriter.writeErrorLog("I/O exception on reading");
			//requestSocket.close();
			cp.setConnectionPreference(false);
			return;
		}
		
		// set the keep alive flag
		if(parser.keepalive == true){
			cp.setConnectionPreference(true);
		}else{
			cp.setConnectionPreference(false);
		}

				
		//log.debug("Request object Contents: " + req.toString());
		
		HttpServlet servlet = parser.m_servlet;
		
		//log.debug(threadMessage(servlet.toString()));
		
		if(servlet != null){
			
			synchronized(state){
				state = "Handling servlet request" + parser.resource_uri;
			}
			
			// Check if a temp session needs to be created.
			
			String requested_session_id = parser.getRequested_session_id();
			myHttpServletSession session = null;
			
			log.debug("searching for requested session id: " + requested_session_id);
			
			//check if the id is of a valid session
			synchronized(sessionMap){
				
				log.debug("session Map contains these keys: ");
				
				for(String sess_id  : sessionMap.keySet() ){
					log.debug("key:" + sess_id);
				}
				
				if ( sessionMap.containsKey(requested_session_id) ){

					log.debug("FOUND a session with the id specified!!!");

					session = sessionMap.get(requested_session_id);

					log.debug("session instance: " + session.toString());

					if(checkifStillValid(session) == false){ // if not valid, request will need to invalidate it.  
						log.debug("Session is now invalid... need to recreate it");

						// invalid session, remove it from list of sessions

						sessionMap.remove(session.getId());

						// clear it and set flags
						try{
							session.invalidate();
						}
						catch(IllegalStateException e){
							e.printStackTrace();
						}
						
						session = null;
						
					}
//					else{
//						
//						// this is our session.
//						
//						// if the isNew flag is still set to true, now set it to false, this is not a newSession.
//						if(session.isNew() == true){
//							
//							//set the flag to false.
//							session = new myHttpServletSession(session, false);
//							
//						}
//						
//						// Update the session's last access time.
//						log.debug("Session is still valid!");
//						Date now = Calendar.getInstance().getTime();
//																				
//						log.debug("got calender instance");
//																				
//						// Property map, wants string format
//						session.setAttribute("last-access", HttpResponseUtils.getDateFormat().format(now.getTime()));
//						
//						sessionMap.put(requested_session_id, session);
//						
//					}
				}
				else{
					log.debug("session id not found in the map...");
				}
			}
			
			
//			if(parser.m_session == null){
//				log.debug("creating temp session");
//				parser.m_session = new myHttpServletSession(servlet.getServletContext());
//				
//				synchronized(sessionMap){
//					if(!sessionMap.containsKey(parser.m_session.getId())){ // only for first time use
//
//						log.debug("Adding Session ref to map!");
//
//						sessionMap.put(parser.m_session.getId(), parser.m_session);
//					}
//				}
//				
//			}else{ // its an existing session
//				
//				log.debug("Client chose to join Session, by passing session ID with Request!");
//				
//				// set the session is new flag to false
//				myHttpServletSession httpSession = new myHttpServletSession(parser.m_session, false);
//				parser.m_session = httpSession;
//			}
			
			
			myHttpServletRequest req = new myHttpServletRequest(parser,servlet, session);
			myHttpServletResponse res = new myHttpServletResponse(requestSocket, parser, servlet);

			
			log.debug("Servlet?: " + servlet.toString());
			
			log.debug("servlet method set to: " + req.getMethod());
			
			try{
				servlet.service(req, res);
			}catch(Exception e){
				
				log.debug("Got an Servlet Exception");
				code = 500;
				String response = HttpResponseUtils.writeErrorResponse(code, "HTTP/1.1", cp.getConnectionPreference());
				PrintWriter out = new PrintWriter(new OutputStreamWriter(requestSocket.getOutputStream()));
				out.write(response);
				out.flush();
				cp.setConnectionPreference(false);
				errorwriter.writeException(e);
				return;
			}
			
			// commit changes to the session made in the servlet.

//			synchronized(sessionMap){
//				if(parser.m_session != null){
//					sessionMap.put(parser.m_session.getId(), parser.m_session);
//				}
//			}
			
			// check if a session was created or invalidated
			if( req.hasSession() == true ){
				
				session = (myHttpServletSession) req.getSession(false);
				
				
				if( session.isValid() == false ) { // check if it got invalidated
					synchronized(sessionMap){
						sessionMap.remove(session.getId());
						log.debug("session was invalidated... removing from session Map");
					}
				}
				else if(session.isNew() == true){ // check if session is new.
					
					//set the flag to false.
					session = new myHttpServletSession(session, false);
					synchronized(sessionMap){
						sessionMap.put(session.getId(), session);
						log.debug("NEW Session is being added to the map!!!!");
					}
					
				} 
				else{ // else update the last-accessed time.

					// Update the session's last access time.
					log.debug("Session is still valid!");
					Date now = Calendar.getInstance().getTime();

					log.debug("got calender instance");

					// Property map, wants string format
					session.setAttribute("last-access", HttpResponseUtils.getDateFormat().format(now.getTime()));
					synchronized(sessionMap){
						sessionMap.put(session.getId(), session);
						log.debug("Updating Session last accessed time");
					}

				}
				
				
			} 
			else{
				// servlet did not deal with session...
			}
			
			
			// flush the buffer if it hasn't already been committed.
//			if(!res.isCommitted()){
//				
//				// there is a session associated with the transaction. we should probably include this in the response headers
//				if(req.hasSession() == true ){
//					
//					String id = req.getSession(false).getId();
//					
//					res.addHeader("Set-Cookie: session-id", id);					
//					
//				}
//				
//				log.debug("We have to flush the buffer");
//				res.flushBuffer();
//			} 
			
			
			// flush the buffer and contents, add session cookie if present
			
			// there is a session associated with the transaction. we should probably include this in the response headers
			if(req.hasSession() == true ){
				
				String id = req.getSession(false).getId();
				
				res.addHeader("Set-Cookie", "session-id="+id);					
				
			}
			
			log.debug("We have to flush the buffer");
			//res.flushBuffer();
			res.flush();
			
			
			
			
			
			/***End Servlet Code here***/
		}else{
			
			/******
			 * 
			 * For Static web pages
			 * 
			 *********************************/
			
			log.debug("servlet was null... Perhaps it is not a servlet");
			
			// check headers for HTTP 1.1 compliance
			boolean httpCompliant = false;
			
			String httpVersion = parser.http_protocol;

			// Quick and dirty adaption previous Get methods variables from new parser class.
			Socket clientSocket = requestSocket;
			HashMap<String,List<String>> headers = parser.m_headers;
			String filename = "/"+parser.resource_uri;
			String reqMethod = parser.m_method; 
			
			
			log.debug("Checking if this is a HTTP 1.1 client");
			
			
			
			if( httpVersion.compareTo("HTTP/1.1") == 0){
				log.debug("This is a HTTP 1.1 client, checking compliance... (It must contain at least Host: header)");
				if(!headers.containsKey("host")){

					log.debug(" 'Host:' header not found! this is not HTTP compliant");
					// Malformed request, send a 400 Bad Request.
					code = 400;
					String mimeType = "text/html";
					String response;
					
					if(reqMethod.toLowerCase().compareTo("head") == 0){
						response = HttpResponseUtils.writeHeadErrorResponse(code, httpVersion, false);
					}else {
						response = HttpResponseUtils.writeErrorResponse(code, httpVersion, false);
					}
					
					PrintWriter out;
					try {
						out = new PrintWriter(new OutputStreamWriter(clientSocket.getOutputStream()));

						out.write(response);
						out.flush();

						//out.close();
						return;
					} 
					catch (SocketTimeoutException timeout ){
						try {
							errorwriter.writeErrorLog(timeout.toString());
							clientSocket.close();
							cp.setConnectionPreference(false);
							return;
						} catch (IOException e1) {
							// TODO Auto-generated catch block
							errorwriter.writeErrorLog(e1.toString());
							e1.printStackTrace();
						}
					}
					catch (IOException e) {
						// TODO Auto-generated catch block
						errorwriter.writeErrorLog(e.toString());
						e.printStackTrace();
					}				

				}
				else{
					log.debug(" 'Host:' header found ==> HTTP compliant");
					httpCompliant = true;
				}
			}else{ // HTTP 1.0 doesn't require Headers
				log.debug(" HTTP/1.0 No 'Host:' header required ==> HTTP 1.0 compliant");
				httpCompliant = true;
			}


			if(httpCompliant == true){
				try {
					// Write response (headers and body) for single response
					//if single file... (assuming we validate path and figure out if it exists prior to this...)

					File f;
					boolean isDirectory = false;
					boolean isControl = false;
					boolean expectContinue = false;

					String mimeType = "text/html";


					// check for the 100 Continue header
					if(headers.containsKey("expect") ){

						log.debug("checking expect: header value");
						for(String expects : headers.get("expect")){
							if(expects.toLowerCase().compareTo("100-continue") == 0 
									&& httpVersion.compareTo("HTTP/1.0") != 0){
								expectContinue = true;
							}
						}

					}


					// check for the 'Connection: ' header value

					if(headers.containsKey("connection")){

						log.debug("checking the value for Connection: header value");

						for(String connectionAlive : headers.get("connection")){
							if(connectionAlive.toLowerCase().compareTo("close") == 0){
								log.debug("Connection: header is close");
								cp.setConnectionPreference(false);
							}
						}

					}
					else{ // no connection header value defaults to single request then connection close
						log.debug("No Connection: header  DEFAULTING to close");
						cp.setConnectionPreference(false);
					}

					log.debug("Connection preference currently set to: " + ( cp.getConnectionPreference() == true ? "keep-alive" : "close" ));



					log.debug(threadMessage("filename requested: " + filename));

	
					
					f = new File(root+"/"+filename);


					if(filename.compareTo("/") == 0){
						//filename = "index.html";
						String explicitPath = root;  
						log.debug(threadMessage("Requested " + explicitPath));
						isDirectory = true;
						

					}

					//special control URLs
					else if ( filename.compareTo("/shutdown") == 0 ){
						// in case server is propogating shutdown and we are changing it too, doesn't matter
						//   b/c it is idempotent.

						personalShutdownFlag = true; // volatile boolean

						log.debug(threadMessage("Requested /SHUTDOWN"));

					}
					else if ( filename.compareTo("/control") == 0 ){
						log.debug(threadMessage("Requested /CONTROL"));
						isControl = true;

					}
					// its a directory
					else if ( f.isDirectory() == true ){
						// generate the list of files in the directory
						log.debug(threadMessage("Requested a directory!"));

						isDirectory = true;
					}
					// its a single file.
					else {
						log.debug(threadMessage("Requested a file!"));

					}

					code = 0;
					String body = "";
					String response = "";

					if( personalShutdownFlag == true ){ // we got a shutdown requested

						code = 200;
						mimeType = "text/html";
						if(reqMethod.toLowerCase().compareTo("head") == 0){
							response = HttpResponseUtils.writeResponseHeaders(code, httpVersion, false);
						}else{
							response = HttpResponseUtils.writeResponseHeaders(code, httpVersion, false);
						}
						PrintWriter out = new PrintWriter(new OutputStreamWriter(clientSocket.getOutputStream()));
						//Check for 100 Continue flag
						if( expectContinue == true ){
							String continueResponse = HttpResponseUtils.getContinueResponse();
							out.write(continueResponse);
						}
						out.write(response);
						out.flush();
						
						cp.setConnectionPreference(false);
						return;
						//out.close();

					}
					else if(isControl){  // Return the control page
						String controlPage = getControlPageText();

						File controlFile = new File("resources/controlpage.html");

						code = 200;
						mimeType = "text/html";
						if(reqMethod.toLowerCase().compareTo("head") == 0){
							response = HttpResponseUtils.writeHeadResponseHeaders(code, mimeType, controlPage, httpVersion, controlFile.lastModified(), cp.getConnectionPreference());
						}else{
							response = HttpResponseUtils.writeResponseHeaders(code, mimeType, controlPage, httpVersion, controlFile.lastModified(), cp.getConnectionPreference());
						}
						PrintWriter out = new PrintWriter(new OutputStreamWriter(clientSocket.getOutputStream()));
						//Check for 100 Continue flag
						if( expectContinue == true ){
							String continueResponse = HttpResponseUtils.getContinueResponse();
							out.write(continueResponse);
						}
						out.write(response);
						out.flush();
						
						return;
						//out.close();

					}

					else if(!f.exists() ){ // check if file exists

						// respond with 404 file not found
						code = 404;
						errorwriter.writeErrorLog(threadMessage("ERROR - File does not exist!"));
						log.debug(threadMessage("ERROR - File does not exist!"));
						if(reqMethod.toLowerCase().compareTo("head") == 0){
							response = HttpResponseUtils.writeHeadErrorResponse(code, httpVersion, cp.getConnectionPreference());
						}else{
							response = HttpResponseUtils.writeErrorResponse(code, httpVersion, cp.getConnectionPreference());
						}
						PrintWriter out = new PrintWriter(new OutputStreamWriter(clientSocket.getOutputStream()));
						out.write(response);
						out.flush();
						
						return;
						//out.close();

					}

					else{ // file or directory exists

						if(!f.canRead()){ //check if file is readable

							// respond with 403 file permission denied
							code = 403;
							log.debug(threadMessage("ERROR - File access denied!"));
							errorwriter.writeErrorLog(threadMessage("ERROR - File access denied!"));
							if(reqMethod.toLowerCase().compareTo("head") == 0){
								response = HttpResponseUtils.writeHeadErrorResponse(code, httpVersion, cp.getConnectionPreference());
							}else{
								response = HttpResponseUtils.writeErrorResponse(code, httpVersion, cp.getConnectionPreference());
							}
							
							PrintWriter out = new PrintWriter(new OutputStreamWriter(clientSocket.getOutputStream()));
							out.write(response);
							out.flush();
							
							return;
							//out.close();
						}
						else { // file is readable

							log.debug(threadMessage("File or directory exists!"));

							// validate the path...

							Path pathString =  Paths.get(root+"/"+filename).toRealPath();

							log.debug("Validating path:   "+ pathString.toString());

							boolean validPath = validatePath(pathString);

							if( validPath == false ){

								code = 403;
								log.debug(threadMessage("ERROR - File access forbidden!"));
								errorwriter.writeErrorLog(threadMessage("ERROR - File access forbidden!"));
								if(reqMethod.toLowerCase().compareTo("head") == 0){
									response = HttpResponseUtils.writeHeadErrorResponse(code, httpVersion, cp.getConnectionPreference());
								}else{
									response = HttpResponseUtils.writeErrorResponse(code, httpVersion, cp.getConnectionPreference());
								}
								
								PrintWriter out = new PrintWriter(new OutputStreamWriter(clientSocket.getOutputStream()));
								out.write(response);
								out.flush();
								
								return;
								//out.close();


							}
							else { // it is a valid path

								// check if the request provided the modified/unmodified-Since flag
								boolean modifiedSinceHeader = false;
								boolean unmodifiedSinceHeader = false;

								boolean preconditionMet = true;

								if(headers.containsKey("if-modified-since") && headers.containsKey("if-unmodified-since")){

									//error.... It does not make sense to have both headers.

									log.debug(" 'Malformed Request, contains contradictory modified since headers!");
									errorwriter.writeErrorLog(threadMessage(" 'Malformed Request, contains contradictory modified since headers!"));
									// Malformed request, send a 400 Bad Request.
									code = 400;
									mimeType = "text/html";
									if(reqMethod.toLowerCase().compareTo("head") == 0){
										response = HttpResponseUtils.writeHeadErrorResponse(code, httpVersion, cp.getConnectionPreference());
									}else{
										response = HttpResponseUtils.writeErrorResponse(code, httpVersion, cp.getConnectionPreference());
									}
									PrintWriter out;
									try {
										out = new PrintWriter(new OutputStreamWriter(clientSocket.getOutputStream()));

										out.write(response);
										out.flush();
										
										return;
										//out.close();
									} catch (SocketTimeoutException timeout ){
										try {
											errorwriter.writeErrorLog(threadMessage(timeout.toString()));
											clientSocket.close();
											cp.setConnectionPreference(false);
											return;
										} catch (IOException e1) {
											// TODO Auto-generated catch block
											errorwriter.writeErrorLog(threadMessage(e1.toString()));
											e1.printStackTrace();
										}
									}catch (IOException e) {
										// TODO Auto-generated catch block
										errorwriter.writeErrorLog(threadMessage(e.toString()));
										e.printStackTrace();
									}				

								}
								else if(headers.containsKey("if-modified-since")){
									modifiedSinceHeader = true;	
								}
								else if(headers.containsKey("if-unmodified-since")){
									unmodifiedSinceHeader = true;
								}

								Date lastModified = new Date(f.lastModified());

								if(modifiedSinceHeader ){

									log.debug(threadMessage("checking if the file was modified since certain date"));

									Date headerModified = HttpResponseUtils.parseHeaderDate(headers.get("if-modified-since").get(0));

									if(headerModified == null ){ // ignore the malformed header
										preconditionMet = true;
									}
									else if( lastModified.before( headerModified ) || lastModified.equals( headerModified )){
										if(headerModified != null){
											log.debug(" File had not been modified since: "+ headerModified.toString());
											errorwriter.writeErrorLog(threadMessage(" File had not been modified since: "+ headerModified.toString()));
										}
										// File not modified, send a 304 Bad Request.
										code = 304;
										mimeType = "text/html";
										if(reqMethod.toLowerCase().compareTo("head") == 0){
											response = HttpResponseUtils.writeHeadErrorResponse(code, httpVersion, cp.getConnectionPreference());

										}else{
											response = HttpResponseUtils.writeErrorResponse(code, httpVersion, cp.getConnectionPreference());
										}
										PrintWriter out;
										try {
											out = new PrintWriter(new OutputStreamWriter(clientSocket.getOutputStream()));

											out.write(response);
											out.flush();
											return;
											//out.close();
										} catch (SocketTimeoutException timeout ){
											try {
												errorwriter.writeErrorLog(threadMessage(timeout.toString()));
												clientSocket.close();
												cp.setConnectionPreference(false);
												return;
											} catch (IOException e1) {
												// TODO Auto-generated catch block
												errorwriter.writeErrorLog(threadMessage(e1.toString()));
												e1.printStackTrace();
											}
										}catch (IOException e) {
											// TODO Auto-generated catch block
											errorwriter.writeErrorLog(threadMessage(e.toString()));
											e.printStackTrace();
										}		

										preconditionMet = false;

									}

								}
								else if (unmodifiedSinceHeader ){

									log.debug(threadMessage("checking if the file was modified since certain date"));

									Date headerModified = HttpResponseUtils.parseHeaderDate(headers.get("if-unmodified-since").get(0));

									if(headerModified == null){ // ignore the malformed data
										log.debug(threadMessage("Error Parsing date, ignoring the header"));
										errorwriter.writeErrorLog(threadMessage("Error Parsing date, ignoring the header"));
										preconditionMet = true;
									}

									else if( lastModified.after( headerModified )){

										log.debug(" File had not been unmodified since: "+ headerModified.toString());
										errorwriter.writeErrorLog(threadMessage(" File had not been unmodified since: "+ headerModified.toString()));
										// Malformed request, send a 400 Bad Request.
										code = 412;
										mimeType = "text/html";
										if(reqMethod.toLowerCase().compareTo("head") == 0){
											response = HttpResponseUtils.writeHeadErrorResponse(code, httpVersion, cp.getConnectionPreference());
										}else{
											response = HttpResponseUtils.writeErrorResponse(code, httpVersion, cp.getConnectionPreference());
										}
										PrintWriter out;
										try {
											out = new PrintWriter(new OutputStreamWriter(clientSocket.getOutputStream()));

											out.write(response);
											out.flush();
											return;
											//out.close();
										} catch (SocketTimeoutException timeout ){
											try {
												errorwriter.writeErrorLog(threadMessage(timeout.toString()));
												clientSocket.close();
												cp.setConnectionPreference(false);
												return;
											} catch (IOException e1) {
												// TODO Auto-generated catch block
												errorwriter.writeErrorLog(threadMessage(e1.toString()));
												e1.printStackTrace();
											}
										}catch (IOException e) {
											// TODO Auto-generated catch block
											errorwriter.writeErrorLog(threadMessage(e.toString()));
											e.printStackTrace();
										}		

										preconditionMet = false;

									}
								}

								// either the If-Unmodified/Modified-Since headers were not included or the precondition was met
								//    either one.
								if(preconditionMet == true){ 

									if(isDirectory){
										log.debug(threadMessage("Got a directory request"));
										StringBuffer directoryContents = new StringBuffer();
										StringBuffer fileContents = new StringBuffer();

										// generate html page of the files in directory....

										File directoryPage = new File("resources/directory.html");

										BufferedReader directoryReader = new BufferedReader(new FileReader(directoryPage));

										String line;

										while((line = directoryReader.readLine()) != null){
											directoryContents.append(line);
										}


										File[] listofFiles = f.listFiles();

										for(File file : listofFiles){

											if (file.isFile()) {
												fileContents.append("<p> - " + file.getName()+"</p>");
											} else if (file.isDirectory()) {
												fileContents.append("<p style=\"font-weight:bold\"> - " + file.getName() +"/</p>");
											}

										}

										String directory = directoryContents.toString();
										directory = directory.replace("<!-- Include files HERE -->", fileContents.toString());
										directory = directory.replace("<!-- Directory Name -->", f.getName()+"/");

										// set the code 
										code = 200;
										body = directory.toString();
										mimeType = "text/html";
										if(reqMethod.toLowerCase().compareTo("head") == 0){
											response = HttpResponseUtils.writeHeadResponseHeaders(code, mimeType, body, httpVersion, f.lastModified(), cp.getConnectionPreference());

										}else{
											response = HttpResponseUtils.writeResponseHeaders(code, mimeType, body, httpVersion, f.lastModified(), cp.getConnectionPreference());

										}
										
										PrintWriter out = new PrintWriter(new OutputStreamWriter(clientSocket.getOutputStream()));

										//Check for 100 Continue flag
										if( expectContinue == true ){
											String continueResponse = HttpResponseUtils.getContinueResponse();
											out.write(continueResponse);
										}


										out.write(response);
										out.flush();
										
										//out.close();

									}

									else{
										mimeType = getMimeType(filename);

										if(mimeType == null && f.isFile() == false ){
											//unsupported file type 415
											errorwriter.writeErrorLog(threadMessage("Unsupported MIME type!"));
											code = 415;

											// generate the html here...
											if(reqMethod.toLowerCase().compareTo("head") == 0){
												response = HttpResponseUtils.writeHeadErrorResponse(code, httpVersion, cp.getConnectionPreference());
											}else{
												response = HttpResponseUtils.writeErrorResponse(code, httpVersion, cp.getConnectionPreference());
											}
											PrintWriter out = new PrintWriter(new OutputStreamWriter(clientSocket.getOutputStream()));
											out.write(response);
											out.flush();
											return;
											//out.close();

										}
										else if(mimeType == null && f.isFile() == true ){ // Unsupported Media Type
											log.debug(threadMessage("file type unknown: "));
											
											errorwriter.writeErrorLog(threadMessage("Unknown file MIME type!"));

											code = 200;
											mimeType = "application/octet-stream";

											Path path = FileSystems.getDefault().getPath(root, filename);
											BasicFileAttributes attrs = Files.readAttributes(path , BasicFileAttributes.class);
											// re-use image byte stream headers to send data 
											PrintWriter outHeaders = new PrintWriter(new OutputStreamWriter(clientSocket.getOutputStream()));
											if(reqMethod.toLowerCase().compareTo("head") == 0){
												response = HttpResponseUtils.writeHeadImageResponseHeaders(code, mimeType, httpVersion, attrs.size(), f.lastModified(), cp.getConnectionPreference() );

												//Check for 100 Continue flag
												if( expectContinue == true ){
													String continueResponse = HttpResponseUtils.getContinueResponse();
													outHeaders.write(continueResponse);
												}

												outHeaders.write(response);
												outHeaders.flush();
												return;
											}else{
												
												response = HttpResponseUtils.writeImageResponseHeaders(code, mimeType, httpVersion, attrs.size(), f.lastModified(), cp.getConnectionPreference() );

												//Check for 100 Continue flag
												if( expectContinue == true ){
													String continueResponse = HttpResponseUtils.getContinueResponse();
													outHeaders.write(continueResponse);
												}

												outHeaders.write(response);
												outHeaders.flush();

												FileInputStream fileInputStm = new FileInputStream(f);
												DataOutputStream  dataOutputStm = new DataOutputStream(clientSocket.getOutputStream());

												try {
													sendBinaryData(fileInputStm, dataOutputStm);
												} catch (SocketTimeoutException timeout ){
													try {
														errorwriter.writeErrorLog(threadMessage(timeout.toString()));
														clientSocket.close();
														cp.setConnectionPreference(false);
														return;
													} catch (IOException e1) {
														// TODO Auto-generated catch block
														errorwriter.writeErrorLog(threadMessage(e1.toString()));
														e1.printStackTrace();
													}
												}catch (Exception e) {
													// TODO Auto-generated catch block
													e.printStackTrace();
													errorwriter.writeErrorLog(threadMessage(e.toString()));
													synchronized(state){
														state = "ERROR in sending Binary Data";
													}
												}
												
												return;
											}

											//fileInputStm.close();
											//dataOutputStm.close();
											//outHeaders.close();

										}
										else if ( mimeType.substring(0, mimeType.indexOf("/")).compareTo("image") == 0 ){ // its an image
											log.debug(threadMessage("Got image file type: "+mimeType));

											//set the code
											code = 200;

											Path path = FileSystems.getDefault().getPath(root, filename);
											BasicFileAttributes attrs = Files.readAttributes(path , BasicFileAttributes.class);
											
											PrintWriter outHeaders = new PrintWriter(new OutputStreamWriter(clientSocket.getOutputStream()));

											
											if(reqMethod.toLowerCase().compareTo("head") == 0){
												response = HttpResponseUtils.writeHeadImageResponseHeaders(code, mimeType, httpVersion, attrs.size(), f.lastModified(), cp.getConnectionPreference() );


												//Check for 100 Continue flag
												if( expectContinue == true ){
													String continueResponse = HttpResponseUtils.getContinueResponse();
													outHeaders.write(continueResponse);
												}
												outHeaders.write(response);
												outHeaders.flush();

											}else{
												
												response = HttpResponseUtils.writeImageResponseHeaders(code, mimeType, httpVersion, attrs.size(), f.lastModified(), cp.getConnectionPreference() );

												outHeaders.write(response);
												outHeaders.flush();

												FileInputStream fileInputStm = new FileInputStream(f);
												DataOutputStream  dataOutputStm = new DataOutputStream(clientSocket.getOutputStream());

												try {
													sendBinaryData(fileInputStm, dataOutputStm);
												} catch (SocketTimeoutException timeout ){
													try {
														errorwriter.writeErrorLog(threadMessage(timeout.toString()));
														clientSocket.close();
														cp.setConnectionPreference(false);
														return;
													} catch (IOException e1) {
														// TODO Auto-generated catch block
														errorwriter.writeErrorLog(threadMessage(e1.toString()));
														e1.printStackTrace();
													}
												}catch (Exception e) {
													// TODO Auto-generated catch block
													errorwriter.writeErrorLog(threadMessage(e.toString()));
													e.printStackTrace();
													synchronized(state){
														state = "ERROR in sending Binary Data";
													}
												}


												//											fileInputStm.close();
												//											dataOutputStm.close();
												//											outHeaders.close();
											}
										}else { // its text file

											log.debug(threadMessage("Got text file type: "+mimeType));

											BufferedReader filereader = new BufferedReader(new FileReader(f));

											String ls = System.getProperty("line.separator");
											StringBuffer contents = new StringBuffer();
											String line;
											
											while( (line = filereader.readLine()) != null ){
												contents.append(line);
												contents.append(ls);
											}
											
											log.debug("length of contents: " + contents.length());
											log.debug("file length : " + f.length());
											log.debug(threadMessage("file contents: " + contents.toString()));

											//set the code
											code = 200;
											body = contents.toString();
											if(reqMethod.toLowerCase().compareTo("head") == 0){
												response = HttpResponseUtils.writeHeadResponseHeaders(code, mimeType, body, httpVersion, f.lastModified(), cp.getConnectionPreference());
											}else{
												response = HttpResponseUtils.writeResponseHeaders(code, mimeType, body, httpVersion, f.lastModified(), cp.getConnectionPreference());
											}
											
											PrintWriter out = new PrintWriter(new OutputStreamWriter(clientSocket.getOutputStream()));

											//Check for 100 Continue flag
											if( expectContinue == true ){
												String continueResponse = HttpResponseUtils.getContinueResponse();
												out.write(continueResponse);
											}

											out.write(response);
											out.flush();

											//out.close();
										}
									}

								}
							}
						}
					}
					//clientSocket.close();

				} catch (SocketTimeoutException timeout ){
					try {
						errorwriter.writeErrorLog(threadMessage(timeout.toString()));
						clientSocket.close();
						cp.setConnectionPreference(false);
						return;
					} catch (IOException e1) {
						// TODO Auto-generated catch block
						errorwriter.writeErrorLog(threadMessage(e1.toString()));
						e1.printStackTrace();
					}
				}catch (IOException e) {
					// TODO Auto-generated catch block
					errorwriter.writeErrorLog(threadMessage(e.toString()));
					e.printStackTrace();
					synchronized(state){
						state = "ERROR in retreiving File";
					}
				}
			}			
		}
		
	}
	
	
	@Override
	public void run(){
		
		ConnectionPreference cp = new ConnectionPreference();
		
		
		log.debug(threadMessage("grabbing lock..."));
		while (true){
			
			cp.setConnectionPreference(true);
			//reset socket each time
			requestSocket = null;
			
			// if we get a shutdown signal, we update the shared shutdown flag so that
			//  server can see it and propagate it, then we exit.
			shutdownLock.lock();
			if(personalShutdownFlag == true){
				shutdownLock.unlock();
				shutdownCtrl.shutdown_requested = true;
				try {
					reqQ.setShutdown();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					errorwriter.writeErrorLog(threadMessage(e.toString()));
					e.printStackTrace();
				}
				break;
			}
			shutdownLock.unlock(); // unlock if we don't go into if statement.
			
			log.debug(threadMessage("beginning of loop..."));
			
			log.debug(threadMessage("attempting to dequeue..."));
			synchronized(state){
				state = "WAITING";
			}
			try {
				requestSocket = reqQ.dequeue();
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				errorwriter.writeErrorLog(threadMessage(e1.toString()));
				e1.printStackTrace();
			}
			
						
			// got a socket, lets handle the request.
			if(requestSocket != null){
				
				
				while (cp.getConnectionPreference() == true){
//					synchronized(state){
//						state = "WORKING";
//					}

					log.debug(threadMessage("Got a request Socket!"));

					// do work.


					try {
						
						processRequest(requestSocket, cp);

					} catch (SocketTimeoutException timeout ){
						try {
							errorwriter.writeErrorLog(threadMessage(timeout.toString()));
							requestSocket.close();
							cp.setConnectionPreference(false);
						} catch (IOException e1) {
							// TODO Auto-generated catch block
							errorwriter.writeErrorLog(threadMessage(e1.toString()));
							e1.printStackTrace();
						}
					}catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
						try {
							requestSocket.close();
							cp.setConnectionPreference(false);
						} catch (IOException e1) {
							// TODO Auto-generated catch block
							errorwriter.writeErrorLog(threadMessage(e1.toString()));
							e1.printStackTrace();
						}
					} catch (ServletException e) {
						// TODO Auto-generated catch block
						errorwriter.writeErrorLog(threadMessage(e.toString()));
						e.printStackTrace();
					}
				}
				
				try {
					requestSocket.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					errorwriter.writeErrorLog(threadMessage(e.toString()));
					e.printStackTrace();
				}
				
			}
			
			// after doing some work, close the socket, assume that for persistent connections, 
			//   we handle in the above while loop
			

		}
		
		log.debug(threadMessage("Shutting Down..."));

	}




}
