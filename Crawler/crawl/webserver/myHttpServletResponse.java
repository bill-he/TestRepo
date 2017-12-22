package edu.upenn.cis.cis455.webserver;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.net.Socket;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.IllegalFormatException;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import javax.servlet.ServletResponse;

import edu.upenn.cis.cis455.webserver.utils.HttpRequestParser;
import edu.upenn.cis.cis455.webserver.utils.HttpResponseUtils;

public class myHttpServletResponse implements HttpServletResponse {

	Logger log = Logger.getLogger(myHttpServletResponse.class);
	
	private Socket requestSocket = null;
	
	private String character_encoding = null;
	private String content_type = null;
	private HashMap<String, List<String>>m_headers = new HashMap<String,List<String>>(); 
	private int status_code = 200; // default 200
	private int content_length = 0;
	private String http_protocol = null;
	
	
	private int numbytes = 0;
	private StringBuffer strBuffer = new StringBuffer();
	private Byte[] bytebuffer = null;
	private int buffer_limit = 0; // unbounded by default
	private PrintWriter outWriter = null;
	private OutputStreamWriter streamWriter = null;
	private Locale locale = Locale.US; // default
	private Long lastMod = null;
	private ByteArrayOutputStream buffer =  new ByteArrayOutputStream();
//	private myHttpServletSession servlet_session = null;
	private HttpServlet servlet = null;
	private HttpRequestParser requestparser = null;
	
	
	private boolean keep_alive = false;
	private boolean committed = false;
	private boolean bufferSizeSet = false;
	private boolean fixedBufferedOption = false;
	private boolean writerActive = false;
	private boolean charEncodingSet = false;
	private boolean contentTypeSet = false;
	
	
	
	//default constructor
//	public myHttpServletResponse(Socket s, HttpRequestParser parser, myHttpServletSession session, HttpServlet http_servlet){
	public myHttpServletResponse(Socket s, HttpRequestParser parser, HttpServlet http_servlet){
		requestSocket = s;
		http_protocol = parser.http_protocol;
		keep_alive = parser.keepalive;
		requestparser = parser;
//		servlet_session = session;
		servlet = http_servlet;
	}

		
	//safe to assume we already have an outwriter?
	@Override
	//public void flushBuffer() throws IOException {
	public void flushBuffer(){
		// TODO Auto-generated method stub
		

		if(!isCommitted()){
		
						
//			StringBuffer response = new StringBuffer();
//			
//			// flush all bytes in stream to bytearray to be used as body
//			if(outWriter != null){
//				outWriter.flush();
//			}
//			
//			//Status line
//			response.append(http_protocol).append(" ").append(Integer.toString(status_code)).append(" ").append(HttpResponseUtils.getResponseText(status_code)).append("\n");
//			
//			
//			if(content_type != null ){
//				if(charEncodingSet == true){
//					log.debug("have a content-type with a character encoding!");
//					response.append("Content-Type: ").append(content_type).append("; ").append("charset=").append(character_encoding).append("\n");
//				}else{
//					log.debug("have a content-type without a character encoding!");
//					response.append("Content-Type: ").append(content_type).append("\n");
//				}
//			}else{
//				
//				log.debug("default set content-type to text/html");
//				//default to text/html
//				response.append("Content-Type: ").append("text/html").append("; ").append("charset=").append("UTF-8").append("\n");
//			}
//			
//			boolean lenSet = false;
//			for(String name : m_headers.keySet() ){
//				if(name.toLowerCase().compareTo("content-length") == 0){
//					lenSet = true;
//				}
//			}
//			if(lenSet == false){ // determine size automatically
//				
//				if(fixedBufferedOption == true){
//					response.append("Content-Length: ").append(buffer_limit).append("\n");
//				}else{
//					response.append("Content-Length: ").append(buffer.size()).append("\n");
//				}
//				
//			}
//			else{
//				response.append("Content-Length: ").append(content_length).append("\n");
//			}
//			
//			// Cookies
////			if( m_headers.containsKey("session-id")  ){
////				
////				response.append("Set-Cookie: ").append("session-id=").append(m_headers.get("session_id")).append("\n");
////				
////			}else{
////				
////				log.debug("SESSION is NULL!");
////				
////			}
//
//			// no caching because its dynamically generated servlet code.
//			response.append("Cache-Control: no-cache\n");
//			response.append("Pragma: no-cache\n");
//			
//			// add all the other headers
//			for(String name : m_headers.keySet()   ){
//				
//				if( name.compareTo("Content-Type") != 0 && name.compareTo("Content-Length") != 0 ){
//					
//					log.debug("adding OTHER header: " + name);
//					
//					if(m_headers.get(name).size() > 1){
//						
//						//add cookie content
//						if(name.compareTo("Set-Cookie") == 0){
//							for(String c_str : m_headers.get(name)){
//								response.append(name).append(": ").append(c_str).append("\n");
//							}
//							
//						}else{ // headers with multi-value
//							int size = 0;
//							size = m_headers.get(name).size();
//							response.append(name).append(": ").append(m_headers.get(name).get(0));
//							for(int i = 1; i < size; i++ ){
//								response.append("; ").append(name).append(": ").append(m_headers.get(name).get(i));
//							}
//						}
//						
//					}else{
//						response.append(name).append(": ").append(m_headers.get(name).get(0)).append("\n");
//					}
//					
//				}
//				
//			}
//			
//			
//			response.append("\n");
//			
//			log.debug("byte array size for content: " + buffer.size());
//			
//			//truncate body per the buffer size
//			if( buffer_limit == 0 ){ // unlimited
//				response.append(buffer.toString());
//			}else{// limited buffer size
//				if( buffer.size() < buffer_limit ){
//					response.append(buffer.toString()).append("\n");
//				}else{
//					String truncated = buffer.toString().substring(0, buffer_limit+1);
//					response.append(truncated).append("\n");
//				}
//			}
//			
//
//			
//			log.debug(response.toString());
//			
//			PrintWriter headersPrintWriter = new PrintWriter(requestSocket.getOutputStream(), true);
//			
//			headersPrintWriter.println(response.toString());
//			
//			log.debug("flushed print writer");
//			
//			//headersPrintWriter.close();
			
			committed = true;
		}else{
			throw new IllegalStateException();
		}
		
	}
	
	public void flush() throws IOException{
		
		StringBuffer response = new StringBuffer();
		
		// flush all bytes in stream to bytearray to be used as body
		if(outWriter != null){
			outWriter.flush();
		}
		
		//Status line
		response.append(http_protocol).append(" ").append(Integer.toString(status_code)).append(" ").append(HttpResponseUtils.getResponseText(status_code)).append("\n");
		
		
		if(content_type != null ){
			if(charEncodingSet == true){
				log.debug("have a content-type with a character encoding!");
				response.append("Content-Type: ").append(content_type).append("; ").append("charset=").append(character_encoding).append("\n");
			}else{
				log.debug("have a content-type without a character encoding!");
				response.append("Content-Type: ").append(content_type).append("\n");
			}
		}else{
			
			log.debug("default set content-type to text/html");
			//default to text/html
			response.append("Content-Type: ").append("text/html").append("; ").append("charset=").append("UTF-8").append("\n");
		}
		
		boolean lenSet = false;
		for(String name : m_headers.keySet() ){
			if(name.toLowerCase().compareTo("content-length") == 0){
				lenSet = true;
			}
		}
		if(lenSet == false){ // determine size automatically
			
			if(fixedBufferedOption == true){
				response.append("Content-Length: ").append(buffer_limit).append("\n");
			}else{
				response.append("Content-Length: ").append(buffer.size()).append("\n");
			}
			
		}
		else{
			response.append("Content-Length: ").append(content_length).append("\n");
		}
		
		// Cookies
//		if( m_headers.containsKey("session-id")  ){
//			
//			response.append("Set-Cookie: ").append("session-id=").append(m_headers.get("session_id")).append("\n");
//			
//		}else{
//			
//			log.debug("SESSION is NULL!");
//			
//		}

		// no caching because its dynamically generated servlet code.
		response.append("Cache-Control: no-cache\n");
		response.append("Pragma: no-cache\n");
		
		// add all the other headers
		for(String name : m_headers.keySet()   ){
			
			if( name.compareTo("Content-Type") != 0 && name.compareTo("Content-Length") != 0 ){
				
				log.debug("adding OTHER header: " + name);
				
				if(m_headers.get(name).size() > 1){
					
					//add cookie content
					if(name.compareTo("Set-Cookie") == 0){
						for(String c_str : m_headers.get(name)){
							response.append(name).append(": ").append(c_str).append("\n");
						}
						
					}else{ // headers with multi-value
						int size = 0;
						size = m_headers.get(name).size();
						response.append(name).append(": ").append(m_headers.get(name).get(0));
						for(int i = 1; i < size; i++ ){
							response.append("; ").append(name).append(": ").append(m_headers.get(name).get(i));
						}
					}
					
				}else{
					response.append(name).append(": ").append(m_headers.get(name).get(0)).append("\n");
				}
				
			}
			
		}
		
		
		response.append("\n");
		
		log.debug("byte array size for content: " + buffer.size());
		
		//truncate body per the buffer size
		if( buffer_limit == 0 ){ // unlimited
			response.append(buffer.toString());
		}else{// limited buffer size
			if( buffer.size() < buffer_limit ){
				response.append(buffer.toString()).append("\n");
			}else{
				String truncated = buffer.toString().substring(0, buffer_limit+1);
				response.append(truncated).append("\n");
			}
		}
		

		
		log.debug(response.toString());
		
		PrintWriter headersPrintWriter = new PrintWriter(requestSocket.getOutputStream(), true);
		
		headersPrintWriter.println(response.toString());
		
		log.debug("flushed print writer");
		
		// dont close, it will close the 
		//headersPrintWriter.close();
		
	}
	

	@Override
	public int getBufferSize() {
		
		return buffer_limit;
		
	}

	@Override
	public String getCharacterEncoding() {
		if(character_encoding != null){
		  return character_encoding;
		}
		return "ISO-8859-1";
	}

	@Override
	public String getContentType() {
		
		if( content_type != null ){
			return content_type;
		}
		
		return "text/html";
	}

	@Override
	public Locale getLocale() {
		return locale;
	}

	@Override
	public ServletOutputStream getOutputStream() throws IOException {
		return null;
	}

	@Override
	public PrintWriter getWriter() throws IOException {
		if(isCommitted() == false ){
			
			if(this.outWriter == null || writerActive == false){
				
				// flag to indicate that from now on the buffer size cannot be changed
				bufferSizeSet = true;
				
				
				if(buffer_limit > 0){ // user specified buffer size
					fixedBufferedOption = true;
					buffer = new ByteArrayOutputStream(buffer_limit);
					
				}else{ // unlimited buffer size
					
					buffer = new ByteArrayOutputStream();
				}
				streamWriter = new OutputStreamWriter(buffer, getCharacterEncoding());
				//myServletResponseWriter p =  new myServletResponseWriter(this.streamWriter);
				PrintWriter p =  new PrintWriter(this.streamWriter);
				outWriter = p;
				
				writerActive = true;
				
				return p;
			}
			else{
				return this.outWriter;
			}
			
		}else{
			throw new IllegalStateException();
		}
	}

	@Override
	public boolean isCommitted() {
		
		return committed;
	}

	@Override
	public void reset() {
		
		if(isCommitted() == false){
			
			resetBuffer();
			m_headers.clear();
			status_code = 200;
			
			//reset committed flag
			committed = false;
		}else{
			
			throw new IllegalStateException();
			
		}
		
	}

	@Override
	public void resetBuffer() {
		
		if(isCommitted() == false){
			
			buffer.reset();
			
		}else{
			
			throw new IllegalStateException();
			
		}
		
	}

	
	@Override
	public void setBufferSize(int size) {
		
		if(isCommitted()){
			throw new IllegalStateException();
		}
		else if(buffer.size() > 0){ // content was written prior to this call
			throw new IllegalStateException();
		}
		else if(bufferSizeSet == true){
			throw new IllegalStateException();
		}
		else{
			log.debug("Buffer size was set!");
			fixedBufferedOption = true;
			bufferSizeSet = true;
			buffer_limit = size;
		}
		
	}

	@Override
	public void setCharacterEncoding(String encoding) {
		if(isCommitted()){
			throw new IllegalStateException();
		}else{
			character_encoding = encoding;
			charEncodingSet = true;
		}

	}

	@Override
	public void setContentLength(int len) {
		if(isCommitted()){
			throw new IllegalStateException();
		}else{
			List<String> list = new ArrayList<String>();
			list.add(String.valueOf(len));
			m_headers.put("Content-Length", list);
			content_length = len;
		}
	}

	@Override
	public void setContentType(String type) {
		if(isCommitted()){
			throw new IllegalStateException();
		}else{
			List<String> list = new ArrayList<String>();
			list.add(type);
			m_headers.put("Content-Type", list);
			content_type = type;
			contentTypeSet = true;
		}
	}

	@Override
	public void setLocale(Locale l) {
		if(isCommitted()){
			// does nothing
		}
		else if ( contentTypeSet == true){
			// do nothing, cant change the locale is contentType is set
		}
		else if ( charEncodingSet == true  ){
			// do nothing, cant change the locale if charEncoding is set.
		}
        else if (writerActive == true ){
        	// do nothing, cant change the encoding that current print writer is using.
        }
		else{
			locale = l;
		}
	}

	@Override
	public void addCookie(Cookie c) {
		
		if(c != null){
			String name = c.getName();
			StringBuffer value = new StringBuffer();
			
			String cvalue = c.getValue();
			
			Pattern p = Pattern.compile("^[a-zA-Z0-9]+$");
			Matcher m_name = p.matcher(name);
			Matcher m_value = p.matcher(cvalue);
			if( !m_name.find()){
				throw new IllegalArgumentException();
			}
			
			// if value has whitespace, put double quotes around it
			if(cvalue.contains(" ")){
				
				// put quotes around value with version less than 1.
				//   version puts quotes around strings with space.
				if(c.getVersion() < 1){
				
					if( (cvalue.startsWith("\"") && cvalue.endsWith("\"")) == false ){
						log.debug(cvalue + "has quotes");
						cvalue = "\"" + cvalue + "\"";	
					}
				}
				
			}
			
			value.append(name).append("=").append(cvalue);

			if(c.getMaxAge() != -1){
				Calendar calendar = Calendar.getInstance();
				Long d = calendar.getTime().getTime();
				d += c.getMaxAge();
				SimpleDateFormat dateFormat = new SimpleDateFormat(
				        "EEE dd MMM yyyy hh:mm:ss zzz", getLocale());
				String date_str = dateFormat.format(new Date(d));
				value.append("; ").append("expires=").append(date_str);
			}else if(c.getVersion() != -1){
				value.append("; ").append("Version=").append(c.getVersion());
			}else if(c.getComment() != null  ){
				value.append("; ").append("Comment=").append("\"").append(c.getComment()).append("\"");
			}else if(c.getDomain() != null){
				if(c.getDomain().contains(" ")){
					throw new IllegalArgumentException();
				}
				value.append("; ").append("Domain=").append(c.getDomain());
			}else if(c.getPath() != null){
				if(c.getPath().contains(" ")){
					throw new IllegalArgumentException();
				}
				value.append("; ").append("Path=").append(c.getPath());
			}else if(c.getSecure() == true){
				value.append("; ").append("Secure=").append(c.getSecure());
			}
			
			List<String> cookieHeader = m_headers.get("Set-Cookie");
			if(cookieHeader == null){
				List<String> cookieVal = new ArrayList<String>();
				cookieVal.add(value.toString());
				m_headers.put("Set-Cookie", cookieVal);
			}else{
				m_headers.get("Set-Cookie").add(value.toString());
			}
			
		}
				
	}

	@Override
	public void addDateHeader(String name, long date) {
		

		if(containsHeader(name)){
			Date d = new Date(date);
			m_headers.get(name).add(d.toString());
		}
		else{
			List<String> newlist = new ArrayList<String>();
			Date d = new Date(date);
			newlist.add(d.toString());
			m_headers.put(name, newlist);
		}

	}

	
	@Override
	public void addHeader(String name, String value) {
		

		if(containsHeader(name)){
			m_headers.get(name).add(value);
		}
		else{
			List<String> newlist = new ArrayList<String>();
			newlist.add(value);
			m_headers.put(name, newlist);
		}
		
	}

	
	@Override
	public void addIntHeader(String name, int value) {
		

		if(containsHeader(name)){
			m_headers.get(name).add( String.valueOf(value) );
		}
		else{
			List<String> newlist = new ArrayList<String>();
			newlist.add( String.valueOf(value));
			m_headers.put(name, newlist);
		}

	}

	
	@Override
	public boolean containsHeader(String name) {
		
		return m_headers.containsKey(name);
	}

	@Override
	public String encodeRedirectURL(String url) {
		
		String encoded;
		
		try{
			encoded = URLEncoder.encode(url,"UTF-8");
		}catch(UnsupportedEncodingException ee){
			
			encoded = null;
		}
		
		return encoded;
	}

	//DEPRECATED
	public String encodeRedirectUrl(String arg0) {
		return null;
	}

	@Override
	public String encodeURL(String url) {
		String encoded;
		
		try{
			encoded = URLEncoder.encode(url,"UTF-8");
		}catch(UnsupportedEncodingException ee){
			
			encoded = null;
		}
		
		return encoded;
	}

	//DEPRECATED
	public String encodeUrl(String arg0) {
		return null;
	}

	@Override
	public void sendError(int code) throws IOException {

		if(committed == true){
			throw new IllegalStateException();
		}
		else{
			String errorString = HttpResponseUtils.writeErrorResponse(code, http_protocol, keep_alive);
			PrintWriter out = new PrintWriter(requestSocket.getOutputStream());
			out.write(errorString);
			out.flush();

			// clear the buffer
			reset();
			// set to "Committed" state ? throw IllegalStateException if its already "committed"
			committed = true;
		}
	}

	@Override
	public void sendError(int code, String msg) throws IOException {
	
		if(committed == true){
			throw new IllegalStateException();
		}
		else{
			String errorString = HttpResponseUtils.writeErrorResponse(code, http_protocol, msg, keep_alive);

			PrintWriter out = new PrintWriter(requestSocket.getOutputStream());
			out.write(errorString);
			out.flush();

			// clear the buffer
			reset();
			// set to "Committed" state ? throw IllegalStateException if its already "committed"
			committed = true;
		}
		
	}

	@Override
	public void sendRedirect(String location) throws IOException {
		// TODO Auto-generated method stub
		
		if(committed == true){
			throw new IllegalStateException();
		}
		else{
			String host_string = requestparser.host+":"+String.valueOf(requestparser.port);

			StringBuffer buff = new StringBuffer();
			if(location != null){
				
				if(location.startsWith("/")) // relative to servlet container root (root_dir)
				{
					if(requestparser.url_protocol != null){
						buff.append(requestparser.url_protocol).append(requestparser.host).append(":").append(requestparser.port).append(location);
					}else{
						buff.append("http://").append(requestparser.host).append(":").append(requestparser.port).append(location);
					}
				}else if(location.startsWith("//")){ // resource URI
					URL resource = myServletContext.class.getClassLoader().getResource(location);
					
					try {
						Path p;
						p = Paths.get(resource.toURI()).toRealPath();
						File f = new File(p.toString());

						if(f.exists() == true){
							
							buff.append(p.toString());
						}
					} catch (URISyntaxException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
				}else{
					
					//contextpath is "" for our web app
					String uri_path = ( "" ) + 
							          ( requestparser.m_servlet_path == null ? "" : requestparser.m_servlet_path ) + 
							          ( requestparser.path_info == null ? "" : requestparser.path_info ); 
					
					log.debug("uri_path: " + uri_path);
					
					if( uri_path.indexOf("/") == 0 ){
						// do nothign
						uri_path = "/";
					}else{
						uri_path = uri_path.substring(0, uri_path.lastIndexOf("/"));
					}
					
					
					
					if(requestparser.url_protocol != null){
						buff.append(requestparser.url_protocol).append(requestparser.host).append(":").append(requestparser.port).append(uri_path).append(location);
					}else{
						buff.append("http://").append(requestparser.host).append(":").append(requestparser.port).append(uri_path).append(location);
					}
				}
				resetBuffer();
				m_headers.clear();
				status_code = 302;
				addHeader("Location", encodeURL(buff.toString()));
			}
		}
		
	}

	@Override
	public void setDateHeader(String name, long time) {
		

		Date d = new Date(time);
		List<String> newdate = new ArrayList<String>();
		newdate.add(d.toString());
		m_headers.put(name,newdate);


	}

	@Override
	public void setHeader(String name, String val) {

		List<String> list = new ArrayList<String>();
		list.add(val);
		m_headers.put(name, list);
		
	}

	@Override
	public void setIntHeader(String name, int val) {

		List<String> list = new ArrayList<String>();
		list.add(String.valueOf(val));
		m_headers.put(name, list);
		
	}

	
	/**
	 * This method is used to set the return status code when there is no error. 
	 * 
	 * If this method is used to set an error code, then the container's error page mechanism will 
	 *   not be triggered. If there is an error and the caller wishes to invoke an error page defined 
	 *   in the web application, then sendError(int, java.lang.String) must be used instead.
	 */
	
	@Override
	public void setStatus(int code) {	

		status_code = code;

	}

	//DEPRECATED
	@Override
	public void setStatus(int arg0, String arg1) {
		
	}
	

}
