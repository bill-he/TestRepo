package edu.upenn.cis.cis455.webserver;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.security.Principal;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletContext;
import javax.servlet.ServletInputStream;
import javax.servlet.ServletRequest;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

import org.apache.log4j.Logger;

import edu.upenn.cis.cis455.webserver.utils.HttpRequestParser;
import edu.upenn.cis.cis455.webserver.utils.HttpResponseUtils;

public class myHttpServletRequest implements HttpServletRequest {
	
	Logger log = Logger.getLogger(myHttpServletRequest.class);
	
	private Socket socket = null;
	private HttpServlet servlet = null;
	private Properties m_params = new Properties();
	private Properties m_props = new Properties();
	private HashMap<String, List<String>>m_headers = new HashMap<String,List<String>>();
	
	private String m_method = null;
	private String http_protocol = null;
	private String character_encoding = null;
	private Locale locale = null;
	private String query_string = null;
	private String resource_uri = null;
	private String host = null;
	private int port = 0;
	private String url_protocol = null;
	private Cookie[] cookies;
	
	private String m_servlet_path = null;
	private String pattern_matched = null;
	private String path_info = null;
	private boolean keepalive = false;
	
	private myHttpServletSession m_session = null;
	private String session_id = null;
	private boolean session_id_valid = false;
	private boolean session_id_from_cookie = false;
	private HttpRequestParser requestParser = null;
	
	// Constructors
	public myHttpServletRequest(){
		
	}
	
	public myHttpServletRequest( HttpRequestParser parser, HttpServlet http_servlet, myHttpServletSession session){
		
		servlet = http_servlet;
		socket = parser.socket;
		requestParser = parser;
		//m_session = parser.m_session;
		m_session = session;
		session_id_valid = parser.session_id_valid;
		session_id_from_cookie = parser.session_id_from_cookie;
		
		
		m_method = parser.m_method;
		http_protocol = parser.http_protocol;
		character_encoding = parser.character_encoding;
		locale = parser.locale;
		query_string = parser.query_string;
		resource_uri = parser.resource_uri;
		cookies = parser.cookies;
		session_id = parser.requested_session_id;
		m_headers = parser.m_headers;
		m_params = parser.m_params;
		m_servlet_path = parser.m_servlet_path;
		pattern_matched = parser.pattern_matched;
		path_info = parser.path_info;
		host = parser.host;
		port = parser.port;
		url_protocol = parser.url_protocol;
		keepalive = parser.keepalive;
	}
	
	@Override
	public String getAuthType() {
		return BASIC_AUTH;
	}

	//the part of the path that identifies the application
	@Override
	public String getContextPath() {
		return "";
	}
	
	@Override
	public Cookie[] getCookies() {
		
		return cookies;
	}

	@Override
	public long getDateHeader(String name) {
		
		if( m_headers.containsKey(name.toLowerCase()) == false  ){
			return -1;
		}
		
		String date_str = m_headers.get(name.toLowerCase()).get(0);
		Date d = HttpResponseUtils.parseHeaderDate(date_str);
			
		if(d == null){
			
			throw new IllegalArgumentException();
		}
		
		return d.getTime();
	}

	@Override
	public String getHeader(String name) {
		
		if( m_headers.containsKey(name.toLowerCase()) == false  ){
			return null;
		}
		
		String val = m_headers.get(name.toLowerCase()).get(0);
		
		return val;
	}

	@Override
	public Enumeration<String> getHeaderNames() {
		
		if( m_headers.keySet().isEmpty() == true  ){
			return Collections.emptyEnumeration();
		}
		
		Enumeration e = Collections.enumeration(m_headers.keySet());
		return e;
			
	}
	
	@Override
	public Enumeration<String> getHeaders(String name) {
		
		if( m_headers.containsKey(name.toLowerCase()) == false  ){
			return Collections.emptyEnumeration();
		}
		
		Enumeration<String> e = Collections.enumeration( m_headers.get(name.toLowerCase()));
		return e;
		
	}
	

	@Override
	public int getIntHeader(String name) {
		
		if( m_headers.containsKey(name.toLowerCase()) == false  ){
			return -1;
		}
		
		String val = m_headers.get(name.toLowerCase()).get(0);
		
		int i = 0;
		try {
			i = Integer.valueOf(val);
		}catch (NumberFormatException n){
			throw n;
		}
		
		return i;
	}

	@Override
	public String getMethod() {
		return m_method;
	}

	@Override
	public String getPathInfo() {
		//TO DO, get the remainder of the URL request after the portion matched by the url-pattern
		//  in web.xml
		return path_info;
	}

	// NOT REQUIRED
	@Override
	public String getPathTranslated() {
		return null;
	}

	@Override
	public String getQueryString() {
		return query_string;
	}
	
	@Override
	public String getRemoteUser() {
		//Returns the login of the user making this request, if the user has been authenticated, 
		//  or null if the user has not been authenticated.
		return null;
	}

	@Override
	public String getRequestedSessionId() {
		return session_id;
	}
	
	@Override
	public String getRequestURI() {
		return resource_uri;
	}
	
	@Override
	public StringBuffer getRequestURL() {
		// Reconstructs the URL the client used to make the request. The returned URL contains 
		//		a protocol, server name, port number, and server path, 
		// 		but it does not include query string parameters.
		
		StringBuffer buff = new StringBuffer();
		if(url_protocol != null){
			buff.append(url_protocol).append(host).append(":").append(port).append("/").append(resource_uri);
		}else{
			buff.append(getScheme()).append("://").append(host).append(":").append(port).append("/").append(resource_uri);
		}
		
		return buff;
	}
	
	@Override
	public String getServletPath() {
		// TODO not sure the special case which this conditions hold true for....
		if(pattern_matched != null){
			if(pattern_matched.compareTo("/*") == 0){
				return "/";
			}
			return m_servlet_path;
		}else{
			return null;
		}
		
	}
	
	@Override
	public HttpSession getSession(boolean create) {
		
		if (create == true) { // always return a new session
			m_session = new myHttpServletSession(servlet.getServletContext()); //random generated id in constructor;
			
			log.debug("creating new session! ");
				
//			synchronized(requestParser.sessionMap){
//				if(!requestParser.sessionMap.containsKey(m_session.getId())){ // only for first time use
//
//					log.debug("Adding Session ref to map!");
//
//					requestParser.sessionMap.put(m_session.getId(), m_session);
//				}
//			}
				
			return m_session;
			
		} else {
			// give an existing one, but dont create if there is none.
			if (hasSession() == false) {
				return null;
			} else {
				return m_session;
			}
		}
		
	}

	@Override
	/* Returns the current session associated with this request, or 
	  if the request does not have a session, creates one.*/
	public HttpSession getSession() {
		
		if(m_session == null){
			log.debug("getSession called, and no existing session");
			return getSession(true);
		}
		log.debug("getSession called, and there is an existing session");
		return getSession(false);
		
	}

	// NOT REQUIRED
	@Override
	public Principal getUserPrincipal() {
		return null;
	}
	
	@Override
	public boolean isRequestedSessionIdValid() {
			
		return session_id_valid;
	}

	@Override
	public boolean isRequestedSessionIdFromCookie() {
		
		return session_id_from_cookie;
	}

	@Override
	public boolean isRequestedSessionIdFromURL() {
		// not required to handle this, default to false.
		return false;
	}
	
	
	// NOT REQUIRED
	@Override
	public boolean isUserInRole(String arg0) {
		return false;
	}

	// Deprecated
	public boolean isRequestedSessionIdFromUrl() {
	
		return false;
	}

	@Override
	public Object getAttribute(String name) {

		return m_props.get(name);
	}

	@Override
	public Enumeration getAttributeNames() {

		return m_props.keys();
	}

	@Override
	public String getCharacterEncoding() {
		
		if(character_encoding != null){
			return character_encoding;
		}
		return "ISO-8859-1";
	}

	@Override
	public void setCharacterEncoding(String encoding)
			throws UnsupportedEncodingException {
		character_encoding = encoding;

	}

	@Override
	public int getContentLength() {
		
		if( m_headers.containsKey("content-length")  ){
			return Integer.valueOf(m_headers.get("content-length").get(0));
		}
		
		return 0;
	}


	@Override
	public String getContentType() {

		if( m_headers.containsKey("content-type")  ){
			return m_headers.get("content-type").get(0);
		}
		return null;
	}

	// NOT REQUIRED
	@Override
	public ServletInputStream getInputStream() throws IOException {
		return null;
	}

	//  the server's IP address as a string
	@Override
	public String getLocalAddr() {

		return socket.getLocalAddress().toString();
	}
	
	@Override
	public Locale getLocale() {
		
		if(locale != null){
			return locale;
		}
		return null;
	}
	
	// NOT REQUIRED
	@Override
	public Enumeration getLocales() {
		return null;
	}
	
	// the name of the server recieving the request
	@Override
	public String getLocalName() {
		return host;
	}
	
	//the port the server recieved the request on
	@Override
	public int getLocalPort() {
		return socket.getLocalPort();
	}

	@Override
	public String getParameter(String arg0) {
		return m_params.getProperty(arg0);
	}

	@Override
	public Map getParameterMap() {
		return m_params;
	}
	
	@Override
	public Enumeration getParameterNames() {
		return m_params.keys();
	}

	@Override
	public String[] getParameterValues(String key) {
		//TODO
		String[] value = new String[1];
		value [0] = (String)m_params.get(key);
		return value;
	}

	@Override
	public String getProtocol() {
		return http_protocol;
	}
	
	@Override
	public BufferedReader getReader() throws IOException {

		BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

		return reader;
	}
	
	//DEPRECATED
	@Override
	public String getRealPath(String arg0) {

		return null;
	}
	
	@Override
	public String getRemoteAddr() {
		
		return ((InetSocketAddress)socket.getRemoteSocketAddress()).toString();
	}
	
	@Override
	public String getRemoteHost() {
		
		return ((InetSocketAddress)socket.getRemoteSocketAddress()).getHostName();
	}
	
	@Override
	public int getRemotePort() {

		return ((InetSocketAddress)socket.getRemoteSocketAddress()).getPort();
	}

	// NOT REQUIRED
	@Override
	public RequestDispatcher getRequestDispatcher(String arg0) {
		return null;
	}

	@Override
	public String getScheme() {
		
		return "http";
	}

	//the name of the server that the request was sent to
	@Override
	public String getServerName() {
		
		return host;
	}

	//the port the request was sent to
	@Override
	public int getServerPort() {
		
		return port;
	}

	
	public ServletContext getServletContext(){
		return servlet.getServletContext(); 
	}
	
	@Override
	public boolean isSecure() {
		return false;
	}
	
	@Override
	public void setAttribute(String arg0, Object arg1) {
		m_props.put(arg0, arg1);
	}

	@Override
	public void removeAttribute(String arg0) {
		m_props.remove(arg0);
	}
	
	
	@Override
	public String toString(){
		
		StringBuffer strbuf = new StringBuffer();
		
		strbuf.append(" Auth Type: ").append(getAuthType()).append("\n");
		strbuf.append(" Header Names: ").append("\n");
		   Enumeration e = getHeaderNames();
		   while(e.hasMoreElements()){
			 String name = (String) e.nextElement();  
			 log.debug("Header name=" + name);
		     strbuf.append("\t").append( name ).append(":").append(getHeader(name)).append("\n");
		   }
		strbuf.append(" Method: ").append( getMethod() ).append("\n");
		strbuf.append(" Path Info: ").append(getPathInfo() ).append("\n");
		strbuf.append(" Context Path: ").append( getContextPath()).append("\n");
		
		strbuf.append(" Query String: ").append(getQueryString() ).append("\n");
		strbuf.append(" Session Id: ").append(getRequestedSessionId() ).append("\n");
		strbuf.append(" Request URI: ").append(getRequestURI() ).append("\n");
		strbuf.append(" Request URL: ").append(getRequestURL() ).append("\n");
		strbuf.append(" Servlet Path: ").append(getServletPath() ).append("\n");
		strbuf.append(" Session ").append(getSession().toString() ).append("\n");
		
		strbuf.append(" is Requested Session ID Valid: ").append(isRequestedSessionIdValid() ).append("\n");
		strbuf.append(" is Requested Session ID from Cookie: ").append(isRequestedSessionIdFromCookie() ).append("\n");
		strbuf.append(" is Requested Session ID from URL: ").append(isRequestedSessionIdFromURL() ).append("\n");
		
		strbuf.append(" Character Encoding: ").append(getCharacterEncoding() ).append("\n");
		strbuf.append(" Content Length: ").append(getContentLength() ).append("\n");
		strbuf.append(" Content Type: ").append(getContentType() ).append("\n");
		strbuf.append(" Parameter Names: ").append( getParameterNames().toString() ).append("\n");
		//strbuf.append(" Parameter Values: ").append( ).append("\n");
		strbuf.append(" Protocol: ").append(getProtocol() ).append("\n");
		strbuf.append(" Scheme: ").append(getScheme() ).append("\n");
		strbuf.append(" Server Name: ").append( getServerName()).append("\n");
		strbuf.append(" Server Port: ").append(getServerPort() ).append("\n");
		
		strbuf.append(" Remote Address: ").append(getRemoteAddr() ).append("\n");
		strbuf.append(" Remote Host: ").append(getRemoteHost() ).append("\n");
		strbuf.append(" Remote Port: ").append( getRemotePort()).append("\n");
		
		strbuf.append(" Local Address: ").append(getLocalAddr() ).append("\n");
		strbuf.append(" Local Name: ").append(getLocalName() ).append("\n");
		strbuf.append(" Local Port: ").append(getLocalPort() ).append("\n");
		
//		strbuf.append(" Locale: ").append(getLocale() ).append("\n");
		strbuf.append(" is Secure: ").append(isSecure() ).append("\n");
//		strbuf.append("  ").append( ).append("\n");
//		strbuf.append("  ").append( ).append("\n");
		
		return strbuf.toString();
		
	}
	
	
	
	/** Mutators **/
	
	public void clearParameters() {
		m_params.clear();
	}
	
	public void clearHeaders() {
		m_headers.clear();
	}
	
	public boolean hasSession() {
		return ((m_session != null));
	}
		
	
}
