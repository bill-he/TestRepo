package edu.upenn.cis.cis455.webserver;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Enumeration;
import java.util.Locale;
import java.util.Properties;
import java.util.TimeZone;
import java.util.UUID;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpSession;
import javax.servlet.http.HttpSessionContext;

import org.apache.log4j.Logger;
import org.eclipse.jetty.util.log.Log;

import edu.upenn.cis.cis455.webserver.utils.HttpResponseUtils;

public class myHttpServletSession implements HttpSession {
	
	static SimpleDateFormat dateFormat = new SimpleDateFormat(
	        "EEE dd MMM yyyy hh:mm:ss zzz", Locale.US);
	
	Logger log = Logger.getLogger(myHttpServletSession.class);
	
	String id;
	Calendar calendar = Calendar.getInstance();
	//Date creationDate = null;
	//Date lastAccessed = null;
    boolean persistentSession = false;
    boolean isNew = true;
    int inactiveInterval = 120; // default 2 minutes 
    ServletContext context = null;
    
	// Properties is basically a "Hash Table" for storing name-value pairs, values would be objects
	private Properties m_props = new Properties();
	private boolean m_valid = true;
    
	
	// public constructor
	public myHttpServletSession(ServletContext s ){
		
		// generate a unique uuid for the session as its identifier.
		id = UUID.randomUUID().toString();
		
		m_props.setProperty("creation-date", dateFormat.format(calendar.getTime()));
		m_props.setProperty("last-access", dateFormat.format(calendar.getTime()));
		
		context = s;
		
	}
	
    // To set the is new boolean, copy over all the properties	
	public myHttpServletSession( myHttpServletSession session, boolean isnew){
		
		id = session.getId();
		
		//copy over all attributes
		Enumeration e = session.getAttributeNames(); 
		
		while( e.hasMoreElements()){
			String key = (String) e.nextElement();
			log.debug("copying over key " + key);
			m_props.put(key, session.getAttribute(key) );
		}
		
		//m_props.put("creation-date", dateFormat.format(session.getCreationTime()));
		
		
		// update the last accessed time
		m_props.remove("last-access");
		m_props.setProperty("last-access", dateFormat.format(calendar.getTime()));
		
		inactiveInterval = session.getMaxInactiveInterval();
		context = session.getServletContext();
		isNew = isnew;
	}
	
	
	// updates the last access time 
//	public void setLastAccessedTime(){
//		lastAccessed = Calendar.getInstance().getTime();
//	}
	
	
	/* (non-Javadoc)
	 * @see javax.servlet.http.HttpSession#getCreationTime()
	 */
	@Override
	public long getCreationTime() {
		if(m_valid == false){
			throw new IllegalStateException();
		}
		
		Date d;
		try {
			d = dateFormat.parse( m_props.getProperty("creation-date"));
			return d.getTime();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 0;
	}

	/* )(non-Javadoc)
	 * @see javax.servlet.http.HttpSession#getId()
	 */
	@Override
	public String getId() {
		
		return id.toString();
	}

	/* (non-Javadoc)
	 * @see javax.servlet.http.HttpSession#getLastAccessedTime()
	 */
	@Override
	public long getLastAccessedTime() {
		// TODO Auto-generated method stub
		if(m_valid == false){
			throw new IllegalStateException();
		}
		
		Date d;
		try {
			d = dateFormat.parse( m_props.getProperty("last-access"));
			return d.getTime();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return 0;
	}

	/* (non-Javadoc)
	 * @see javax.servlet.http.HttpSession#getServletContext()
	 */
	@Override
	public ServletContext getServletContext() {
		// TODO Auto-generated method stub
		return context;
	}

	/* (non-Javadoc)
	 * @see javax.servlet.http.HttpSession#setMaxInactiveInterval(int)
	 */
	@Override
	public void setMaxInactiveInterval(int interval) {
		
		// Specifies the time, in seconds, between client requests before the servlet container 
		// will invalidate this session. A negative time indicates the session should never timeout.
		if(m_valid == false){
			throw new IllegalStateException();
		}
		
		if(interval == -1){
			persistentSession = true;
		} 
		else{
			inactiveInterval = interval;
		}
		
	}

	/* (non-Javadoc)
	 * @see javax.servlet.http.HttpSession#getMaxInactiveInterval()
	 */
	@Override
	public int getMaxInactiveInterval() {
		// TODO Auto-generated method stub
		if(m_valid == false){
			throw new IllegalStateException();
		}
		return inactiveInterval;
	}

	//DEPRECATED
	public HttpSessionContext getSessionContext() {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see javax.servlet.http.HttpSession#getAttribute(java.lang.String)
	 */
	@Override
	public Object getAttribute(String arg0) {
		if(m_valid == false){
			throw new IllegalStateException();
		}
		return m_props.getProperty(arg0);
	}


	//DEPRECATED
	public Object getValue(String arg0) {
		if(m_valid == false){
			throw new IllegalStateException();
		}
		return null;
	}

	/* (non-Javadoc)
	 * @see javax.servlet.http.HttpSession#getAttributeNames()
	 */
	@Override
	public Enumeration getAttributeNames() {
		if(m_valid == false){
			throw new IllegalStateException();
		}
		return m_props.keys();
	}


	//DEPRECATED
	public String[] getValueNames() {
		if(m_valid == false){
			throw new IllegalStateException();
		}
		return null;
	}

	/* (non-Javadoc)
	 * @see javax.servlet.http.HttpSession#setAttribute(java.lang.String, java.lang.Object)
	 */
	@Override
	public void setAttribute(String arg0, Object arg1) {
		if(m_valid == false){
			throw new IllegalStateException();
		}
		m_props.setProperty(arg0, (String)arg1);
	}

	//DEPRE)CATED
	public void putValue(String arg0, Object arg1) {
		if(m_valid == false){
			throw new IllegalStateException();
		}
		m_props.put(arg0, arg1);
	}

	/* (non-Javadoc)
	 * @see javax.servlet.http.HttpSession#removeAttribute(java.lang.String)
	 */
	@Override
	public void removeAttribute(String arg0) {
		if(m_valid == false){
			throw new IllegalStateException();
		}
		m_props.remove(arg0);
	}

	//DEPRECATED
	public void removeValue(String arg0) {
		if(m_valid == false){
			throw new IllegalStateException();
		}
		m_props.remove(arg0);
	}

	/* (non-Javadoc)
	 * @see javax.servlet.http.HttpSession#invalidate()
	 */
	@Override
	public void invalidate() {
		if(m_valid == false){
			throw new IllegalStateException();
		}
		m_props.clear();
		m_valid = false;
		
	}

	/* (non-Javadoc)
	 * @see javax.servlet.http.HttpSession#isNew()
	 */
	@Override
	public boolean isNew() {
		// TODO Auto-generated method stub
		if(m_valid == false){
			throw new IllegalStateException();
		}
		return isNew;
	}
	
	public boolean isValid(){
		return m_valid;
	}
	

}
