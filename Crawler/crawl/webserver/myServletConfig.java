package edu.upenn.cis.cis455.webserver;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Set;
import java.util.Vector;

import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;

import org.apache.log4j.Logger;

public class myServletConfig implements ServletConfig {

	private String servletname;
	private myServletContext context;
	private HashMap<String,String> initParams;
	
	Logger log = Logger.getLogger(myServletConfig.class);
	
	public myServletConfig(String sname, myServletContext context){
		this.servletname = sname;
		this.context = context;
		initParams = new HashMap<String,String>();
	}
	
	
	@Override
	public String getInitParameter(String pname) {
		return initParams.get(pname);
	}

	@Override
	public Enumeration getInitParameterNames() {
		Set<String> keys = initParams.keySet();
		Vector<String> atts = new Vector<String>(keys);
		return atts.elements();
	}

	@Override
	public myServletContext getServletContext() {
		return context;
	}

	@Override
	public String getServletName() {
		return servletname;
	}
	
	public void setInitParam(String pname, String value) {
//		log.debug("setting ");
		initParams.put(pname, value);
	}

}
