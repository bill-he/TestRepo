package edu.upenn.cis.cis455.xpathengine;

import org.xml.sax.helpers.DefaultHandler;

/**
 * XPath Evaluator: works perfectly: DOM (not SAX)
 * 
 * Sets the XPath expression(s) that are to be evaluated.
 * @param expressions
 
	void setXPaths(String[] expressions);
 
 * Returns true if the i.th XPath expression given to the last setXPaths() call
 * was valid, and false otherwise. If setXPaths() has not yet been called, the
 * return value is undefined. 
 * @param i
 * @return
 
	boolean isValid(int i);
	
 * DOM Parser evaluation.
 * Takes a DOM root node as its argument, which contains the representation of the 
 * HTML or XML document. Returns an array of the same length as the 'expressions'
 * argument to setXPaths(), with the i.th element set to true if the document 
 * matches the i.th XPath expression, and false otherwise. If setXPaths() has not
 * yet been called, the return value is undefined.
 *
 * @param d Document root node
 * @return bit vector of matches
 * 
	boolean[] evaluate(Document d);
	
	
 * @author billhe
 *
 */
public class XPathEngineFactory {
	public static XPathEngine getXPathEngine() {
		return new XPathEngineImpl();
	}
	
	public static DefaultHandler getSAXHandler() {
		return null;
	}
}
