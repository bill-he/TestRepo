package edu.upenn.cis.cis455.xpathengine;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.helpers.DefaultHandler;

public class XPathEngineImpl implements XPathEngine {

	String[] XPaths;
	ArrayList<LinkedList<String>> XPathDeliminatedArray;
	LinkedList<String> currentXPathValidity;
	//HashSet<Node> currentLevelNodeSet;
	
	public XPathEngineImpl() {
	    // Do NOT add arguments to the constructor!!
	}
	
	public void setXPaths(String[] s) {
		if (s == null) s = new String[0];
		XPaths = s;
		XPathDeliminatedArray = 
			new ArrayList<LinkedList<String>>();
		for (int x=0; x<XPaths.length; x++) {
			XPathDeliminatedArray.add(x,deliminate(XPaths[x]));
		}
	}
	
	public LinkedList<String> deliminate(String xPath) {
		xPath = xPath.trim();
		LinkedList<String> xPathDelim =
			new LinkedList<String>();
		while(xPath.length() > 0) {
			int index = 0;
			if (Character.isAlphabetic(xPath.charAt(index))
			 || Character.isDigit(xPath.charAt(index))) {
				while (Character.isAlphabetic(xPath.charAt(index)) 
					|| Character.isDigit(xPath.charAt(index))
					|| xPath.charAt(index) == '-') {
					if (++index >= xPath.length()) {
						break;
					}
				}
			} else {
				if (xPath.charAt(0) == '\"') {
					index = xPath.indexOf("\"", 1) + 1;
					if (index == 0) {
						index = xPath.length();
					}
				} else {
					index = 1;
				}
			}
			// System.out.println(xPath.substring(0, index));
			xPathDelim.add(xPath.substring(0, index));
			xPath = xPath.substring(index);
			xPath = xPath.trim();
		}
		return xPathDelim;
	}

	public boolean isValid(int i) {
		if (XPaths == null) {
			// System.out.println("isValid: XPath is null");
			return false;
		}
		if (i < 0 || i > XPaths.length) {
			// System.out.println("isValid: index out of bounds");
			return false;
		}

		return validityCheck(new LinkedList<String>(XPathDeliminatedArray.get(i)));
	}
	
	public boolean validityCheck(LinkedList<String> xPathToCheck) {
		currentXPathValidity = xPathToCheck;
		
		try {
			expect("/");
		    stepValid();
		} catch (Exception ec) {
			currentXPathValidity = null;
			return false;
		}
		currentXPathValidity = null;
		// System.out.println("Valid");
		return true;
	}
	
	public boolean[] evaluate(Document doc) { 
		if (XPaths == null) {
			// System.out.println("xpaths is null");
			return null;
		}
		boolean[] booleanArray = new boolean[XPaths.length];
		if (doc == null) {
			// System.out.println("document is null");
			return booleanArray;
		}
		
        for (int x=0; x<XPaths.length; x++) {
        	Node rootNode = doc.getDocumentElement();	        	
        	if (!isValid(x)) {
    			// System.out.println("isValid(" + x +") not valid");
        		continue;
        	}
        	
        	try {
	    		// System.out.println("EVALUATED START!!!!! \r\n\r\n");
	        	currentXPathValidity = new LinkedList<String>(XPathDeliminatedArray.get(x));
	        	
	        	
	        	expect("/");
	        	booleanArray[x] = step(rootNode);
	        	
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				// System.out.println("EXCEPTIONS!!");
				booleanArray[x] = false;
			}
        }
		return booleanArray; 
	}
	
	private boolean step(Node node) throws Exception {
		boolean isMatch = true;
    	boolean childNodeMatch = false;
		
		// System.out.println("\r\nStep Start");
		String nodeName = name();
		
		String currentNodeNodeName = node.getNodeName();
//		// System.out.println("nodeName:             " + nodeName);
//		// System.out.println("currentNodeNodeName : " + currentNodeNodeName);
		if (!nodeName.equalsIgnoreCase(node.getNodeName())) {
			// System.out.println("Does not Match");
			//isMatch = false;
			return false;
		} else {
//			// System.out.println("Match");
		}
		
	    while (accept("[")) {
	    	// System.out.println("Entered test condition");
	    	if (!test(node)) {
		        expect("]");
	    		//isMatch = false;
	    		return false;
	    	}
	        expect("]");
	    }
	    
	    if (accept("/")) {
	    	NodeList listOfNodes = node.getChildNodes();
	    	LinkedList<String> currentXPath = new LinkedList<String>(currentXPathValidity);
	    	// System.out.println("Children nodes set size: " + listOfNodes.getLength());
	    	for (int z=0; z<listOfNodes.getLength(); z++) {
	    		currentXPathValidity = new LinkedList<String>(currentXPath);
	    		if (step(listOfNodes.item(z))) {
	    			//childNodeMatch = true;
	    			return true;
	    		} else {
	    			// System.out.println("Rejected");
	    		}
    		}
	    	return false;
	    }
	    //return isMatch && childNodeMatch;
	    return true;
	}  
	
	private boolean test(Node node) throws Exception {
		
	    if(accept("text")) {
			// System.out.println("\r\n test:TEXT");
	    	expect("("); expect(")"); expect("=");
	    	
	    	String quotedString = quotedString();
			String nodeText = node.getTextContent();
			
			// System.out.println("quotedString: " + quotedString);
			// System.out.println("nodeText:     " + nodeText);
			
			if (node.getChildNodes() != null) {
				// System.out.println("current node child size:" + node.getChildNodes().getLength());
				for (int x=0; x<node.getChildNodes().getLength(); x++) {
					// System.out.println("child node name:" + node.getChildNodes().item(x).getNodeName());
					if (node.getChildNodes().item(x).getNodeName().equals("#text")) {
						// System.out.println("child node text content:" + node.getChildNodes().item(x).getTextContent());
						if (node.getChildNodes().item(x).getTextContent().equalsIgnoreCase(quotedString)) {
							return true;
						}	
					}
				}
			}
			return false;
			
	    } else if(accept("contains")) {
			// System.out.println("\r\n test:CONTAINS");
	    	expect("("); expect("text"); expect("("); expect(")"); expect(",");
	    	
	    	String quotedString = quotedString();
			String nodeText = node.getTextContent();
			
			// System.out.println("quotedString: " + quotedString);
			// System.out.println("nodeText:     " + nodeText);
			
			if (node.getChildNodes() != null) {
				// System.out.println("current node child size:" + node.getChildNodes().getLength());
				for (int x=0; x<node.getChildNodes().getLength(); x++) {
					// System.out.println("child node name:" + node.getChildNodes().item(x).getNodeName());
					if (node.getChildNodes().item(x).getNodeName().equals("#text")) {
						// System.out.println("child node text content:" + node.getChildNodes().item(x).getTextContent());
						String text = node.getChildNodes().item(x).getTextContent().toLowerCase();
						if (text.contains(quotedString.toLowerCase()))
							expect(")");
							return true;
					}
				}
			}
			expect(")");
			return false;
			
	    }  else if (accept("@")) {
			// System.out.println("\r\n test:ATTRIBUTE");
	    	String attributeName = name();
	        expect("=");
	        String attributeValue = quotedString();
	        // System.out.println("Quoted Attribute: " + attributeName);
	        // System.out.println("Quoted Value: " + attributeValue);
	        // System.out.println("Return true (unevaluated)");
	        return true;
	        
	        // test step
	    } else {
			// System.out.println("\r\ntest:STEP");
			
	    	NodeList listOfNodes = node.getChildNodes();
	    	LinkedList<String> currentXPath = new LinkedList<String>(currentXPathValidity);
	    	// System.out.println("Children nodes set size: " + listOfNodes.getLength());
	    	for (int z=0; z<listOfNodes.getLength(); z++) {
	    		currentXPathValidity = new LinkedList<String>(currentXPath);
	    		if (step(listOfNodes.item(z))) {
	    			 return true;
	    		}
    		}
	    	return false;
	    }
	}
	
	private String quotedString() throws Exception {
		//// System.out.println("quotedString called");
		String actual = currentXPathValidity.remove();
		//// System.out.println("Expect quote. Actual: "+ actual);
		return actual.substring(1, actual.length()-1);
	}
	
	private String name() throws Exception {
		//// System.out.println("name called");
		String actual = currentXPathValidity.remove();
		//// System.out.println("Expect name. Actual: "+ actual);
		return actual;
	}
	
	private boolean accept(String accept) {
		String actual = currentXPathValidity.peek();
		//System.out.print("Accept: " + accept + ". Actual: "+ actual);
		if (accept.equalsIgnoreCase(actual)) {
		//	// System.out.println("    Accept");
			currentXPathValidity.remove();
			return true;
		}
		//// System.out.println("    Don't Accept");
	    return false;
	}
	
	private boolean expect(String expected) throws Exception {
		String actual = currentXPathValidity.remove();
		// System.out.println("Expect: " + expected + ". Actual: "+ actual);
		if (!expected.equalsIgnoreCase(actual)) {
			throw new Exception();
		} else {
			return true;
		}
	}
	
	
	// to check validity
	private void testValid() throws Exception {
		// System.out.println("testValid called");
	    if(accept("text")) {
	    	expect("(");
	    	expect(")");
	    	expect("=");
	    	quotedStringValid();
	    } else if(accept("contains")) {
	    	expect("(");
	    	expect("text");
	    	expect("(");
	    	expect(")");
	    	expect(",");
	    	quotedStringValid();
	    	expect(")");
	    }  else if (accept("@")) {
	    	nameValid();
	        expect("=");
	    	quotedStringValid();
	    } else {
	    	stepValid();
	    }
	}
	
	private void quotedStringValid() throws Exception {
		// System.out.println("quotedStringValid called");
		String actual = currentXPathValidity.remove();
		// System.out.println("Expect quote. Actual: "+ actual);
		if (actual.charAt(0) != '\"' || actual.charAt(actual.length()-1) != '\"') {
			throw new Exception();
		}
	}
	
	private void nameValid() throws Exception {
		// System.out.println("nameValid called");
		String actual = currentXPathValidity.remove();
		// System.out.println("Expect name. Actual: "+ actual);
		for (int x=0; x < actual.length(); x++) {
			if (!(Character.isAlphabetic(actual.charAt(x)) 
		     || Character.isDigit(actual.charAt(x))
			 || actual.charAt(x) == '-')) {
				throw new Exception();
			}
		}
	}
	
	private void stepValid() throws Exception {
		// System.out.println("stepvalid called");
		name();
	    while (accept("[")) {
	    	testValid();
	        expect("]");
	    }
	    
	    if (accept("/")) {
	    	stepValid();
	    }
	}  

	@Override
	public boolean isSAX() {
		return false;
	}
	
	@Override
	public boolean[] evaluateSAX(InputStream document, DefaultHandler handler) {
		return null;
	} 
}
