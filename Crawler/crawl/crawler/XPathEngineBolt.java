package edu.upenn.cis.cis455.crawler;

import java.io.ByteArrayInputStream;
import java.util.Map;
import java.util.UUID;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.jsoup.Jsoup;
import org.jsoup.helper.W3CDom;
import org.jsoup.nodes.Document;

import edu.upenn.cis.cis455.storage.ChannelDA;
import edu.upenn.cis.cis455.storage.URLRecordDA;
import edu.upenn.cis.cis455.xpathengine.XPathEngine;
import edu.upenn.cis.cis455.xpathengine.XPathEngineFactory;
import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.bolt.IRichBolt;
import edu.upenn.cis.stormlite.bolt.OutputCollector;
import edu.upenn.cis.stormlite.routers.IStreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;

public class XPathEngineBolt implements IRichBolt {
	Fields myFields = new Fields();
    private URLRecordDA URLRecordDA;
    String executorId = UUID.randomUUID().toString();

    public XPathEngineBolt() {
    }

	@Override
	public void cleanup() {
		// Do nothing
	}

	@Override
	public void execute(Tuple input) {
		try {
			String urlString = input.getStringByField("url");
			String htmlDocument = input.getStringByField("HTMLdocument");

			System.out.println("Xpathing Engine: " + urlString);
			
			org.w3c.dom.Document w3cDoc;
			if (urlString.endsWith("xml")) {
				  DocumentBuilderFactory factory =
						  DocumentBuilderFactory.newInstance();
				  DocumentBuilder builder;
			
				  builder = factory.newDocumentBuilder();
				  StringBuilder xmlStringBuilder = new StringBuilder();
				  xmlStringBuilder.append(htmlDocument);
				  ByteArrayInputStream inputByte =  new ByteArrayInputStream(
				     xmlStringBuilder.toString().getBytes("UTF-8"));
				  w3cDoc = builder.parse(inputByte);
			} else {
				Document doc = Jsoup.parse(htmlDocument);
		    	W3CDom w3cDom = new W3CDom();
		    	w3cDoc = w3cDom.fromJsoup(doc);
			}
			
	        ChannelDA channelDA = new ChannelDA(XPathCrawler.dbdDirectory);
	        String[] channelArray = (String[]) channelDA.getChannelMap().keySet().toArray();
	        String[] xpathArray = new String[channelArray.length];
			boolean[] booleanArray = new boolean[channelArray.length];
	        for (int x=0; x<channelArray.length;x++) {
	        	xpathArray[x] = channelDA.getXPath(channelArray[x]);
	        }
			XPathEngine xpathEngine = XPathEngineFactory.getXPathEngine();
	    	xpathEngine.setXPaths(xpathArray);
	    	
	    	booleanArray = xpathEngine.evaluate(w3cDoc);
	    	for (int x=0; x < channelArray.length; x++) {
	    		if (booleanArray[x]) {
	    			channelDA.putDocument(channelArray[x], URLRecordDA.getRecord(urlString));
	    		}
	    	}
		} catch (Exception ex) {
			
		}
	}

	@Override
	public void prepare(Map<String, String> stormConf, TopologyContext context, OutputCollector collector) {
	}

	@Override
	public String getExecutorId() {
		return executorId;
	}

	@Override
	public void setRouter(IStreamRouter router) {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(myFields);
	}

	@Override
	public Fields getSchema() {
		return myFields;
	}

}
