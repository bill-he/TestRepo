package edu.upenn.cis.cis455.webserver.servlet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.ECS.client.jax.AWSECommerceService;
import com.ECS.client.jax.AWSECommerceServicePortType;
import com.ECS.client.jax.AWSHandlerResolver;
import com.ECS.client.jax.Item;
import com.ECS.client.jax.ItemSearch;
import com.ECS.client.jax.ItemSearchRequest;
import com.ECS.client.jax.ItemSearchResponse;
import com.ECS.client.jax.Items;
import com.ECS.client.jax.SignedRequestsHelper;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Node;

public class AmazonSearchHelper {

	private static final String ACCESS_KEY_ID = "AKIAIUYRAI3RK74ZH2PQ";
	private static final String SECRET_ACCESS_KEY_ID = "bV7mbicKiFendMJh6jH2vpqp9EJH39JKCBXSz3Ec";
	private static final String ASSOCIATE_TAG = "mantism-20";
	private static final String ENDPOINT = "ecs.amazonaws.com";
	private static final int limit = 3;
	private String query;
	private AWSECommerceService service;
	private AWSECommerceServicePortType port;
	private ItemSearchRequest itemRequest;
	private ItemSearch itemSearch;
	private ItemSearchResponse itemResponse;

	public AmazonSearchHelper(String q) {
		query = q;
		service = new AWSECommerceService();
		service.setHandlerResolver(new AWSHandlerResolver(SECRET_ACCESS_KEY_ID));
		port = service.getAWSECommerceServicePort();
		itemRequest = new ItemSearchRequest();

	}

	public ArrayList<Item> search() {
		ArrayList<Item> results = new ArrayList<Item>();
		itemRequest.setSearchIndex("All");
		itemRequest.setKeywords(query);
		itemSearch = new ItemSearch();
		itemSearch.setAWSAccessKeyId(ACCESS_KEY_ID);
		itemSearch.setAssociateTag(ASSOCIATE_TAG);

		itemSearch.getRequest().add(itemRequest);

		try {
			itemResponse = port.itemSearch(itemSearch);
			for (Items itemList : itemResponse.getItems()) {
				for (Item item : itemList.getItem()) {
					results.add(item);
					if (results.size() == limit)
						return results;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return results;
	}
	
	public static Document restRequest(String asin, String responseGroup) {
		
		Document doc = null;
		
		SignedRequestsHelper helper;
		try {
			helper = SignedRequestsHelper.getInstance(ENDPOINT, ACCESS_KEY_ID, SECRET_ACCESS_KEY_ID);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}

		String requestUrl = null;

		Map<String, String> params = new HashMap<String, String>();
		params.put("Service", "AWSECommerceService");
		params.put("AssociateTag", ASSOCIATE_TAG);
		params.put("Version", "2013-08-01");
		params.put("Operation", "ItemLookup");
		params.put("IdType", "ASIN");
		params.put("ItemId", asin);
		params.put("ResponseGroup", responseGroup);
		
		requestUrl = helper.sign(params);
		boolean successRequest = false;
		while (!successRequest) {
			try {
	    		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
	            DocumentBuilder db = dbf.newDocumentBuilder();
	            doc = db.parse(requestUrl);
	            successRequest = true;
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		return doc;
	}
	
	
	
	
	
	
	
}
