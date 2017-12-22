package edu.upenn.cis.cis455.webserver.servlet;

import java.io.StringReader;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.scribe.builder.ServiceBuilder;
import org.scribe.model.OAuthRequest;
import org.scribe.model.Response;
import org.scribe.model.Token;
import org.scribe.model.Verb;
import org.scribe.oauth.OAuthService;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;

/*
 * Helper class based off of the YelpAPI example 
 * found @https://github.com/Yelp/yelp-api/blob/master/v2/java/YelpAPI.java
 */
public class YelpSearchHelper {
	private static final String CONSUMER_KEY = "gwsaUokgPX9ldzrFGRorvg";
	private static final String CONSUMER_SECRET = "ZIOkC0fzxfVLWASzSCVjN_d5mpU";
	private static final String TOKEN = "D-WTIvdSKkkhS9J_6Nhgg5dW0Wo2cvbr";
	private static final String TOKEN_SECRET = "ZSlNjAS05NjLs5w-YIlw4EGn0NE";
	private static final String API_HOST = "api.yelp.com";
	private static final int SEARCH_LIMIT = 3;
	private static final String SEARCH_PATH = "/v2/search";
	private static final String BUSINESS_PATH = "/v2/business";

	OAuthService service;
	Token accessToken;
	YelpAPI yelp;

	public YelpSearchHelper() {
		yelp = new YelpAPI(CONSUMER_KEY, CONSUMER_SECRET, TOKEN, TOKEN_SECRET);
	}

	public void query(String term, String location) {
		String searchJSON = yelp.searchForBusinessesByLocation(term, location);
		JSONParser parser = new JSONParser();
		JSONObject response = null;
		JSONObject businessResponse = null;
		
		try {
			response = (JSONObject) parser.parse(searchJSON);
		} catch (ParseException pe) {
			System.out.println("Error: could not parse JSON response:");
		}
		
		if (response != null) {
			JSONArray businesses = (JSONArray) response.get("businesses");
			JSONObject firstBusiness = (JSONObject) businesses.get(0);
			String firstBusinessID = firstBusiness.get("id").toString();
			System.out.println(String.format("%s businesses found, querying business info for the top result \"%s\" ...",
					businesses.size(), firstBusinessID));

			// Select the first business and display business details
			String businessResponseJSON = yelp.searchByBusinessId(firstBusinessID.toString());
			try {
				businessResponse = (JSONObject) parser.parse(businessResponseJSON);
			} catch (ParseException pe) {
				System.out.println("Error: could not parse JSON response");
			}
			if (businessResponse != null) {
				try {
					Double rating = (Double) businessResponse.get("rating");
					System.out.println("Rating: " + rating);
					
				} catch (Exception e) {
					e.printStackTrace();
				}
				
			}

		}
		
	}

}
