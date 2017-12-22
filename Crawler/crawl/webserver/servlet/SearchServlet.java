package edu.upenn.cis.cis455.webserver.servlet;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.w3c.dom.Node;

import com.ECS.client.jax.Item;
import com.ECS.client.jax.ItemAttributes;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

public class SearchServlet extends HttpServlet {

	private final String styleURL = "https://s3.amazonaws.com/frontendresources/style.css";
	private final String logoURL = "https://s3.amazonaws.com/frontendresources/logo.svg";
	private final String noresultsURL = "https://s3.amazonaws.com/frontendresources/noresults.svg";
	public static QueryHelper qh;
	private String location = null;
	private static AWSCredentialsProvider s3Credentials = new InstanceProfileCredentialsProvider(true);
	private static AmazonS3Client s3Client = new AmazonS3Client(s3Credentials);

	public void init(ServletConfig config) throws ServletException {
		qh = new QueryHelper();
		super.init(config);
	}

	public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
		response.setContentType("text/html;charset=UTF-8");
		PrintWriter out = response.getWriter();
		HttpSession session = request.getSession();
		Cookie[] cookies = request.getCookies();
		Cookie geoCookie = getCookie(cookies, "location");
		if (geoCookie != null) {
			location = geoCookie.getValue();
		}
		/*
		 * html header with dependencies
		 */
		out.println("<!DOCTYPE html><html><head><title>Flying Mantis Search Engine</title>");
		out.println(
				"<link rel=\"stylesheet\" href=\"https://maxcdn.bootstrapcdn.com/bootstrap/4.0.0-alpha.5/css/bootstrap.min.css\" integrity=\"sha384-AysaV+vQoT3kOAXZkl02PThvDr8HYKPZhNT5h/CXfBThSRXQ6jW5DO2ekP5ViFdi\" crossorigin=\"anonymous\">");
		out.println("<link rel=\"stylesheet\" href=\"" + styleURL + "\" />");
		out.println("<script src=\"https://s3.amazonaws.com/frontendresources/pace.min.js\"></script>");
		out.println("<link href=\"https://fonts.googleapis.com/css?family=Roboto\" rel=\"stylesheet\">");
		out.println("<link href=\"https://fonts.googleapis.com/css?family=Indie+Flower\" rel=\"stylesheet\">");
		out.println(
				"<link rel=\"stylesheet\" href=\"https://maxcdn.bootstrapcdn.com/font-awesome/4.7.0/css/font-awesome.min.css\" />");
		out.println(
				"<script src=\"https://ajax.googleapis.com/ajax/libs/jquery/3.1.1/jquery.min.js\" integrity=\"sha384-3ceskX3iaEnIogmQchP8opvBy3Mi7Ce34nWjpBIwVTHfGYWQS9jwHDVRnpKKHJg7\" crossorigin=\"anonymous\"></script>");
		out.println(
				"<script src=\"https://cdnjs.cloudflare.com/ajax/libs/tether/1.3.7/js/tether.min.js\" integrity=\"sha384-XTs3FgkjiBgo8qjEjBk0tGmf3wPrWtA6coPfQDfFEY8AnYJwjalXCiosYRBIBZX8\" crossorigin=\"anonymous\"></script>");
		out.println(
				"<script src=\"https://maxcdn.bootstrapcdn.com/bootstrap/4.0.0-alpha.5/js/bootstrap.min.js\" integrity=\"sha384-BLiI7JTZm+JWlgKa0M0kGRpJbF2J8q+qreVrKBC47e3K6BW78kGLrCkeRX6I9RoK\" crossorigin=\"anonymous\"></script>");
		out.println("<script src=\"https://s3.amazonaws.com/frontendresources/search.js\"></script>");
		out.println("</head><body>");
		// html body
		out.println("<div class=\"container search\"><div class=\"row logo\">");
		out.println("Flying <img src=\"" + logoURL + "\" style=\"width: 15%; height: 15%\"/>Mantis</div>");
		out.println("<div class=\"row\"><div class=\"col-md-6 offset-md-3\">");

		out.println("<form method=\"post\" class=\"form-inline\" id =\"searchForm\">");

		out.println("<input type=\"text\" name=\"query\" id=\"searchBar\"/>");
		out.println("<div class=\"col-md-6 offset-md-3\">");
		out.println(
				"<button class=\"btn\" type=\"submit\" id=\"searchButton\">Search</button></div></form></div></div>");

		out.println("</div></body></html>");

	}

	public void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {
		response.setContentType("text/html;charset=UTF-8");
		PrintWriter out = response.getWriter();
		HttpSession session = request.getSession();

		String query = request.getParameter("query");
		String load = request.getParameter("load");
		String temp = request.getParameter("temp");
		LinkedList<String> nextUrls = new LinkedList<String>();
		long initTime = Calendar.getInstance().getTimeInMillis();

		if (query == null && load != null && temp != null) {
			out.println("<div class=\"row\" id=\"searchHeader\">");
			out.println("<div class=\"col-md-2\"><div class=\"logo\">");
			out.println("Flying <img src=\"" + logoURL + "\" style=\"width: 30%; height: 30%\"/>Mantis</div></div>");
			out.println("<div class=\"col-md-6\">");
			out.println("<form method=\"post\" class=\"form-inline\">");
			out.println("<input type=\"text\" name=\"query\" id=\"searchBar\" value=\"" + temp + "\"/>");
			out.println("<button class=\"btn\" type=\"submit\" id=\"searchButton\" >Search</button>");
			out.println("</form><p style=\"color: #9c9e9b\">Press enter to search</p></div></div>");
			out.println("<div class=\"row\" id=\"results\"></body></html>");
		} else {

			/*
			 * html header with dependencies
			 */
			out.println("<!DOCTYPE html><html><head><title>Flying Mantis Search Engine</title>");
			out.println(
					"<link rel=\"stylesheet\" href=\"https://maxcdn.bootstrapcdn.com/bootstrap/4.0.0-alpha.5/css/bootstrap.min.css\" integrity=\"sha384-AysaV+vQoT3kOAXZkl02PThvDr8HYKPZhNT5h/CXfBThSRXQ6jW5DO2ekP5ViFdi\" crossorigin=\"anonymous\">");
			out.println("<link rel=\"stylesheet\" href=\"" + styleURL + "\" />");
			out.println("<script src=\"https://s3.amazonaws.com/frontendresources/pace.min.js\"></script>");
			out.println("<link href=\"https://fonts.googleapis.com/css?family=Roboto\" rel=\"stylesheet\">");
			out.println("<link href=\"https://fonts.googleapis.com/css?family=Indie+Flower\" rel=\"stylesheet\">");
			out.println(
					"<link rel=\"stylesheet\" href=\"https://maxcdn.bootstrapcdn.com/font-awesome/4.7.0/css/font-awesome.min.css\" />");
			out.println(
					"<script src=\"https://ajax.googleapis.com/ajax/libs/jquery/3.1.1/jquery.min.js\" integrity=\"sha384-3ceskX3iaEnIogmQchP8opvBy3Mi7Ce34nWjpBIwVTHfGYWQS9jwHDVRnpKKHJg7\" crossorigin=\"anonymous\"></script>");
			out.println(
					"<script src=\"https://cdnjs.cloudflare.com/ajax/libs/tether/1.3.7/js/tether.min.js\" integrity=\"sha384-XTs3FgkjiBgo8qjEjBk0tGmf3wPrWtA6coPfQDfFEY8AnYJwjalXCiosYRBIBZX8\" crossorigin=\"anonymous\"></script>");
			out.println(
					"<script src=\"https://maxcdn.bootstrapcdn.com/bootstrap/4.0.0-alpha.5/js/bootstrap.min.js\" integrity=\"sha384-BLiI7JTZm+JWlgKa0M0kGRpJbF2J8q+qreVrKBC47e3K6BW78kGLrCkeRX6I9RoK\" crossorigin=\"anonymous\"></script>");
			out.println("<script src=\"https://s3.amazonaws.com/frontendresources/search.js\"></script>");
			out.println("</head><body>");
			// html body
			out.println("<div class=\"container search post\"><div class=\"row\" id=\"searchHeader\">");
			out.println("<div class=\"col-md-2\"><div class=\"logo\">");
			out.println("Flying <img src=\"" + logoURL + "\" style=\"width: 30%; height: 30%\"/>Mantis</div></div>");
			out.println("<div class=\"col-md-6\">");
			out.println("<form method=\"post\" class=\"form-inline\">");
			out.println("<input type=\"text\" name=\"query\" id=\"searchBar\" value=\"" + query + "\"/>");
			out.println("<button class=\"btn\" type=\"submit\" id=\"searchButton\" >Search</button>");
			out.println("</form></div></div>");
			/*
			 * TODO 1. Display top 10 pages
			 */

			LinkedList<String> queryRanks;
			List<String> queryTerms = new LinkedList<String>();
			String[] splitQuery = query.split(" ");
			for (String s : splitQuery) {
				s = s.toLowerCase();
				s = Stemmer2.stemString(s);
				queryTerms.add(s);
			}

			synchronized (qh) {
				qh.setQueryTable(queryTerms);
				queryRanks = qh.getRanks();
				qh.clearQueryTable();
			}

			int numResults = queryRanks.size();

			AmazonSearchHelper as = new AmazonSearchHelper(query);
			ArrayList<Item> amazonItems = as.search();
			

			long finishTime = Calendar.getInstance().getTimeInMillis();
			long deltaT = finishTime - initTime;
			double seconds = deltaT / 1000.0;

			if (queryRanks.isEmpty()) {
				out.println("<div class=\"row\" id=\"results\">");
				out.println("<p>Your search <strong>" + query + "</strong> did not yield any results (");
				out.println(seconds + " seconds)</p>");
				out.println("<div class=\"col-md-8\"><p>Suggestions:</p><ul>");
				out.println("<li>Make sure all words are spelled correctly.</li>");
				out.println("<li>Try different keywords.</li>");
				out.println("<li>Try more general keywords.</li>");
				out.println("<li>Try fewer keywords.</li>");
				out.println("<li>Or use google T.T</li></ul></div>");
				if (!amazonItems.isEmpty()) {
					out.println("<div class=\"col-md-4\"><p>But here are some results on Amazon!</p>");
					Item tempItem;
					for (int i = 0; i < amazonItems.size(); i++) {
						tempItem = amazonItems.get(i);
						String itemHTML = formatItem(tempItem);
						if (itemHTML != null) {
							out.println(itemHTML);
						}
					}
				} else {
					out.println("</div>");
				}

				out.println("</div></div><div class=\"row\"><img src=\"" + noresultsURL
						+ "\" style=\"width:50%; height:50%\">");
			} else {
				out.println("<div class=\"row\" id=\"results\"><p style=\"color: #9c9e9b\">About " + numResults
						+ " results (" + seconds + " seconds)</p>");

				out.println("<div class=\"col-md-8\">");
				int maxSize = 10;
				if (queryRanks.size() < 10)
					maxSize = queryRanks.size();
				for (int i = 0; i < maxSize; i++) {
					String url = queryRanks.removeFirst();

					String[] meta = getMetaData(url, qh);

					out.println("<div class=\"result\">");
					out.println("<a href=\"" + url + "\">" + meta[0] + "</a>");
					out.println("<h6><a style=\"color:#9c9e9b !important\"href=\"" + url + "\">" + url + "</a></h6>");
					out.println("<h6>" + meta[1] + "</h6></div>");

				}

				out.println("</div>");
				
				if (!amazonItems.isEmpty()) {
					out.println("<div class=\"col-md-4\"><p>Here are some results on Amazon:</p>");
					Item tempItem;
					for (int i = 0; i < amazonItems.size(); i++) {
						tempItem = amazonItems.get(i);
						String itemHTML = formatItem(tempItem);
						if (itemHTML != null) {
							out.println(itemHTML);
						}
					}
				} else {
					out.println("</div>");
				}
			}

			out.println("</div></body></html>");
		}

	}

	private String formatItem(Item i) {
		ItemAttributes attr = i.getItemAttributes();
		String output = "";
		String url = i.getDetailPageURL();
		String title = attr.getTitle();
		String price = "";
		String imageUrl = "";
		StringBuilder sb = new StringBuilder();
		String asin = i.getASIN();

		org.w3c.dom.Document offersDoc = AmazonSearchHelper.restRequest(asin, "Offers");
		Node priceNode = offersDoc.getElementsByTagName("FormattedPrice").item(0);
		if (priceNode != null) {
			price = priceNode.getTextContent();
		} else {
			price = "$Unknown";
		}

		org.w3c.dom.Document imageDoc = AmazonSearchHelper.restRequest(asin, "Images");
		Node mediumImage = imageDoc.getElementsByTagName("MediumImage").item(0);
		if (mediumImage != null) {
			Node imageNode = mediumImage.getFirstChild();
			if (imageNode != null) {
				imageUrl = imageNode.getTextContent();
			} else {
				imageUrl = null;
			}

			sb.append("<div class=\"amazon result\">");
			sb.append("<h5><a href=\"" + url + "\">" + title + "</a></h5>");
			sb.append("<h6>" + price + "</h6>");
			if (imageUrl == null) {
				sb.append("No image found");
			} else {
				sb.append("<img src=\"" + imageUrl + "\">");
			}
			sb.append("</div>");
			output = sb.toString();
			return output;
		} else {
			return null;
		}

	}

	public String[] getMetaData(String url, QueryHelper qh) {
		String[] metaData = new String[2];
		String id = "" + qh.getDocId(url);
		
		S3Object s3docObject = s3Client.getObject(new GetObjectRequest("mantisdistributedcombined", id));
		
		String title = null;
		String content = null;
		String description = null;
		Document doc;
		try {
			content = getTextInputStream(s3docObject.getObjectContent());
			doc = Jsoup.parse(content);
			title = doc.title();
			if (title == null) {
				title = url;
			}
			description = getMetaTag(doc, "description");
			if (description == null) {
				description = getMetaTag(doc, "og:description");
				if (description == null) {
					description = url;
				}
			}

			metaData[0] = title;
			metaData[1] = description;

			return metaData;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("Could not get metadata");
			metaData[0] = url;
			metaData[1] = "\n";
			return metaData;
		}

	}

	private static String getTextInputStream(InputStream input) throws IOException {
		// Read one text line at a time and display.
		BufferedReader reader = new BufferedReader(new InputStreamReader(input));
		StringBuilder output = new StringBuilder();
		while (true) {
			String line = reader.readLine();
			if (line == null)
				break;
			output.append("    " + line);
		}
		return output.toString();
	}

	String getMetaTag(Document document, String attr) {
		Elements elements = document.select("meta[name=" + attr + "]");
		Element temp;
		for (int i = 0; i < elements.size(); i++) {
			temp = elements.get(i);
			final String s = temp.attr("content");
			if (s != null)
				return s;
		}
		elements = document.select("meta[property=" + attr + "]");
		for (int i = 0; i < elements.size(); i++) {
			temp = elements.get(i);
			final String s = temp.attr("content");
			if (s != null)
				return s;
		}
		return null;
	}

	// checks if cookie exists
	public boolean hasCookie(Cookie[] cookies, String cookieName) {
		for (int i = 0; i < cookies.length; i++) {
			Cookie c = cookies[i];
			if (c.getName().equals(cookieName)) {
				return true;
			}
		}

		return false;
	}

	// gets specified cookie
	public Cookie getCookie(Cookie[] cookies, String cookieName) {
		for (int i = 0; i < cookies.length; i++) {
			Cookie c = cookies[i];
			if (c.getName().equals(cookieName)) {
				return c;
			}
		}
		return null;
	}

}