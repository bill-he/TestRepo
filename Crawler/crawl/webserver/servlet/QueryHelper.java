package edu.upenn.cis.cis455.webserver.servlet;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

public class QueryHelper {
	private Statement mystat = null;
	private ResultSet myrs = null;
	private Connection conn = null;
	private final double DEFAULT_PAGERANK = 0.15;
	private String dbHostName = "newdbinstance.cdc3aijmhcd9.us-east-1.rds.amazonaws.com:1521";
	private String dbUser = "flyingmantis";
	private String dbPassword = "flyingmantis";
	private String database = "ORCL";

	public QueryHelper() {
	}

	// sets the query table on the rdb
	public void setQueryTable(List<String> queryTerms) {

		Statement mystat = null;
		ResultSet myrs = null;

		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			conn = DriverManager.getConnection("jdbc:oracle:thin:@//" + dbHostName + "/" + database, dbUser,
					dbPassword);
			mystat = conn.createStatement();
			for (String q : queryTerms) {
				String insert = "INSERT INTO QUERYTABLE(WORD) VALUES ('" + q + "')";
				myrs = mystat.executeQuery(insert);
			}

				myrs.close();
				conn.close();
			
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	// clears the query table of the rdb
	public void clearQueryTable() {

		Statement mystat = null;
		ResultSet myrs = null;

		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			conn = DriverManager.getConnection("jdbc:oracle:thin:@//" + dbHostName + "/" + database, dbUser,
					dbPassword);
			mystat = conn.createStatement();

			String delete = "DELETE FROM QUERYTABLE";
			myrs = mystat.executeQuery(delete);

			myrs.close();
			conn.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public LinkedList<String> getRanks() {

		Statement mystat = null;
		ResultSet myrs = null;
		LinkedList<String> results = new LinkedList<String>();

		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			conn = DriverManager.getConnection("jdbc:oracle:thin:@//" + dbHostName + "/" + database, dbUser,
					dbPassword);
			mystat = conn.createStatement();

			String query = "select url, sum(score) as rank from (RESULTS "
					+ "inner join QUERYTABLE on results.word = querytable.word)"
					+ " group by url order by rank desc";
			//harmonic mean: (2 * (weight * tfidfscore) / (weight + tfidfscore))
			

			myrs = mystat.executeQuery(query);
			
			while (myrs.next()) {
				results.add( myrs.getString("url"));	
			}
			myrs.close();
			conn.close();
			return results;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return results;

		
	}
	
	public int getDocId(String url) {
		int id = -1;
		Statement mystat = null;
		ResultSet myrs = null;
		
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			conn = DriverManager.getConnection("jdbc:oracle:thin:@//" + dbHostName + "/" + database, dbUser,
					dbPassword);
			mystat = conn.createStatement();

			String query = "select id from DOCID d where d.url ='" + url + "'";

			myrs = mystat.executeQuery(query);

			if (myrs.next()) {
				id = Integer.parseInt(myrs.getString("id"));
			}
			myrs.close();
			conn.close();
			return id;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return id;
	}

}
