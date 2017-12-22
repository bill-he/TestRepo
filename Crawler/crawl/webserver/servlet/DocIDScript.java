package edu.upenn.cis.cis455.webserver.servlet;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

public class DocIDScript {

	private static Statement mystat = null;
	private static ResultSet myrs = null;
	private static Connection conn = null;
	private static String dbHostName = "newdbinstance.cdc3aijmhcd9.us-east-1.rds.amazonaws.com:1521";
	private static String dbUser = "flyingmantis";
	private static String dbPassword = "flyingmantis";
	private static String database = "ORCL";

	@SuppressWarnings("deprecation")
	public static void main(String[] args) {
		String filePath = args[0];
		File idFile = new File(filePath);
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			conn = DriverManager.getConnection("jdbc:oracle:thin:@//" + dbHostName + "/" + database, dbUser,
					dbPassword);
			mystat = conn.createStatement();

			FileReader fr = new FileReader(idFile);
			BufferedReader br = new BufferedReader(fr);
			String line = "";
			while ((line = br.readLine()) != null) {
				String[] splitLine = line.split(" ");
				String id = splitLine[0];
				String url = splitLine[1];

				System.out.println(id + " " + url);

				if (url.contains("\'")) {
					url = url.replaceAll("\'", "%27");
				}

				String insert = "INSERT INTO DOCID(id, url) VALUES ('" + id + "','" + url + "')";

				myrs = mystat.executeQuery(insert);
			}
			myrs.close();
			conn.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
