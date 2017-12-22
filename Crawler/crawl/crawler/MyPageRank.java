//package edu.upenn.cis.cis455.crawler;
//
///*
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements.See the NOTICE file distributed with
// * this work for additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License.You may obtain a copy of the License at
// *
// *http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//import java.io.BufferedWriter;
//import java.io.File;
//import java.io.FileWriter;
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.Date;
//import java.util.List;
//import java.util.Random;
//import java.util.Iterator;
//import java.util.regex.Pattern;
//
//import scala.Tuple2;
//
//import com.amazonaws.auth.AWSCredentialsProvider;
//import com.amazonaws.auth.InstanceProfileCredentialsProvider;
//import com.amazonaws.services.s3.AmazonS3Client;
//import com.google.common.collect.Iterables;
//
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.api.java.function.Function;
//import org.apache.spark.api.java.function.Function2;
//import org.apache.spark.api.java.function.PairFlatMapFunction;
//import org.apache.spark.api.java.function.PairFunction;
//import org.apache.spark.sql.SparkSession;
//
//import java.sql.Connection;
//import java.sql.DriverManager;
//import java.sql.ResultSet;
//import java.sql.SQLException;
//import java.sql.Statement;
//
//import org.apache.log4j.Logger;
//import org.apache.log4j.Level;
//
//
//
////import oracle.jdbc.driver.OracleDriver;
//
///**
// * Computes the PageRank of URLs from an input file. Input file should
// * be in format of:
// * URL neighbor URL
// * URL neighbor URL
// * URL neighbor URL
// * ...
// * where URL and their neighbors are separated by space(s).
// *
// * This is an example implementation for learning how to use Spark. For more conventional use,
// * please refer to org.apache.spark.graphx.lib.PageRank
// *
// * Example Usage:
// * <pre>
// * bin/run-example JavaPageRank data/mllib/pagerank_data.txt 10
// * </pre>
// */
//public class MyPageRank 
//{
//	private static final Double DIFTHRESHOLD = 1E-2;
//	private static final Pattern SPACES = Pattern.compile("\\s+");
//	
//	private static AWSCredentialsProvider s3Credentials;
//	private static AmazonS3Client s3Client;
//
//
//	public static void main(String[] args) throws Exception 
//	{
////		if (args.length < 2) 
////		{
////			System.err.println("Usage: JavaPageRank <file> <number_of_iterations>");
////			System.exit(1);
////		}
//		
////		getWeight("test2");
////		setTFIDF("Google", 1, 2.2);
////		setDOCID(1, "www.google.com");
////		System.exit(0);
////		
//		Logger.getLogger("org").setLevel(Level.OFF);
//		Logger.getLogger("akka").setLevel(Level.OFF);
//		int MAXCOUNT = 50;
//		
////		showWarning();
//		
//		boolean dev = true;
//		String sourceFile;
//		JavaSparkContext sc;
//		if(dev)
//		{
//			SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("PageRank");
//	//		SparkConf conf = new SparkConf().setMaster("yarn-client").setAppName("PageRank");
//			sc = new JavaSparkContext(conf);
//
//		
//			s3Credentials = new InstanceProfileCredentialsProvider();
//			s3Client = new AmazonS3Client(s3Credentials);
//			
//			sourceFile = "s3a://testbucket-555-3/PageRank_newest.txt";
//			
//			sc.hadoopConfiguration().set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
//			sc.hadoopConfiguration().set("fs.s3.awsAccessKeyId", s3Credentials.getCredentials().getAWSAccessKeyId());
//			sc.hadoopConfiguration().set("fs.s3.awsSecretAccessKey", s3Credentials.getCredentials().getAWSSecretKey());
//			
//		}
//		else
//		{
//			SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("PageRank");
////			SparkConf conf = new SparkConf().setMaster("yarn-client").setAppName("PageRank");
//			sc = new JavaSparkContext(conf);
//			sourceFile = "files/PageRank_newest.txt";
//		}
//		
//		//SparkConf sparkConf = new SparkConf().setAppName("JavaPageRank").setMaster("spark://localhost:7077");
//		
////		SparkSession spark = SparkSession
////			.builder()
////			.appName("JavaPageRank")
////			.getOrCreate();
//		
//
//		System.out.println("Start Generating Files");
////		generateFile(args[0]);
//		System.out.println("Finished Generating Files");
//		
//		// Loads in input file. It should be in format of:
//		// URL neighbor URL
//		// URL neighbor URL
//		// URL neighbor URL
//		
//		Date start = new Date();
//		
////		JavaRDD<String> lines = spark.read().textFile(sourceFile).javaRDD();
//		JavaRDD<String> lines = sc.textFile(sourceFile);
//		
//		//System.out.println(lines.first());
//		JavaPairRDD<String, String> invLinks = lines.mapToPair(
//				new PairFunction<String, String, String>()
//				{
//					@Override
//					public Tuple2<String, String> call(String s) 
//					{
//						
//						String[] parts = SPACES.split(s);
//						return new Tuple2<String, String>(parts[1].replace("'", "%27"), parts[0].replace("'", "%27"));
//					}
//				}
//				);
//		// Loads all URLs from input file and initialize their neighbors.
//		JavaPairRDD<String, Iterable<String>> links = lines.mapToPair(
//			new PairFunction<String, String, String>() 
//			{
//				@Override
//				public Tuple2<String, String> call(String s) 
//				{
//					
//					String[] parts = SPACES.split(s);
//				//	System.out.println("lines: " + parts[1]);
//					return new Tuple2<String, String>(parts[0].replace("'", "%27"), parts[1].replace("'", "%27"));
//				}
//			}
//			).distinct().groupByKey().cache();
//		
//		JavaRDD<String> orgNodes = links.keys().distinct();
//		JavaRDD<String> destNodes = lines.map(
//				new Function<String, String>()
//				{
//
//					@Override
//					public String call(String arg0) 
//					{
//						// TODO Auto-generated method stub
//						String[] parts = SPACES.split(arg0);
//						return parts[1].replace("'", "%27");
//					}
//					
//				}
//				).distinct();
//		JavaRDD<String> danglingLinks = destNodes.subtract(orgNodes);
//		
//		System.out.println("DANGLING LINKS:" + danglingLinks.first());
//		System.out.println("ORGURLNUMS: " + orgNodes.count());
//		System.out.println("DESTURLNUMS: " + destNodes.count());
//		
//
//		JavaPairRDD<String, Double> tempDanglingLinkPair = danglingLinks.mapToPair(
//					new PairFunction<String, String, Double>()
//					{
//						@Override
//						public Tuple2<String, Double> call(String arg0) throws Exception
//						{
//							// TODO Auto-generated method stub
//							return new Tuple2<String, Double>(arg0, 1.0);
//						}
//					}
//				);
//		
//		JavaPairRDD<String, String> subInvLinks = invLinks.subtractByKey(tempDanglingLinkPair);
//		JavaPairRDD<String, Iterable<String>> processedLinks = subInvLinks.mapToPair(
//				new PairFunction<Tuple2<String, String>, String, String>()
//				{
//					@Override
//					public Tuple2<String, String> call(Tuple2<String, String> arg0) throws Exception
//					{
//						// TODO Auto-generated method stub
//						return new Tuple2<String, String> (arg0._2(), arg0._1());
//					}
//				}
//				).distinct().groupByKey();
////		JavaPairRDD<String, Iterable<String>>  circleLinkes = links.subtractByKey(tempDanglingLinkPair);
//		JavaPairRDD<String, Iterable<String>> wholeLinks = links;
//		links = processedLinks;
//		
//		// Loads all URLs with other URL(s) link to from input file and i-nitialize ranks of them to one.
//		JavaPairRDD<String, Double> ranks = links.mapValues(
//				new Function<Iterable<String>, Double>() 
//				{
//					@Override
//					public Double call(Iterable<String> rs) 
//					{
//						return 1.0;
//					}
//				}
//				);
//		
//		System.out.println("LINKS: " + links.first());
//		System.out.println("RANKS: " + ranks.first());
//		
//		JavaPairRDD<String, Double> dif;
//		JavaPairRDD<String, Double> orgRanks;
//		
//		// Calculates and updates URL ranks continuously using PageRank algorithm.
//		for (int current = 0; current < MAXCOUNT; current++) 
//		{
//		// Calculates URL contributions to the rank of other URLs.
//		//	System.out.println(links.join(ranks).first());
//			JavaPairRDD<String, Double> contribs = links.join(ranks).values().flatMapToPair(
//					new PairFlatMapFunction<Tuple2<Iterable<String>, Double>, String, Double>() 
//					{
//						@Override
//						public Iterator<Tuple2<String, Double>> call(Tuple2<Iterable<String>, Double> s) 
//						{
//							int urlCount = Iterables.size(s._1);
//							List<Tuple2<String, Double>> results = new ArrayList<Tuple2<String, Double>>();
//							for (String n : s._1) 
//							{
//								results.add(new Tuple2<String, Double>(n, s._2() / urlCount));
//							}
////							System.out.println("Results: " + s);
////							System.out.println("Results: " + results.toString());
//							return results.iterator();
//					 	}
//					}
//					);
//			
//		//	System.out.println("Contribs:" + contribs.first());
//			
//				// Re-calculates URL ranks based on neighbor contributions.
//			orgRanks = ranks;
//			
//			ranks = contribs.reduceByKey(new Sum()).mapValues(
//					new Function<Double, Double>() 
//					{
//						@Override
//						public Double call(Double sum) 
//						{
//						//	return sum ;
//							return 0.15 + sum * 0.85;
//						}
//					}
//					);
//			
////			System.out.println("DIF: " + ranks.join(orgRanks).first());
//			
//			dif = ranks.join(orgRanks).mapToPair(
//					new PairFunction<Tuple2<String, Tuple2<Double, Double>>, String, Double>()
//					{
//						@Override
//						public Tuple2<String, Double> call(Tuple2<String, Tuple2<Double, Double>> arg0)
//						{
//							Double tempDif = Math.abs(arg0._2()._1() - arg0._2()._2());
//
////							System.out.println("TupleDif: " + arg0._1() + tempDif);
//							return new Tuple2<String, Double> (arg0._1(), tempDif);
//						}
//					}
//					);
//			
////			System.out.println(dif.first());
//			
//			long difSum = dif.filter(
//					new Function<Tuple2<String, Double>, Boolean>()
//					{
//						@Override
//						public Boolean call(Tuple2<String, Double> arg0) throws Exception
//						{
//							return arg0._2() > DIFTHRESHOLD;
//						}
//					}
//					).count();
//			
//			System.out.println("DIFSUM: " + difSum + " ITERATION: " + current);
//			
//			if(difSum == 0 && current > 2)
//			{
//				Date end = new Date();
//				long period = end.getTime() - start.getTime();
//				System.out.println("BREAK! Time: " + period);
//				break;
//				
//			}
//		//		System.out.println("Ranks:" + ranks.first());
//		}
//		
//		// Do this for all links (Dangling Links)
//		JavaPairRDD<String, Double> finalContribs = wholeLinks.join(ranks).values().flatMapToPair(
//				new PairFlatMapFunction<Tuple2<Iterable<String>, Double>, String, Double>() 
//				{
//					@Override
//					public Iterator<Tuple2<String, Double>> call(Tuple2<Iterable<String>, Double> s) 
//					{
//						int urlCount = Iterables.size(s._1);
//						List<Tuple2<String, Double>> results = new ArrayList<Tuple2<String, Double>>();
//						for (String n : s._1) 
//						{
//							results.add(new Tuple2<String, Double>(n, s._2() / urlCount));
//						}
////						System.out.println("Final Results: " + s);
////						System.out.println("Results: " + results.toString());
//						return results.iterator();
//				 	}
//				}
//				);
//		
//		JavaPairRDD<String, Double> danglingRanks = finalContribs.reduceByKey(new Sum()).mapValues(
//				new Function<Double, Double>() 
//				{
//					@Override
//					public Double call(Double sum) 
//					{
//					//	return sum ;
//						return 0.15 + sum * 0.85;
//					}
//				}
//				);
//		
//		JavaPairRDD<String, Double> finalRanks = ranks.union(danglingRanks.subtractByKey(ranks));
//		
//			// Collects all URL ranks and dump them to console.
//		
//		Class.forName("oracle.jdbc.driver.OracleDriver");
//		Connection conn;
//		Statement stat = null;
//		ResultSet rs = null;
//		String hostName = "mydbinstance.ciynpeqwoevt.us-east-1.rds.amazonaws.com";
//		String user = "chenleshang";
//		String password = "chenleshang";
//		String database = "ORCL";
//		
//		conn = DriverManager.getConnection("jdbc:oracle:thin:@//" + hostName
//				+ "/" + database, user, password);
//		stat = conn.createStatement();
//
//		String insert = "";
//		List<Tuple2<String, Double>> output = finalRanks.collect();
//		int count = 0;
//		for (Tuple2<?,?> tuple : output) 
//		{
////			System.out.println(tuple._1() + " has rank: " + tuple._2() + ".");
//			insert = "INSERT INTO PAGERANK(url, weight) VALUES ('" + tuple._1() + "', " + tuple._2() + ")";
//			System.out.println(insert);
//			rs = stat.executeQuery(insert);
//			count += 1;
////			if(count % 100 == 0)
////			{
////				System.out.println("COUNT: " + count + "; " + insert);
////			}
//		}
//		rs.close();
//		conn.close();
//	
////		spark.stop();
//		sc.stop();
//	}
//	
//	static void showWarning() 
//	{
//		String warning = "WARN: This is a naive implementation of PageRank " +
//		"and is given as an example! \n" +
//		"Please use the PageRank implementation found in " +
//		"org.apache.spark.graphx.lib.PageRank for more conventional use.";
//		System.err.println(warning);
//	}
//
//	private static class Sum implements Function2<Double, Double, Double> 
//	{
//		@Override
//		public Double call(Double a, Double b) 
//		{
//			return a + b;
//		}
//	}
//	
//	private static class Dif implements Function2<Double, Double, Double>
//	{
//		@Override
//		public Double call(Double a, Double b)
//		{
//			return a - b;
//		}
//	}
//	
//	private static class DifTuple implements Function<Tuple2<Double, Double>, Double>
//	{
//
//		@Override
//		public Double call(Tuple2<Double, Double> arg0) throws Exception
//		{
//			return arg0._1() - arg0._2();
//		}
//
//	}
//	
//	private static void getWeight(String url)
//	{
//		Connection conn;
//		Statement mystat = null;
//		ResultSet myrs = null;
//		String hostName = "mydbinstance.ciynpeqwoevt.us-east-1.rds.amazonaws.com:1521";
//		String user = "chenleshang";
//		String password = "chenleshang";
//		String database = "ORCL"; 
//		//登记JDBC驱动程序
////		String url = "";
//		String weight = "";
////		Double weight;
//
//		try 
//		{
//			Class.forName("oracle.jdbc.driver.OracleDriver");
//			conn = DriverManager.getConnection("jdbc:oracle:thin:@//" + hostName
//					+ "/" + database, user, password);
//			mystat = conn.createStatement();
//			String sql = "select * from PAGERANK where url='" +
//					url + 
//					"' and weight= '" +
//					weight + "'";
//			String sql1 = "select * from PAGERANK where url='" +
//					url + 
//					"'";
//			String insert = "INSERT INTO PAGERANK(url, weight) VALUES ('" + url + "', " + "1.2" + ")";
//			System.out.println(sql1);
//			//INSERT INTO "CHENLESHANG"."PAGERANK" (URL, WEIGHT) VALUES ('test', '3.1415926')
//			myrs = mystat.executeQuery(sql1);
//			
////			rs = stat.executeQuery(sql1);
////			
//			if(myrs.next())
//			{
//				System.out.println("url: " + myrs.getString("url"));
//				System.out.println("weight: " + myrs.getString("weight"));
//			}
//			
//			myrs.close();
//			conn.close();
//		} 
//		catch (Exception e)
//		{
////			System.out.print("Class Not Found Exception");
//			e.printStackTrace();
//		}
//	}
//	
//	private static void setTFIDF(String url, int id, Double score)
//	{
//		Connection conn;
//		Statement mystat = null;
//		ResultSet myrs = null;
//		String hostName = "mydbinstance.ciynpeqwoevt.us-east-1.rds.amazonaws.com:1521";
//		String user = "chenleshang";
//		String password = "chenleshang";
//		String database = "ORCL"; 
//
//		try 
//		{
//			Class.forName("oracle.jdbc.driver.OracleDriver");
//			conn = DriverManager.getConnection("jdbc:oracle:thin:@//" + hostName
//					+ "/" + database, user, password);
//			mystat = conn.createStatement();
//
//			String insert = "INSERT INTO TFIDF(URL, DOCID, SCORE) VALUES ('" + url + "', " + id + "," + score + ")";
//			System.out.println(insert);
//			myrs = mystat.executeQuery(insert);
//
//			myrs.close();
//			conn.close();
//		} 
//		catch (Exception e)
//		{
//			e.printStackTrace();
//		}
//	}
//	
//
//	private static void setLINKS(String url1, String url2)
//	{
//		Connection conn;
//		Statement mystat = null;
//		ResultSet myrs = null;
//		String hostName = "mydbinstance.ciynpeqwoevt.us-east-1.rds.amazonaws.com:1521";
//		String user = "chenleshang";
//		String password = "chenleshang";
//		String database = "ORCL"; 
//
//		try 
//		{
//			Class.forName("oracle.jdbc.driver.OracleDriver");
//			conn = DriverManager.getConnection("jdbc:oracle:thin:@//" + hostName
//					+ "/" + database, user, password);
//			mystat = conn.createStatement();
//
//			String insert = "INSERT INTO LINKS(URL1, URL2) VALUES ('" + url1 + "', '" + url2+ "')";
//			System.out.println(insert);
//			myrs = mystat.executeQuery(insert);
//
//			myrs.close();
//			conn.close();
//		} 
//		catch (Exception e)
//		{
//			e.printStackTrace();
//		}
//	}
//
//	private static void setDOCID(int id, String url)
//	{
//		Connection conn;
//		Statement mystat = null;
//		ResultSet myrs = null;
//		String hostName = "mydbinstance.ciynpeqwoevt.us-east-1.rds.amazonaws.com:1521";
//		String user = "chenleshang";
//		String password = "chenleshang";
//		String database = "ORCL"; 
//
//		try 
//		{
//			Class.forName("oracle.jdbc.driver.OracleDriver");
//			conn = DriverManager.getConnection("jdbc:oracle:thin:@//" + hostName
//					+ "/" + database, user, password);
//			mystat = conn.createStatement();
//
//			String insert = "INSERT INTO DOCID(ID, URL) VALUES (" + id + ", '" + url + "')";
//			System.out.println(insert);
//			myrs = mystat.executeQuery(insert);
//
//			myrs.close();
//			conn.close();
//		} 
//		catch (Exception e)
//		{
//			e.printStackTrace();
//		}
//	}
//	
//	public static void generateFile(String path)
//	{
//		try
//		{
//			File outputFile = new File(path);//.replaceAll("//", "/")
//			if(!outputFile.exists())
//			{
//				//outputFile.
//				outputFile.createNewFile();
//			}
//			BufferedWriter out = new BufferedWriter(new FileWriter(outputFile));
//			String content = "";
//			
//			Random rand = new Random();
//			
//			for( int i = 0; i < 10000000; i ++)
//			{
//				int source = rand.nextInt(1000000);
//				int dest = rand.nextInt(1000000);
//				content = source + " " + dest + "\n";
//				out.write(content);
//			}
//			out.close();
//		} 
//		catch (IOException e)
//		{
//			e.printStackTrace();
//		}
//	}
//}
