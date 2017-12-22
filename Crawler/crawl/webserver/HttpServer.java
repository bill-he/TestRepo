package edu.upenn.cis.cis455.webserver;

import java.io.File;

import org.apache.log4j.Logger;

public class HttpServer {

	/**
	 * Logger for this particular class
	 */
	static Logger log = Logger.getLogger(HttpServer.class);
	static int num_threads = 5;
	
	private static myServer initializeServer(int p, String dir, String web){
		return new myServer(p, dir, web);
	}

	public static void main(String args[]) throws InterruptedException
	{
		log.info("Start of Http Server");
		
		/* 
		 * port = args[1]
		 * root_dir = args[2]
		 * */
		
		if(args.length == 0){ 
			System.out.println("Full name: Steven Hwang\nSEAS login: stevenhw");
			System.exit(0);
		}
		else if(args.length == 2){ // for MS2
			log.info("Args:______");
			log.info(args[0]);
			log.info(args[1]);
			log.info("End Args____\n");
			
			String web_xml_path = "conf/web.xml";
			File web_xml_file = new File(web_xml_path);
			
			if(web_xml_file.exists() == false){
				log.error("Can't find the web.xml file... :(");
				System.exit(1);
			}
			
			myServer server = initializeServer(Integer.parseInt(args[0]), args[1], web_xml_path);

			server.start();

			server.join();
		}else{
			System.out.println("Invalid number of args");
			System.exit(0);
		}
		log.info("Http Server terminating");
	}

}
