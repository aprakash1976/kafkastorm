package com.clarity.storm.kafkastorm;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogRegExp {
	
	// number of fields that should be in a line
	public static final int NUM_FIELDS = 9;

	// Sample log entry
	public static final String logEntryLine =
	 //"123.45.67.89 - - [27/Oct/2009:09:27:09 -0400] \"GET /java/java examples.html HTTP/1.0\" 200 10450 \"-\" \"Mozilla/4.6 [en] (X11; U; OpenBSD 2.8 i586; Nav)\"";
			//"10.1.20.210 - - [29/Sep/2014:07:37:32 -0500] \"GET /icons/powered_by_rh.png HTTP/1.1\" 404 298 \"http://10.1.10.162/\" \"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2062.124 Safari/537.36\"";
			//"10.1.20.114 - - [30/Sep/2014:05:19:58 -0500] \"GET / HTTP/1.1\" 403 495 \"-\" \"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2062.124 Safari/537.36";
			"10.1.20.114 - - [30/Sep/2014:05:19:58 -0500] \"GET / HTTP/1.1\" 403 4958 \"-\" \"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2062.124 Safari/537.36";
	public static void main(String argv[]) {

		//String logEntryLine = "";
		String logEntryPattern = "^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\d+) \"([^\"]+)\" (.+)";
		//String logEntryPattern = "([0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+)[^[]+\\[([^]]+)\\][^/]+([^ ]+).+";
		//String logEntryPattern = "^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\d+) \" (.+)\" \"([^\"]+)\"";
		Pattern p = Pattern.compile(logEntryPattern);
		Matcher matcher = null;

		//try {
			//BufferedReader in = new BufferedReader(new FileReader("access_log"));
			//while ((logEntryLine = in.readLine()) != null) {
				try {
					addClasspath(new File("hbase-site.xml"));
					Configuration conf = HBaseConfiguration.create();
					System.out.println(String.format(
							"Initializing connection to HBase table %s at %s", "apache_access_log",
							conf.get("hbase.rootdir")));
					HTable table = new HTable(conf, "apache_access_log");
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				matcher = p.matcher(logEntryLine);
				matcher.find();
				System.out.println(matcher.matches());

				if (!matcher.matches() || NUM_FIELDS != matcher.groupCount()) {
					System.err.println("Problem with Expression Parsing");
					return;
				}

				System.out.println("IP Address: " + matcher.group(1));
				System.out.println("Date&Time: " + matcher.group(4));
				System.out.println("Request: " + matcher.group(5));
				System.out.println("Response: " + matcher.group(6));
				System.out.println("Bytes Sent: " + matcher.group(7));

				if (!matcher.group(8).equals("-"))
					System.out.println("Referer: " + matcher.group(8));
				else 
					System.out.println("Referer: " + matcher.group(8));

				System.out.println("Browser: " + matcher.group(9));

			//} // end while

		//} // end try

		//catch (FileNotFoundException fne) {
		//	fne.printStackTrace();
		//} catch (IOException ioe) {
		//	ioe.printStackTrace();
		//} catch (Exception e) {
		//	e.printStackTrace();
		//}

	} // end main
	
	private static void addClasspath(File file) throws Exception {
	    Method method = URLClassLoader.class.getDeclaredMethod("addURL", new Class[]{URL.class});
	    method.setAccessible(true);
	    method.invoke(ClassLoader.getSystemClassLoader(), new Object[]{file.toURI().toURL()});
	}

} // end class