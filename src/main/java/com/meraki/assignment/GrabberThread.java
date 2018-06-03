package com.meraki.assignment;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;

/***
 * 
 * Runnable class to grab and store data for a single node.
 * 
 * @author vivek bhandari
 *
 */
public class GrabberThread implements Runnable {
	private int nodeId;
	private DBManager dbManager;
	private long currentTotal;

	public GrabberThread(int nodeId, DBManager dbManager,
			long currentTotal) {
		this.nodeId = nodeId;
		this.dbManager = dbManager;
		this.currentTotal = currentTotal;
	}

	public void run() {
		System.out.println("currentTotal=" + currentTotal);
//		String httpURL ="http://localhost:9000/nodes/"+nodeId+"/usage";
		String httpURL = "http://localhost/mfp/v1/nodes/" + nodeId;
		BufferedReader br = null;
		HttpURLConnection c = null;

		while (true) {
			try {
				// Make request to node
				System.out.println("Running HTTP GET against " + httpURL);
				c = (HttpURLConnection) new java.net.URL(httpURL)
						.openConnection();
				//for this example setting timeouts to infinite. 
				c.setConnectTimeout(0);
				c.setReadTimeout(0);
				//should not be required. but its a safe option. 
				c.connect();
				
				// Read the response data
				br = new BufferedReader(
						new InputStreamReader(c.getInputStream()));
				String output = br.readLine();
				if (output != null) {
					System.out.println(output);
					// parse the output and fetch values
					String splits[] = output.split(",");
					long timestamp = Long.parseLong(splits[0]);
					long kb = Long.parseLong(splits[1]);
					// Store it in db.
					dbManager.insertToDb(nodeId, timestamp, kb - currentTotal);
					// update the current data value
					currentTotal = kb;
				}
				// Wait for 5 seconds before polling again. If previous request
				// took longer than 5 seconds
				// even then next request will be sent after 5 seconds.
				Thread.sleep(5000);
			} catch (Exception e) {
				System.err.println(e.getMessage());
			} finally {
				if (br != null) {
					try {
						br.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				if (c != null)
					c.disconnect();
			}
		}
	}
}
