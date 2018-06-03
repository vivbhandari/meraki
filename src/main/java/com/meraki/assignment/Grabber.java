package com.meraki.assignment;

import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/***
 * Main class to initiate Grabber application.
 * 
 * @author vivek bhandari
 */
public class Grabber {
	private void startThreads(int numOfNodes) {
		try {
			// Load existing cumulative data values for each node. For restart
			// scenario.
			HashMap<Integer, Long> dataMap = DBManager.dbManager
					.getKbSoFar();
			// Create a thread pool for all nodes.
			ExecutorService executorService = Executors
					.newFixedThreadPool(numOfNodes);
			// Submit runnables for execution.
			for (int i = 1; i <= numOfNodes; i++) {
				executorService.submit(new GrabberThread(i, DBManager.dbManager,
						dataMap.getOrDefault(i, 0l)));
			}
		} catch (ClassNotFoundException e) {
			System.err.println(e);
		} catch (InterruptedException e) {
			System.err.println(e);
		}
	}

	public static void main(String[] args) {
		try {
			// default number of nodes to run the application for.
			int numOfNodes = 1;
			// check thr argument and parse if available.
			if (args.length > 0) {
				System.out.println(Arrays.toString(args));
				try {
					numOfNodes = Integer.parseInt(args[0]);
				} catch (Exception e) {
					System.err.println(e);
				}
			}

			// load the db driver class
			Class.forName("org.sqlite.JDBC");

			// Initiate threads for each node.
			new Grabber().startThreads(numOfNodes);
		} catch (Exception e) {
			System.err.println(e);
		}
	}
}
