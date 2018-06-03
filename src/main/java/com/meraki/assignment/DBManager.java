package com.meraki.assignment;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.HashMap;

/***
 * Singleton class to provide synchronized DB access to multiple threads.
 * 
 * @author vivek bhandari
 *
 */

public class DBManager {
	public static DBManager dbManager = new DBManager();
	private Connection connection = null;
	private Statement getStatement = null;
	private PreparedStatement insertStatement = null;

	private DBManager() {
		try {
			// Create a new connection with the db file located in demo home.
//			connection = DriverManager.getConnection(
//					"jdbc:sqlite:/home/demo/usage_data.sqlite3");
			connection = DriverManager.getConnection(
					"jdbc:sqlite:/Users/vivb/eclipse/workspace/meraki/usage_data.sqlite3");
			getStatement = connection.createStatement();
			getStatement.setQueryTimeout(5);
			insertStatement = connection
					.prepareStatement("insert into usage_data values(?, ?, ?)");
			insertStatement.setQueryTimeout(15);
		} catch (SQLException e) {
			System.err.println(e.getMessage());
		}
	}

	/**
	 * Method to insert new data value for a given node.
	 * 
	 * @param nodeId
	 *            - node for which new grabbed data needs to be stored.
	 * @param timestamp
	 *            - timestamp returned by the node.
	 * @param kb
	 *            - delta between data value returned by node and last stored
	 *            value in database.
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	public synchronized void insertToDb(int nodeId, long timestamp, long kb)
			throws ClassNotFoundException, InterruptedException {
		try {
			insertStatement.setInt(1, nodeId);
			insertStatement.setTimestamp(2, new Timestamp(timestamp));
			insertStatement.setLong(3, kb);
			insertStatement.execute();
		} catch (SQLException e) {
			System.err.println(e.getMessage());
		}
	}

	/****
	 * Method to fetch cumulative SUM of the data values for all nodes so far.
	 * It needed when application is restarted. Then next delta needs to be
	 * between current value returned by node and total already stored in db.
	 * 
	 * @return HashMap containing node Id to total data value
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	public synchronized HashMap<Integer, Long> getKbSoFar()
			throws ClassNotFoundException, InterruptedException {
		HashMap<Integer, Long> dataMap = new HashMap<Integer, Long>();
		try {
			ResultSet rs = getStatement.executeQuery(
					"select node_id, sum(kb) as kbSum from usage_data group by node_Id");
			while (rs.next()) {
				int nodeId = rs.getInt("node_id");
				long kbSum = rs.getInt("kbSum");
				dataMap.put(nodeId, kbSum);
			}
			System.out.println(dataMap);
		} catch (SQLException e) {
			System.err.println(e.getMessage());
		}
		return dataMap;
	}

	public void close() throws SQLException {
		connection.close();
	}
}
