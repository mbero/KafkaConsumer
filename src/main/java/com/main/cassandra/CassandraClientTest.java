package com.main.cassandra;

import static org.junit.Assert.*;

import java.util.Calendar;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.ResultSet;

public class CassandraClientTest {

	private CassandraClient cassandraClient;
	private String selectQuery;
	private String insertQuery;

	@Before
	public void setUp() throws Exception {
		cassandraClient = new CassandraClient();
		cassandraClient.connect("127.0.0.1");
		selectQuery = "select * from twitter_messages;";
	}

	@Test
	public void testSelectQuery() {
		ResultSet resultSet = (ResultSet) cassandraClient.executeQuery(selectQuery, "hr");
		assertNotNull(resultSet);
	}

	@Test
	public void testInsertQuery() {
		UUID uuid = cassandraClient.getUUID();
		String convertedUUID = uuid.toString();
		long currentMillisecondsDate = Calendar.getInstance().get(Calendar.MILLISECOND);
		insertQuery = "insert into twitter_messages (id, hashtag, message, milliseconds) values ("+ convertedUUID +", 'Trump', 'Some message', "+currentMillisecondsDate+");";
		ResultSet resultSet = (ResultSet) cassandraClient.executeQuery(insertQuery, "hr");
		assertNotNull(resultSet);
	}
	
	@Test
	public void testManyInsertsQuery() {
		int amountOfRows = 10;
		for(int i=0; i<amountOfRows; i++){
			UUID uuid = cassandraClient.getUUID();
			String convertedUUID = uuid.toString();
			long millisStart = Calendar.getInstance().getTimeInMillis();
			insertQuery = "insert into twitter_messages (id, hashtag, message, milliseconds) values ("+ convertedUUID +", 'Trump', 'Some message', "+millisStart+");";
			cassandraClient.executeQuery(insertQuery, "hr");
		}
		ResultSet resultSet = (ResultSet) cassandraClient.executeQuery(selectQuery, "hr");
		assertTrue(resultSet.all().size()>amountOfRows);
		//cassandraClient.executeQuery("TRUNCATE twitter_messages;", "hr");
		
	}


}
