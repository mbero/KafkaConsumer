package com.main.cassandra;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.ResultSet;

public class CassandraClientTest {

	private CassandraClient cassandraClient;
	
	@Before
	public void setUp() throws Exception {
		cassandraClient = new CassandraClient();
		cassandraClient.connect("127.0.0.1");
	}


	@Test
	public void testExecuteQuery() {
		ResultSet resultSet = (ResultSet) cassandraClient.executeQuery("select * from twitter_messages;", "hr");
		assertNotNull(resultSet);
	}

}
