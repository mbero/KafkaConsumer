package com.main.cassandra;

import java.util.Iterator;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

public class Main {
	//insert into twitter_messages(id, hashtag, message, milliseconds) values (1, 'Trump', 'Some message content', 120203023);
	public static void main(String[] args) {
		CassandraClient cassandraClient = new CassandraClient();
		cassandraClient.connect("127.0.0.1");
		ResultSet resultSet = (ResultSet) cassandraClient.executeQuery("select * from emp;", "hr");
		printResultToConsole(resultSet);
		cassandraClient.close();
	}

	private static void printResultToConsole(ResultSet result) {
		Iterator<Row> resultIterator = result.iterator();
		while(resultIterator.hasNext())
		{
			Row currentRow = resultIterator.next();
			System.out.println(currentRow);
		}
	}

}
