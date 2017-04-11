package com.main.kafka;

import java.util.Calendar;
import java.util.UUID;

import com.main.cassandra.CassandraClient;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

public class ConsumerTest implements Runnable {
	private KafkaStream m_stream;
	private int m_threadNumber;
	private CassandraClient cassandraClient;

	public ConsumerTest(KafkaStream a_stream, int a_threadNumber) {
		cassandraClient = new CassandraClient();
		cassandraClient.connect("127.0.0.1");

		m_threadNumber = a_threadNumber;
		m_stream = a_stream;
	}

	public void run() {
		ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
		while (it.hasNext()) {
			System.out.println("Thread " + m_threadNumber + ": " + new String(it.next().message()));
			System.out.println("Shutting down Thread: " + m_threadNumber);
			//Saving each message to Cassandra
			UUID uuid = cassandraClient.getUUID();
			String convertedUUID = uuid.toString();
			long currentMillisecondsDate = Calendar.getInstance().getTimeInMillis();
			String insertQuery = "insert into twitter_messages (id, hashtag, message, milliseconds) values ("
					+ convertedUUID + ", Trump, " +  new String(it.next().message()) + ", " + currentMillisecondsDate
					+ ");";
			cassandraClient.executeQuery(insertQuery, "hr"); 
		}
	}
}