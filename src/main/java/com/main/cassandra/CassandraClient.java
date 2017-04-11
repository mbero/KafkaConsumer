package com.main.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

public class CassandraClient {
	private Cluster cluster;
	private Session session;

	/**
	 * Connects with node at given adress
	 * 
	 * @param nodeAdress
	 */
	public void connect(String nodeAdress) {
		cluster = Cluster.builder().addContactPoint(nodeAdress).build();
		Metadata metadata = cluster.getMetadata();
		System.out.printf("Connected to cluster: %s\n", metadata.getClusterName());
		for (Host host : metadata.getAllHosts()) {
			System.out.printf("Datatacenter: %s; Host: %s; Rack: %s\n",
					host.getDatacenter(), 
					host.getAddress(),
					host.getRack());
		}
	}
	
	/**
	 * Executes query - keyspace is optional, you can define keyspace in query
	 * itsfelt
	 * 
	 * @param query
	 * @param keyspace
	 * @return
	 */
	public Object executeQuery(String query, String... keyspace) {
		if (session == null) {
			session = getSession();
		}
		if (keyspace.length != 0) {
			session.execute("use " + keyspace[0] + ";");
		}
		ResultSet resultSet = session.execute(query);
		return resultSet;
	}

	public Cluster getCluster() {
		return cluster;
	}

	public void setCluster(Cluster cluster) {
		this.cluster = cluster;
	}

	public void setSession(Session session) {
		this.session = session;
	}

	public Session getSession() {
		if (session == null) {
			session = cluster.newSession();
		}
		return session;
	}

	public void close() {
		cluster.close();
	}
}
