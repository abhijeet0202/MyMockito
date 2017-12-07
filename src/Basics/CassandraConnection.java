/**
 * 
 */
package Basics;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

/**
 * @author ezbanab
 *
 */
public class CassandraConnection {
	private static Cluster cluster = null;
	private static Session session = null;

	public static Session ConnectMe() {
		if (cluster == null) {
			try {
				cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
				session = cluster.connect(); // (2)

				ResultSet rs = session.execute("select release_version from system.local"); // (3)
				Row row = rs.one();
				System.out.println(row.getString("release_version")); // (4)
			} catch (Exception ex) {
				System.out.println(ex);
			}

		}
		return session;
	}

	public static void DestroyMe() {
		if (cluster != null) {
			cluster.close();
		}
	}
}
