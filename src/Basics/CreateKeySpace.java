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
public class CreateKeySpace {

	/**
	 * 
	 */
	public CreateKeySpace() {
		Cluster cluster = null;
		try {
			cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
			Session session = cluster.connect(); // (2)
			
			session.execute("CREATE KEYSPACE Basic WITH replication "
		            + "= {'class':'SimpleStrategy', 'replication_factor':1};");
			
			ResultSet rs = session.execute("select release_version from system.local"); // (3)
			Row row = rs.one();
			System.out.println(row.getString("release_version")); // (4)
		} finally {
			if (cluster != null)
				cluster.close(); // (5)
		}

	}
	public static void main(String[] args) {
		new CreateKeySpace();
	}

}
