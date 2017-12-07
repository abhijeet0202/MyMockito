/**
 * 
 */
package Basics;

import com.datastax.driver.core.Session;

/**
 * @author ezbanab
 *
 */
public class CreateKeySpace {

	/**
	 * 
	 */
	public CreateKeySpace(Session session, String keySpace) {
		//session = CassandraConnection.ConnectMe();

		StringBuilder sb = new StringBuilder("CREATE KEYSPACE IF NOT EXISTS ").append(keySpace)
				.append(" WITH replication = {").append("'class':").append("'SimpleStrategy'")
				.append(",'replication_factor':").append("1").append("};");

		System.out.println(sb.toString());
		session.execute(sb.toString());
	}

}
