/**
 * 
 */
package Basics;

import com.datastax.driver.core.Session;

/**
 * @author ezbanab
 *
 */
public class PlayWithList implements BasicQuery {

	/**
	 * 
	 */
	public PlayWithList(Session session, String keySpaceName, String tableName) {
		// session = CassandraConnection.ConnectMe();

		StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ").append(keySpaceName).append(".")
				.append(tableName).append("( id text PRIMARY KEY, game text, score list<int> );");
		System.out.println(sb.toString());

		session.execute(sb.toString());

	}

}
