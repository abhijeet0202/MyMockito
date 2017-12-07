/**
 * 
 */
package Basics;

import com.datastax.driver.core.Session;

/**
 * @author ezbanab
 *
 */
public class PlayWithSets implements BasicQuery {

	/**
	 * 
	 */
	public PlayWithSets(Session session, String keySpaceName, String tableName) {
		// session = CassandraConnection.ConnectMe();

		StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ").append(keySpaceName).append(".")
				.append(tableName).append("( id text PRIMARY KEY, name text, favs set<text> );");
		System.out.println(sb.toString());

		session.execute(sb.toString());

	}

}
