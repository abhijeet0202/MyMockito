package Basics;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

public class PlayWithMaps {

	public PlayWithMaps(Session session, String keySpaceName, String tableName) {
		// session = CassandraConnection.ConnectMe();

		StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ").append(keySpaceName).append(".")
				.append(tableName).append("( id text PRIMARY KEY, name text, favs map<text, text> );");
		System.out.println(sb.toString());

		session.execute(sb.toString());

	}

	public ResultSet executeQuery(Session session, String sqlQuery) {
		System.out.println(sqlQuery);
		return session.execute(sqlQuery);
	}

}
