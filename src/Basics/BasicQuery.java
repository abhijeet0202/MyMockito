/**
 * 
 */
package Basics;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

/**
 * @author ezbanab
 *
 */
public interface BasicQuery {
	public default ResultSet executeQuery(Session session, String sqlQuery) {
		System.out.println(sqlQuery);
		return session.execute(sqlQuery);
	}
	public default void printStatement(Session session, String keySpaceName, String tableName, String id) {
		ResultSet result = null;
		if (id ==null) {
			result = session.execute("SELECT * FROM "+keySpaceName+"."+tableName);
		}else {
			System.out.println("SELECT * FROM "+keySpaceName+"."+tableName+ " WHERE id = "+id);
			result = session.execute("SELECT * FROM "+keySpaceName+"."+tableName+ " WHERE id = "+id);
		}
		while (!result.isExhausted()) {
			Row row = result.one();
			System.out.println("OutPut:"+row);
		}
		
	}
}
