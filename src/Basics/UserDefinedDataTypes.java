/**
 * 
 */
package Basics;

import com.datastax.driver.core.Session;

/**
 * @author ezbanab
 *
 */
public class UserDefinedDataTypes implements BasicQuery{

	/**
	 * 
	 */
	public UserDefinedDataTypes(Session session) {
		StringBuilder sb = new StringBuilder("CREATE TYPE IF NOT EXISTS mobile ( country_code int, number text )");
		System.out.println(sb.toString());
		session.execute(sb.toString());
		
		StringBuilder sb1 = new StringBuilder("CREATE TYPE IF NOT EXISTS address ( street text, city text, zip text, phones map<text, phone>)");
		System.out.println(sb1.toString());
		session.execute(sb1.toString());
		
		StringBuilder sb2 = new StringBuilder("CREATE TABLE IF NOT EXISTS baisc.user ( name text PRIMARY KEY, addresses map<text, frozen<address>>)");
		System.out.println(sb2.toString());
		session.execute(sb2.toString());
	}

}
