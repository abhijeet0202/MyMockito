/**
 * 
 */
package Basics;

import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

/**
 * @author ezbanab
 *
 */
class TestBasicList implements BasicQuery {
	private static Session session = null;
	private static final String KEYSPACE_NAME = "basic";
	private static final String TABLE_NAME = "myList";
	private static BasicQuery myObject = null;

	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	static void setUpBeforeClass() throws Exception {
		session = CassandraConnection.ConnectMe();
		new CreateKeySpace(session, KEYSPACE_NAME);
		myObject = new PlayWithList(session, KEYSPACE_NAME, TABLE_NAME);
	}

	/**
	 * @throws java.lang.Exception
	 */
	@AfterClass
	static void tearDownAfterClass() throws Exception {
		System.out.println("Destroying Connection");
		// session.execute("DROP TABLE " + KEYSPACE_NAME+"."+TABLE_NAME);
		CassandraConnection.DestroyMe();
	}

	@Test()
	void insertEntry() {
		StringBuilder sqlQuery = new StringBuilder("INSERT INTO ").append(KEYSPACE_NAME).append(".").append(TABLE_NAME)
				.append("(id, game, score)").append(" VALUES (").append("'123-abcd','quake',[12,14,17]").append(");");

		ResultSet result = myObject.executeQuery(session, sqlQuery.toString());
		AssertJUnit.assertNotNull(result);
		printStatement(session, KEYSPACE_NAME, TABLE_NAME, null);
	}

	/*
	 * Replacing all data from List. i.e. it will replace the existing set entirely
	 */
	@Test(dependsOnMethods = "insertEntry")
	void updateEntryReplaceListEntirly() {

		StringBuilder sqlQuery = new StringBuilder("UPDATE ").append(KEYSPACE_NAME).append(".").append(TABLE_NAME)
				.append(" SET score= ").append("[3,8,1]").append(" WHERE id = ").append("'123-abcd'").append(";");

		ResultSet result = myObject.executeQuery(session, sqlQuery.toString());
		AssertJUnit.assertNotNull(result);
		printStatement(session, KEYSPACE_NAME, TABLE_NAME, "'123-abcd'");
	}

	/*
	 * Append the value in the List
	 */
	@Test(dependsOnMethods = "updateEntryReplaceListEntirly")
	void appendDataInList() {

		StringBuilder sqlQuery = new StringBuilder("UPDATE ").append(KEYSPACE_NAME).append(".").append(TABLE_NAME)
				.append(" SET score= score + ").append("[200,201,202]").append(" WHERE id = ").append("'123-abcd'").append(";");

		ResultSet result = myObject.executeQuery(session, sqlQuery.toString());
		AssertJUnit.assertNotNull(result);
		printStatement(session, KEYSPACE_NAME, TABLE_NAME, "'123-abcd'");
	}
	
	/*
	 * Prepend the value in the List
	 */
	@Test(dependsOnMethods = "updateEntryReplaceListEntirly")
	void prependDataInList() {

		StringBuilder sqlQuery = new StringBuilder("UPDATE ").append(KEYSPACE_NAME).append(".").append(TABLE_NAME)
				.append(" SET score=  ").append("[100,101,102] + score").append(" WHERE id = ").append("'123-abcd'").append(";");

		ResultSet result = myObject.executeQuery(session, sqlQuery.toString());
		AssertJUnit.assertNotNull(result);
		printStatement(session, KEYSPACE_NAME, TABLE_NAME, "'123-abcd'");
	}
	
	/*
	 * Setting the value at a particular position in the list
	 */
	@Test(dependsOnMethods = "prependDataInList")
	void settingDataInList() {

		StringBuilder sqlQuery = new StringBuilder("UPDATE ").append(KEYSPACE_NAME).append(".").append(TABLE_NAME)
				.append(" SET score[1]= 1000 ").append(" WHERE id = ").append("'123-abcd'").append(";");

		ResultSet result = myObject.executeQuery(session, sqlQuery.toString());
		AssertJUnit.assertNotNull(result);
		printStatement(session, KEYSPACE_NAME, TABLE_NAME, "'123-abcd'");
	}
	
	/*
	 * Deleting the value at a particular position in the list
	 */
	@Test(dependsOnMethods = "prependDataInList")
	void deletingDatazFromListPosition() {

		StringBuilder sqlQuery = new StringBuilder("DELETE ").append("score[1] FROM ").append(KEYSPACE_NAME).append(".").append(TABLE_NAME)
				.append(" WHERE id = ").append("'123-abcd'").append(";");

		ResultSet result = myObject.executeQuery(session, sqlQuery.toString());
		AssertJUnit.assertNotNull(result);
		printStatement(session, KEYSPACE_NAME, TABLE_NAME, "'123-abcd'");
	}
	
	/*
	 * Deleting all the occurences of particular value form list
	 */
	@Test(dependsOnMethods = "prependDataInList")
	void deletingDatazFromList() {

		StringBuilder sqlQuery = new StringBuilder("UPDATE ").append(KEYSPACE_NAME).append(".").append(TABLE_NAME)
				.append(" SET score= score - ").append("[100,201]").append(" WHERE id = ").append("'123-abcd'").append(";");

		ResultSet result = myObject.executeQuery(session, sqlQuery.toString());
		AssertJUnit.assertNotNull(result);
		printStatement(session, KEYSPACE_NAME, TABLE_NAME, "'123-abcd'");
	}

}
