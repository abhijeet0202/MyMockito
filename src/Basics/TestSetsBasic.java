/**
 * 
 */
package Basics;

import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

/**
 * @author ezbanab
 *
 */
class TestSetsBasic implements BasicQuery {
	private static Session session = null;
	private static final String KEYSPACE_NAME = "basic";
	private static final String TABLE_NAME = "mySets";
	private static BasicQuery myObject = null;

	@DataProvider(name = "insertEntry")
	public Object[][] provideInsertEntryInMap() {
		return new Object[][] { { "'abhibane', 'Abhijeet Banerjee', { 'U' , 'V', 'W' , 'X' }" },
				{ "'ezbanab', 'Abhijeet Banerjee', { 'A' , 'B', 'C' , 'D' }" },
				{ "'HR26BZ2435', 'Abhijeet Banerjee', { 'E' , 'F', 'G' , 'H' }" },
				{ "'aryan', 'Abhijeet Banerjee', { 'I' , 'J', 'K' , 'L' }" },
				{ "'Abhi', 'Abhijeet Banerjee', { 'M' , 'N', 'O' , 'P' }" },
				{ "'21786', 'Abhijeet Banerjee', { 'Q' , 'R', 'S' , 'T'}" } };
	}

	@DataProvider(name = "updateEntry")
	public Object[][] provideUpdateEntryInMap() {
		return new Object[][] { { "'abhibane'", "{ '1' , '2', '3' , '4' }" },
				{ "'ezbanab'", "{ '5' , '6', '7' , '8' }" }, { "'HR26BZ2435'", "{ '9' , '10', '11' , '12' }" },
				{ "'aryan'", "{ '13' , '14', '15' , '16' }" }, { "'Abhi'", "{ '17' , '18', '19' , '20' }" },
				{ "'21786'", "{ '21' , '22', '23' , '24', '25' }" } };
	}
	@DataProvider(name = "updateNewEntryCase")
	public Object[][] provideUpdateNewEntryInSet() {
		return new Object[][] { { "'abhibane'", "{ 'movie' , 'RamJaane', 'Travel' , 'Munnar' }" },
				{ "'ezbanab'", "{ 'movie' , 'Singh IS King', 'Travel' , 'Chilka' }" },
				{ "'HR26BZ2435'", "{ 'movie' , 'Dhoom', 'Travel' , 'Kodaikanal' }" },
				{ "'aryan'", "{ 'movie' , 'Dhoom 1', 'Travel' , 'varthur' }" },
				{ "'Abhi'", "{ 'movie' , 'Dhoom 2', 'Travel' , 'Not Intrested' }" },
				{ "'21786'", "{ 'movie' , 'Dhoom 3', 'Travel' , 'Ooty' }" } };
	}

	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	static void setUpBeforeClass() throws Exception {
		session = CassandraConnection.ConnectMe();
		new CreateKeySpace(session, KEYSPACE_NAME);
		myObject = new PlayWithSets(session, KEYSPACE_NAME, TABLE_NAME);
	}

	/**
	 * @throws java.lang.Exception
	 */
	@AfterClass
	static void tearDownAfterClass() throws Exception {
		System.out.println("Destroying Connection");
		session.execute("DROP TABLE " + KEYSPACE_NAME+"."+TABLE_NAME);
		CassandraConnection.DestroyMe();
	}

	@Test(dataProvider = "insertEntry")
	void insertEntry(String query) {
		StringBuilder sqlQuery = new StringBuilder("INSERT INTO ").append(KEYSPACE_NAME).append(".").append(TABLE_NAME)
				.append("(id, name, favs)").append(" VALUES (").append(query).append(");");

		ResultSet result = myObject.executeQuery(session, sqlQuery.toString());
		AssertJUnit.assertNotNull(result);
		printStatement(session, KEYSPACE_NAME, TABLE_NAME, null);
	}

	/*
	 * Replacing  any data from Set. But it will replace the existing set
	 * entirely
	 */
	@Test(dependsOnMethods = "insertEntry", dataProvider = "updateEntry")
	void updateEntryReplaceSetEntirly(String id, String setData) {

		StringBuilder sqlQuery = new StringBuilder("UPDATE ").append(KEYSPACE_NAME).append(".").append(TABLE_NAME)
				.append(" SET favs= ").append(setData).append(" WHERE id = ").append(id).append(";");

		ResultSet result = myObject.executeQuery(session, sqlQuery.toString());
		AssertJUnit.assertNotNull(result);
		printStatement(session, KEYSPACE_NAME, TABLE_NAME, id);
	}
	
	/*
	 * INSERTING one or more elements:
	 */
	@Test(dependsOnMethods = "insertEntry", dataProvider = "updateNewEntryCase")
	void updateOrInsertElements(String id, String setData) {
		StringBuilder sqlQuery = new StringBuilder("UPDATE ").append(KEYSPACE_NAME).append(".").append(TABLE_NAME)
				.append(" SET favs = favs +").append(setData).append(" WHERE id = ").append(id).append(";");

		ResultSet result = myObject.executeQuery(session, sqlQuery.toString());
		AssertJUnit.assertNotNull(result);
		printStatement(session, KEYSPACE_NAME, TABLE_NAME,id);
	}
	
	/*
	 * DELETINg one or more elements:
	 */
	@Test(dependsOnMethods = "updateOrInsertElements", dataProvider = "updateNewEntryCase")
	void deleteElements(String id, String setData) {
		StringBuilder sqlQuery = new StringBuilder("UPDATE ").append(KEYSPACE_NAME).append(".").append(TABLE_NAME)
				.append(" SET favs = favs -").append(setData).append(" WHERE id = ").append(id).append(";");

		ResultSet result = myObject.executeQuery(session, sqlQuery.toString());
		AssertJUnit.assertNotNull(result);
		printStatement(session, KEYSPACE_NAME, TABLE_NAME,id);
	}

}
