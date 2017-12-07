package Basics;

import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

class TestBasics {
	private static Session session = null;
	private static final String KEYSPACE_NAME = "basic";
	private static final String TABLE_NAME = "myMaps";
	private static PlayWithMaps myObject = null;

	@DataProvider(name = "insertEntry")
	public Object[][] provideInsertEntryInMap() {
		return new Object[][] { { "'abhibane', 'Abhijeet Banerjee', { 'fruit' : 'Banana', 'band' : 'Beatles' }" },
				{ "'ezbanab', 'Abhijeet Banerjee', { 'fruit' : 'Banana', 'band' : 'Beatles' }" },
				{ "'HR26BZ2435', 'Abhijeet Banerjee', { 'fruit' : 'Banana', 'band' : 'Beatles' }" },
				{ "'aryan', 'Abhijeet Banerjee', { 'fruit' : 'Banana', 'band' : 'Beatles' }" },
				{ "'Abhi', 'Abhijeet Banerjee', { 'fruit1' : 'Banana', 'band' : 'Beatles' }" },
				{ "'21786', 'Abhijeet Banerjee', { 'fruit1' : 'Banana', 'band' : 'Beatles', 'Drink' : 'Aaj Kuch Toofani' }" } };
	}

	@DataProvider(name = "updateEntry")
	public Object[][] provideUpdateEntryInMap() {
		return new Object[][] { { "'abhibane'", "{ 'fruit' : 'Banana', 'band' : 'Kings' }" },
				{ "'ezbanab'", "{ 'fruit' : 'Banana', 'band' : 'Kings' }" },
				{ "'HR26BZ2435'", "{ 'fruit' : 'Banana', 'band' : 'Kings' }" },
				{ "'aryan'", "{ 'fruit' : 'Banana', 'band' : 'Kings' }" },
				{ "'Abhi'", "{ 'fruit1' : 'Banana', 'band' : 'Kings' }" },
				{ "'21786'", "{ 'fruit1' : 'Banana', 'band' : 'Kings', 'Drink' : 'Aaj Kuch Toofani' }" } };
	}

	@DataProvider(name = "updateNewEntryCase1")
	public Object[][] provideUpdateNewEntryInMapCase1() {
		return new Object[][] { { "'21786'", "['author'] = 'Ed Poe'" }, { "'abhibane'", "['author'] = 'Gyan'" },
				{ "'ezbanab'", "['author'] = 'Dhyan'" }, { "'HR26BZ2435'", "['author'] = 'Speechless'" },
				{ "'aryan'", "['author'] = 'ED Hardy'" }, { "'Abhi'", "['author'] = 'No Idea'" } };
	}
	
	@DataProvider(name = "updateNewEntryCase2")
	public Object[][] provideUpdateNewEntryInMapCase2() {
		return new Object[][] { { "'abhibane'", "{ 'movie' : 'RamJaane', 'Travel' : 'Munnar' }" },
				{ "'ezbanab'", "{ 'movie' : 'Singh IS King', 'Travel' : 'Chilka' }" },
				{ "'HR26BZ2435'", "{ 'movie' : 'Dhoom', 'Travel' : 'Kodaikanal' }" },
				{ "'aryan'", "{ 'movie' : 'Dhoom 1', 'Travel' : 'varthur' }" },
				{ "'Abhi'", "{ 'movie' : 'Dhoom 2', 'Travel' : 'Not Intrested' }" },
				{ "'21786'", "{ 'movie' : 'Dhoom 3', 'Travel' : 'Ooty' }" } };
	}
	
	@DataProvider(name = "deleteEntryCase1")
	public Object[][] provideDeleteEntryInMapCase1() {
		return new Object[][] { 
			    { "'abhibane'", "'fruit'" },
				{ "'ezbanab'", "'fruit'" },
				{ "'HR26BZ2435'", "'fruit'" },
				{ "'aryan'", "'fruit'" },
				{ "'Abhi'", "'fruit'" },
				{ "'21786'", "'fruit'" } };
	}
	@DataProvider(name = "deleteEntryCase2")
	public Object[][] provideDeleteEntryInMapCase2() {
		return new Object[][] { 
			    { "'abhibane'", "{'movie', 'band'}" },
				{ "'ezbanab'", "{'movie', 'band'}" },
				{ "'HR26BZ2435'", "{'movie', 'band'}" },
				{ "'aryan'", "{'movie', 'band'}" },
				{ "'Abhi'", "{'movie', 'band'}" },
				{ "'21786'", "{'movie', 'band'}" } };
	}
	
	private void printStatement(Session session, String keySpaceName, String tableName, String id) {
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

	@BeforeClass
	static void setUpBeforeClass() throws Exception {
		session = CassandraConnection.ConnectMe();
		new CreateKeySpace(session, KEYSPACE_NAME);
		myObject = new PlayWithMaps(session, KEYSPACE_NAME, TABLE_NAME);
	}

	@AfterClass
	static void tearDownAfterClass() throws Exception {
		System.out.println("Destroying Connection");
		session.execute("DROP KEYSPACE "+ KEYSPACE_NAME);
		CassandraConnection.DestroyMe();
	}

	@Test(dataProvider = "insertEntry")
	void insertEntry(String query) {
		/*
		 * StringBuilder sqlQuery = new
		 * StringBuilder("INSERT INTO ").append(KEYSPACE_NAME).append(".").append(
		 * TABLE_NAME). append("(id, name, favs)").append(" VALUES ").
		 * append("( 'abhibane', 'Abhijeet Banerjee', { 'fruit' : 'Banana', 'band' : 'Beatles' });"
		 * );
		 */
		StringBuilder sqlQuery = new StringBuilder("INSERT INTO ").append(KEYSPACE_NAME).append(".").append(TABLE_NAME)
				.append("(id, name, favs)").append(" VALUES (").append(query).append(");");

		ResultSet result = myObject.executeQuery(session, sqlQuery.toString());
		AssertJUnit.assertNotNull(result);
		printStatement(session, KEYSPACE_NAME, TABLE_NAME, null);
	}

	/*
	 * Replacing the any data from Map. But it will replace the existing map
	 * entirely
	 */
	@Test(dependsOnMethods = "insertEntry", dataProvider = "updateEntry")
	void updateEntryReplaceMapEntirly(String id, String mapData) {
	
		StringBuilder sqlQuery = new StringBuilder("UPDATE ").append(KEYSPACE_NAME).append(".").append(TABLE_NAME)
				.append(" SET favs= ").append(mapData).append(" WHERE id = ").append(id).append(";");

		ResultSet result = myObject.executeQuery(session, sqlQuery.toString());
		AssertJUnit.assertNotNull(result);
		printStatement(session, KEYSPACE_NAME, TABLE_NAME, id);
	}

	/*
	 * STYLE1: UPDATING or INSERTING one or more elements:
	 */
	@Test(dependsOnMethods = "insertEntry", dataProvider = "updateNewEntryCase1")
	void TC1_updateOrInsertElements(String id, String mapData) {
		StringBuilder sqlQuery = new StringBuilder("UPDATE ").append(KEYSPACE_NAME).append(".").append(TABLE_NAME)
				.append(" SET favs").append(mapData).append(" WHERE id = ").append(id).append(";");

		ResultSet result = myObject.executeQuery(session, sqlQuery.toString());
		AssertJUnit.assertNotNull(result);
		printStatement(session, KEYSPACE_NAME, TABLE_NAME,id);
	}
	
	/*
	 * STYLe2:UPDATING or INSERTING one or more elements:
	 */
	@Test(dependsOnMethods = "insertEntry", dataProvider = "updateNewEntryCase2")
	void TC2_updateOrInsertElements(String id, String mapData) {
		StringBuilder sqlQuery = new StringBuilder("UPDATE ").append(KEYSPACE_NAME).append(".").append(TABLE_NAME)
				.append(" SET favs = favs +").append(mapData).append(" WHERE id = ").append(id).append(";");

		ResultSet result = myObject.executeQuery(session, sqlQuery.toString());
		AssertJUnit.assertNotNull(result);
		printStatement(session, KEYSPACE_NAME, TABLE_NAME, id);
	}
	
	/*
	 * STYLE1: UPDATING or INSERTING one or more elements:
	 */
	@Test(dependsOnMethods = "insertEntry", dataProvider = "deleteEntryCase1")
	void TC1_deleteElements(String id, String mapData) {
		StringBuilder sqlQuery = new StringBuilder("DELETE ").append("favs[").append(mapData).append("] FROM ").append(KEYSPACE_NAME).append(".").append(TABLE_NAME)
				.append(" WHERE id = ").append(id).append(";");

		ResultSet result = myObject.executeQuery(session, sqlQuery.toString());
		AssertJUnit.assertNotNull(result);
		printStatement(session, KEYSPACE_NAME, TABLE_NAME, id);
	}
	
	/*
	 * STYLe2:UPDATING or INSERTING one or more elements:
	 */
	@Test(dependsOnMethods = "insertEntry", dataProvider = "deleteEntryCase2")
	void TC2_deleteElements(String id, String mapData) {
		StringBuilder sqlQuery = new StringBuilder("UPDATE ").append(KEYSPACE_NAME).append(".").append(TABLE_NAME)
				.append(" SET favs = favs -").append(mapData).append(" WHERE id = ").append(id).append(";");

		ResultSet result = myObject.executeQuery(session, sqlQuery.toString());
		AssertJUnit.assertNotNull(result);
		printStatement(session, KEYSPACE_NAME, TABLE_NAME, id);
	}

}
