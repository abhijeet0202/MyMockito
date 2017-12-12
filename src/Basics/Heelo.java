/**
 * 
 */
package Basics;

import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * @author ezbanab
 *
 */
class Heelo {

	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	static void setUpBeforeClass() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@AfterClass	
	static void tearDownAfterClass() throws Exception {
	}

	@Test
	void test() {
		AssertJUnit.fail("Not yet implemented");
	}

}
