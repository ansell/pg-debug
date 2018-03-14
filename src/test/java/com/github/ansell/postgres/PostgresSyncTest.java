/**
 * 
 */
package com.github.ansell.postgres;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link PostgresSync}
 * 
 * @author Peter Ansell p_ansell@yahoo.com
 */
class PostgresSyncTest {

	/**
	 * @throws java.lang.Exception
	 */
	@BeforeEach
	void setUp() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@AfterEach
	void tearDown() throws Exception {
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.postgres.PostgresSync#main(java.lang.String[])}.
	 */
	@Test
	final void testMainHelp() throws Exception {
		PostgresSync.main("--help");
	}

}
