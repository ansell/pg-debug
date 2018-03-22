/**
 * 
 */
package com.github.ansell.postgres;

import static org.junit.jupiter.api.Assertions.*;

import io.airlift.testing.postgresql.TestingPostgreSqlServer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import joptsimple.OptionException;

/**
 * Tests for {@link PostgresSync}
 * 
 * @author Peter Ansell p_ansell@yahoo.com
 */
class PostgresSyncTest {

	private static TestingPostgreSqlServer server;

	@BeforeAll
	static void setUpClass() throws Exception {
		server = new TestingPostgreSqlServer("testUser", "testDatabase");
	}

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

	@AfterAll
	static void tearDownClass() throws Exception {
		if(server != null) {
			server.close();
		}
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.postgres.PostgresSync#main(java.lang.String[])}.
	 */
	@Test
	final void testMainHelp() throws Exception {
		PostgresSync.main("--help");
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.postgres.PostgresSync#main(java.lang.String[])}.
	 */
	@Test
	final void testMainNoArgs() throws Exception {
		assertThrows(OptionException.class, () -> PostgresSync.main());
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.postgres.PostgresSync#main(java.lang.String[])}.
	 */
	@Test
	final void testMainUnrecognisedArg() throws Exception {
		assertThrows(OptionException.class, () -> PostgresSync.main("--no-matching-arg"));
	}

}
