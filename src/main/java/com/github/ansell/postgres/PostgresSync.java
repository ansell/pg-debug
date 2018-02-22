/*
 * Copyright (c) 2016, Peter Ansell
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.github.ansell.postgres;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.stream.IntStream;

import org.jooq.lambda.Unchecked;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

/**
 * Tool for syncing between two tables in separate databases.
 * 
 * @author Peter Ansell p_ansell@yahoo.com
 */
public class PostgresSync {

	public static void main(String... args) throws Exception {
		final OptionParser parser = new OptionParser();

		final OptionSpec<Void> help = parser.accepts("help").forHelp();
		final OptionSpec<Boolean> debugOption = parser.accepts("debug").withRequiredArg().ofType(Boolean.class)
				.defaultsTo(false).describedAs("Set to true to enable debug statements on sysout");

		final OptionSpec<String> sourceJDBCOption = parser.accepts("source-jdbc").withRequiredArg().ofType(String.class)
				.required().describedAs("The JDBC connection string for the source database");
		final OptionSpec<String> sourceUsernameOption = parser.accepts("source-username").withRequiredArg()
				.ofType(String.class).required().describedAs("The JDBC username for the source database");
		final OptionSpec<String> sourcePasswordOption = parser.accepts("source-password").withRequiredArg()
				.ofType(String.class).required().describedAs("The JDBC password for the source database");

		final OptionSpec<String> destJDBCOption = parser.accepts("dest-jdbc").withRequiredArg().ofType(String.class)
				.describedAs("The JDBC connection string for the destination database");
		final OptionSpec<String> destUsernameOption = parser.accepts("dest-username").withRequiredArg()
				.ofType(String.class).describedAs("The JDBC username for the destination database");
		final OptionSpec<String> destPasswordOption = parser.accepts("dest-password").withRequiredArg()
				.ofType(String.class).describedAs("The JDBC password for the destination database");

		final OptionSpec<String> sourceMaxQueryOption = parser.accepts("source-max-query").withRequiredArg()
				.ofType(String.class).required().describedAs(
						"The query on the source to determine the maximum value. Must return a single result with a single column.");
		final OptionSpec<String> sourceSelectQueryOption = parser.accepts("source-select-query").withRequiredArg()
				.ofType(String.class).describedAs(
						"The query on the source to select rows. Must accept a parameterised value which will be substituted with the max value from the destination to get newer records.");

		final OptionSpec<String> destMaxQueryOption = parser.accepts("dest-max-query").withRequiredArg()
				.ofType(String.class).describedAs(
						"The query on the destination to determine the maximum value. Must return a single result with a single column.");
		final OptionSpec<String> destInsertQueryOption = parser.accepts("dest-insert-query").withRequiredArg()
				.ofType(String.class).describedAs(
						"The query on the destination to insert new values. Must accept the same number of parameters as were found in the rows from the source and in the same order.");

		OptionSet options = null;

		try {
			options = parser.parse(args);
		} catch (final OptionException e) {
			System.out.println(e.getMessage());
			parser.printHelpOn(System.out);
			throw e;
		}

		if (options.has(help)) {
			parser.printHelpOn(System.out);
			return;
		}

		boolean debug = debugOption.value(options);

		String sourceJDBCUrl = sourceJDBCOption.value(options);
		String sourceUsername = sourceUsernameOption.value(options);
		String sourcePassword = sourcePasswordOption.value(options);
		String sourceMaxQuery = sourceMaxQueryOption.value(options);
		String sourceSelectQuery = sourceSelectQueryOption.value(options);

		String destJDBCUrl = destJDBCOption.value(options);
		String destUsername = destUsernameOption.value(options);
		String destPassword = destPasswordOption.value(options);
		String destMaxQuery = destMaxQueryOption.value(options);
		String destInsertQuery = destInsertQueryOption.value(options);

		int sourceMaxId = executeMaxQuery(sourceJDBCUrl, sourceUsername, sourcePassword, sourceMaxQuery, debug);
		if (sourceMaxId < 0) {
			throw new RuntimeException("Failed to find source max id using query: " + sourceMaxId);
		}
		int destMaxId = executeMaxQuery(destJDBCUrl, destUsername, destPassword, destMaxQuery, debug);
		if (destMaxId < 0) {
			throw new RuntimeException("Failed to find dest max id using query: " + destMaxId);
		}
	}

	/**
	 * @param nextJDBCUrl
	 * @param nextUsername
	 * @param nextPassword
	 * @param maxQuery
	 * @param debug
	 * @return The maximum id as an integer
	 * @throws SQLException
	 * @throws RuntimeException
	 * @throws NumberFormatException
	 */
	public static int executeMaxQuery(String nextJDBCUrl, String nextUsername, String nextPassword, String maxQuery,
			boolean debug) throws SQLException, RuntimeException, NumberFormatException {
		int sourceMaxId = -1;
		try (Connection sourceConn = DriverManager.getConnection(nextJDBCUrl, nextUsername, nextPassword);
				PreparedStatement sourceMaxStatement = sourceConn.prepareStatement(maxQuery);
				ResultSet sourceMaxResults = sourceMaxStatement.executeQuery();) {
			ResultSetMetaData sourceMaxMetadata = sourceMaxResults.getMetaData();
			int sourceMaxColumns = sourceMaxMetadata.getColumnCount();
			if (sourceMaxColumns != 1) {
				throw new RuntimeException("The source max query did not return a single column");
			}
			while (sourceMaxResults.next()) {
				if (debug) {
					IntStream.range(1, sourceMaxColumns + 1)
							.forEachOrdered(Unchecked.intConsumer(i -> System.out
									.println(sourceMaxMetadata.getColumnName(i) + "=" + sourceMaxResults.getString(i)
											+ " as " + sourceMaxMetadata.getColumnTypeName(i))));
					System.out.println();
				}

				sourceMaxId = Integer.parseInt(sourceMaxResults.getString(1));
				System.out.println("Source max id = " + sourceMaxId);
			}
		}
		return sourceMaxId;
	}
}
