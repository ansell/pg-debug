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

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import org.jooq.lambda.Unchecked;
import org.postgis.PGgeometry;
import org.postgresql.util.PGobject;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.github.ansell.csv.stream.util.JSONStreamUtil;

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

		final OptionSpec<File> configFileOption = parser.accepts("config").withRequiredArg().ofType(File.class)
				.required().describedAs("A JSON file containing the queries of this sync operation");

		// final OptionSpec<String> sourceJDBCOption =
		// parser.accepts("source-jdbc").withRequiredArg().ofType(String.class)
		// .required().describedAs("The JDBC connection string for the source
		// database");
		// final OptionSpec<String> sourceUsernameOption =
		// parser.accepts("source-username").withRequiredArg()
		// .ofType(String.class).required().describedAs("The JDBC username for the
		// source database");
		// final OptionSpec<String> sourcePasswordOption =
		// parser.accepts("source-password").withRequiredArg()
		// .ofType(String.class).required().describedAs("The JDBC password for the
		// source database");
		//
		// final OptionSpec<String> destJDBCOption =
		// parser.accepts("dest-jdbc").withRequiredArg().ofType(String.class)
		// .describedAs("The JDBC connection string for the destination database");
		// final OptionSpec<String> destUsernameOption =
		// parser.accepts("dest-username").withRequiredArg()
		// .ofType(String.class).describedAs("The JDBC username for the destination
		// database");
		// final OptionSpec<String> destPasswordOption =
		// parser.accepts("dest-password").withRequiredArg()
		// .ofType(String.class).describedAs("The JDBC password for the destination
		// database");

		// final OptionSpec<String> sourceMaxQueryOption =
		// parser.accepts("source-max-query").withRequiredArg()
		// .ofType(String.class).required().describedAs(
		// "The query on the source to determine the maximum value. Must return a single
		// result with a single column.");
		// final OptionSpec<String> sourceSelectQueryOption =
		// parser.accepts("source-select-query").withRequiredArg()
		// .ofType(String.class).describedAs(
		// "The query on the source to select rows. Must accept a parameterised value
		// which will be substituted with the max value from the destination to get
		// newer records.");
		//
		// final OptionSpec<String> destMaxQueryOption =
		// parser.accepts("dest-max-query").withRequiredArg()
		// .ofType(String.class).describedAs(
		// "The query on the destination to determine the maximum value. Must return a
		// single result with a single column.");
		// final OptionSpec<String> destInsertQueryOption =
		// parser.accepts("dest-insert-query").withRequiredArg()
		// .ofType(String.class).describedAs(
		// "The query on the destination to insert new values. Must accept the same
		// number of parameters as were found in the rows from the source and in the
		// same order.");
		// final OptionSpec<String> destAutoIncrementQueryOption =
		// parser.accepts("dest-insert-query").withRequiredArg()
		// .ofType(String.class).describedAs("The query on the destination to update the
		// auto-increment counter.");

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

		JsonNode config = JSONStreamUtil.loadJSON(configFileOption.value(options).toPath());

		System.out.println(JSONStreamUtil.queryJSONNodeAsText(config, "/label"));

		String sourceJDBCUrl = JSONStreamUtil.queryJSONNodeAsText(config, "/source/jdbcUrl");
		String sourceUsername = JSONStreamUtil.queryJSONNodeAsText(config, "/source/username");
		String sourcePassword = JSONStreamUtil.queryJSONNodeAsText(config, "/source/password");

		String destJDBCUrl = JSONStreamUtil.queryJSONNodeAsText(config, "/destination/jdbcUrl");
		String destUsername = JSONStreamUtil.queryJSONNodeAsText(config, "/destination/username");
		String destPassword = JSONStreamUtil.queryJSONNodeAsText(config, "/destination/password");

		JsonNode queries = JSONStreamUtil.queryJSONNode(config, "/syncQueries");

		executeQueries(sourceJDBCUrl, sourceUsername, sourcePassword, destJDBCUrl, destUsername, destPassword, queries,
				debug);
	}

	private static void executeQueries(String sourceJDBCUrl, String sourceUsername, String sourcePassword,
			String destJDBCUrl, String destUsername, String destPassword, JsonNode queries, boolean debug) {
		queries.forEach(Unchecked.consumer(nextQuery -> executeQuery(sourceJDBCUrl, sourceUsername, sourcePassword,
				destJDBCUrl, destUsername, destPassword, nextQuery, debug)));
	}

	private static void executeQuery(String sourceJDBCUrl, String sourceUsername, String sourcePassword,
			String destJDBCUrl, String destUsername, String destPassword, JsonNode query, boolean debug)
			throws JsonProcessingException, IOException, NumberFormatException, SQLException, RuntimeException {

		String label = JSONStreamUtil.queryJSONNodeAsText(query, "/label");

		String sourceMaxQuery = JSONStreamUtil.queryJSONNodeAsText(query, "/source/maxQuery");
		String sourceSelectQuery = JSONStreamUtil.queryJSONNodeAsText(query, "/source/selectQuery");
		int selectIdFieldIndex = Integer
				.parseInt(JSONStreamUtil.queryJSONNodeAsText(query, "/source/selectIdFieldIndex"));
		int sourcePagingSize = Integer.parseInt(JSONStreamUtil.queryJSONNodeAsText(query, "/source/pagingSize"));
		String destMaxQuery = JSONStreamUtil.queryJSONNodeAsText(query, "/destination/maxQuery");
		String destInsertQuery = JSONStreamUtil.queryJSONNodeAsText(query, "/destination/insertQuery");
		String destUpdateQuery = JSONStreamUtil.queryJSONNodeAsText(query, "/destination/updateQuery");
		String destUpdateAutoincrementQuery = JSONStreamUtil.queryJSONNodeAsText(query,
				"/destination/updateAutoincrementQuery");

		int sourceMaxId = executeMaxQuery(sourceJDBCUrl, sourceUsername, sourcePassword, sourceMaxQuery, debug,
				label + " (source)");
		if (sourceMaxId < 0) {
			throw new RuntimeException("Failed to find source max id using query: " + sourceMaxId);
		}
		int destMaxId = executeMaxQuery(destJDBCUrl, destUsername, destPassword, destMaxQuery, debug,
				label + " (dest)");
		if (destMaxId < 0) {
			throw new RuntimeException("Failed to find dest max id using query: " + destMaxId);
		}

		if (destMaxId >= sourceMaxId) {
			System.out.println("Auto-increment value on destination does not need updating");
		} else {
			System.out.println(
					"Need to update auto-increment value on destination from " + destMaxId + " to " + sourceMaxId);
		}

		executeSync(sourceJDBCUrl, sourceUsername, sourcePassword, sourceSelectQuery, sourcePagingSize, destMaxId,
				destJDBCUrl, destUsername, destPassword, destInsertQuery, selectIdFieldIndex, destUpdateQuery, debug,
				"source");
	}

	private static void executeSync(String sourceJDBCUrl, String sourceUsername, String sourcePassword,
			String sourceSelectQuery, int sourcePagingSize, int destMaxId, String destJDBCUrl, String destUsername,
			String destPassword, String destInsertQuery, int selectIdFieldIndex, String destUpdateQuery, boolean debug,
			String targetName) throws SQLException, RuntimeException {

		try (Connection sourceConn = DriverManager.getConnection(sourceJDBCUrl, sourceUsername, sourcePassword);
				Connection destConn = DriverManager.getConnection(destJDBCUrl, destUsername, destPassword);) {
			// https://jdbc.postgresql.org/documentation/head/query.html#query-with-cursor
			sourceConn.setAutoCommit(false);
			if (sourcePagingSize > 0) {
				System.out.println("Setting fetch page size to " + sourcePagingSize);
				((org.postgresql.PGConnection) sourceConn).setDefaultFetchSize(sourcePagingSize);
				((org.postgresql.PGConnection) destConn).setDefaultFetchSize(sourcePagingSize);
			}
			// Following examples from:
			// http://postgis.refractions.net:80/documentation/manual-1.4/ch05.html#id2765827
			((org.postgresql.PGConnection) sourceConn).addDataType("geometry", org.postgis.PGgeometry.class);
			((org.postgresql.PGConnection) sourceConn).addDataType("box3d", org.postgis.PGbox3d.class);
			((org.postgresql.PGConnection) destConn).addDataType("geometry", org.postgis.PGgeometry.class);
			((org.postgresql.PGConnection) destConn).addDataType("box3d", org.postgis.PGbox3d.class);

			// TODO: Actually page through by adding LIMIT and OFFSET since Postgres (at
			// least 8.3/8.4) doesn't seem to understand fetch size parameter
			int lastRowCount = 1;
			AtomicLong totalRowCount = new AtomicLong(0);
			int nextLimit = sourcePagingSize;
			int nextOffset = 0;

			String trimmedOriginalQuery = sourceSelectQuery.trim();
			// if (trimmedOriginalQuery.endsWith(";")) {
			// trimmedOriginalQuery = trimmedOriginalQuery.substring(0,
			// trimmedOriginalQuery.length() - 1);
			// System.out.println("Trimmed semi-colon to add limit and offset: " +
			// trimmedOriginalQuery);
			// }

			long startTime = System.currentTimeMillis();

			// do {
			// String nextSelectQuery = trimmedOriginalQuery + " LIMIT " + nextLimit + "
			// OFFSET " + nextOffset + ";";
			String nextSelectQuery = sourceSelectQuery;
			lastRowCount = queryPage(nextSelectQuery, sourcePagingSize, destMaxId, destInsertQuery, debug, targetName,
					sourceConn, destConn, startTime, totalRowCount);
			// nextOffset += nextLimit;
			// totalRowCount += lastRowCount;

			// } while (lastRowCount > 0 && sourcePagingSize > 0);

			double secondsSinceStart = (System.currentTimeMillis() - startTime) / 1000.0d;
			System.out.printf("%d\tSeconds since start: %f\tRecords per second: %f%n", totalRowCount.get(), secondsSinceStart,
					totalRowCount.get() / secondsSinceStart);
			System.out.println("Completed syncing for " + targetName + " processed " + totalRowCount.get() + " rows");
		}
	}

	public static int queryPage(String sourceSelectQuery, int sourcePagingSize, int destMaxId, String destInsertQuery,
			boolean debug, String targetName, Connection sourceConn, Connection destConn, long startTime,
			AtomicLong totalRowCount) throws SQLException, RuntimeException {
		int rowCounter = 0;
		try (PreparedStatement sourceSelectStatement = sourceConn.prepareStatement(sourceSelectQuery,
				ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.FETCH_FORWARD);
				PreparedStatement destInsertStatement = destConn.prepareStatement(destInsertQuery);) {

			if (sourcePagingSize > 0) {
				// https://jdbc.postgresql.org/documentation/head/query.html#query-with-cursor
				sourceSelectStatement.setFetchSize(sourcePagingSize);
			}
			if (sourceSelectStatement.getParameterMetaData().getParameterCount() > 0) {
				sourceSelectStatement.setInt(1, destMaxId);
			}
			if (debug) {
				System.out.println("Executing select statement: " + sourceSelectStatement.toString());
			}
			try (ResultSet selectResults = sourceSelectStatement.executeQuery();) {
				if (debug) {
					System.out.println("Statement executed, checking metadata");
				}
				ResultSetMetaData selectMetadata = selectResults.getMetaData();
				int selectColumns = selectMetadata.getColumnCount();
				if (selectColumns < 1) {
					throw new RuntimeException("The select query (" + targetName + ") did not return any columns");
				}
				if (debug) {
					System.out.println("Iterating over results...");
				}
				while (selectResults.next()) {
					rowCounter++;
					totalRowCount.incrementAndGet();
					if (debug) {
						System.out.println("Processing row: " + rowCounter);
					}
					IntStream.range(1, selectColumns + 1).forEachOrdered(Unchecked.intConsumer(i -> {
						String typeName = selectMetadata.getColumnTypeName(i);
						if ("geometry".equals(typeName)) {
							PGgeometry geom = (PGgeometry) selectResults.getObject(i);
							destInsertStatement.setObject(i, geom);
							// destUpdateStatement.setObject(i, geom);
						} else if ("int4".equals(typeName)) {
							destInsertStatement.setInt(i, selectResults.getInt(i));
							// destUpdateStatement.setInt(i, selectResults.getInt(i));
							// if (i == selectIdFieldIndex) {
							// destUpdateStatement.setInt(selectColumns + 1, selectResults.getInt(i));
							// }
						} else if ("float8".equals(typeName)) {
							destInsertStatement.setFloat(i, selectResults.getFloat(i));
							// destUpdateStatement.setFloat(i, selectResults.getFloat(i));
						} else if ("bool".equals(typeName)) {
							destInsertStatement.setBoolean(i, selectResults.getBoolean(i));
							// destUpdateStatement.setBoolean(i, selectResults.getBoolean(i));
						} else if ("varchar".equals(typeName)) {
							String rawString = selectResults.getString(i);
							if (debug) {
								String outputString = rawString;
								if (outputString == null) {
									outputString = "";
								}
								if (outputString.length() > 100) {
									outputString = outputString.substring(0, 100) + "...";
								}
								System.out.println(selectMetadata.getColumnName(i) + "=" + outputString + " (as "
										+ selectMetadata.getColumnTypeName(i) + ")");
							}
							destInsertStatement.setString(i, rawString);
							// destUpdateStatement.setString(i, rawString);
							// if (i == selectIdFieldIndex) {
							// destUpdateStatement.setString(selectColumns + 1, rawString);
							// }
						} else {
							throw new RuntimeException("Unsupported type: " + typeName + " for column "
									+ selectMetadata.getColumnName(i) + " (" + targetName + ")");
						}
					}));
					if (debug) {
						System.out.println();
					}
					try {
						if (debug) {
							System.out.println(
									"Executing insert query on destination: " + destInsertStatement.toString());
						}
						destInsertStatement.execute();
					} catch (SQLException e) {
						e.printStackTrace();
						System.err.println(
								"Found exception inserting line: " + rowCounter + " " + destInsertStatement.toString());
						throw e;
						// try {
						// destUpdateStatement.execute();
						// } catch (SQLException e1) {
						//
						// if (debug) {
						// e1.printStackTrace();
						// System.err.println("Also found exception updating line: " + rowCounter +
						// "!");
						// }
						//
						// }
					} finally {
						destInsertStatement.clearParameters();
						// destUpdateStatement.clearParameters();

						if(totalRowCount.get() % 100 == 0) {
							double secondsSinceStart = (System.currentTimeMillis() - startTime) / 1000.0d;
							System.out.printf("%d\tSeconds since start: %f\tRecords per second: %f%n", totalRowCount.get(),
									secondsSinceStart, totalRowCount.get() / secondsSinceStart);
						}
					}
				}
			}
		}
		return rowCounter;
	}

	/**
	 * Executes a query to get the maximum value.
	 * 
	 * @param nextJDBCUrl
	 *            The JDBC connection string
	 * @param nextUsername
	 *            The username to use
	 * @param nextPassword
	 *            The password to use
	 * @param maxQuery
	 *            The query to get the maximum value needed to determine whether
	 *            syncing is required and if so where to start and end from.
	 * @param debug
	 *            True to emit debugging information and false otherwise
	 * @param targetName
	 *            The name to use in debugging for this target
	 * @return The maximum id as an integer
	 * @throws SQLException
	 *             If there is a SQL error
	 * @throws RuntimeException
	 *             If there is an unknown error
	 * @throws NumberFormatException
	 *             If there is an error converting the maximum id to an integer
	 */
	public static int executeMaxQuery(String nextJDBCUrl, String nextUsername, String nextPassword, String maxQuery,
			boolean debug, String targetName) throws SQLException, RuntimeException, NumberFormatException {
		int result = -1;
		try (Connection nextConn = DriverManager.getConnection(nextJDBCUrl, nextUsername, nextPassword);
				PreparedStatement nextMaxStatement = nextConn.prepareStatement(maxQuery);
				ResultSet maxResults = nextMaxStatement.executeQuery();) {
			ResultSetMetaData maxMetadata = maxResults.getMetaData();
			int maxColumns = maxMetadata.getColumnCount();
			if (maxColumns != 1) {
				throw new RuntimeException("The max query (" + targetName + ") did not return a single column");
			}
			while (maxResults.next()) {
				if (debug) {
					IntStream.range(1, maxColumns + 1)
							.forEachOrdered(Unchecked.intConsumer(i -> System.out.println(maxMetadata.getColumnName(i)
									+ "=" + maxResults.getString(i) + " as " + maxMetadata.getColumnTypeName(i))));
					System.out.println();
				}

				result = Integer.parseInt(maxResults.getString(1));
				System.out.println("Max id (" + targetName + ") = " + result);
			}
		}
		return result;
	}
}
