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

		final OptionSpec<String> sourceJDBCOption = parser.accepts("source-jdbc").withRequiredArg().ofType(String.class)
				.required().describedAs("The JDBC connection string for the source database");
		final OptionSpec<String> sourceUsernameOption = parser.accepts("source-username").withRequiredArg()
				.ofType(String.class).required().describedAs("The JDBC username for the source database");
		final OptionSpec<String> sourcePasswordOption = parser.accepts("source-password").withRequiredArg()
				.ofType(String.class).required().describedAs("The JDBC password for the source database");
		final OptionSpec<String> sourceQueryOption = parser.accepts("source-query").withRequiredArg()
				.ofType(String.class).required().describedAs("The source query");

		final OptionSpec<String> destJDBCOption = parser.accepts("dest-jdbc").withRequiredArg().ofType(String.class)
				.describedAs("The JDBC connection string for the destination database");
		final OptionSpec<String> destUsernameOption = parser.accepts("dest-username").withRequiredArg()
				.ofType(String.class).describedAs("The JDBC username for the destination database");
		final OptionSpec<String> destPasswordOption = parser.accepts("dest-password").withRequiredArg()
				.ofType(String.class).describedAs("The JDBC password for the destination database");
		final OptionSpec<String> destQueryOption = parser.accepts("dest-query").withRequiredArg().ofType(String.class)
				.describedAs("The query on the destination");

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

		String sourceJDBCUrl = sourceJDBCOption.value(options);
		String sourceUsername = sourceUsernameOption.value(options);
		String sourcePassword = sourcePasswordOption.value(options);
		String sourceQuery = sourceQueryOption.value(options);

		String destJDBCUrl = destJDBCOption.value(options);
		String destUsername = destUsernameOption.value(options);
		String destPassword = destPasswordOption.value(options);
		String destQuery = destQueryOption.value(options);

		try (Connection sourceConn = DriverManager.getConnection(sourceJDBCUrl, sourceUsername, sourcePassword);
				PreparedStatement sourceStatement = sourceConn.prepareStatement(sourceQuery);
				ResultSet sourceResults = sourceStatement.executeQuery();) {
			ResultSetMetaData sourceMetadata = sourceResults.getMetaData();
			int sourceColumns = sourceMetadata.getColumnCount();
			while (sourceResults.next()) {
				IntStream.range(1, sourceColumns + 1).forEachOrdered(Unchecked.intConsumer(
						i -> System.out.println(sourceMetadata.getColumnName(i) + "=" + sourceResults.getString(i))));
				System.out.println();
			}
		}
	}
}
