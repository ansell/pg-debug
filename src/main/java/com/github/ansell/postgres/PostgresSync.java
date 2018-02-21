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
import java.io.FileNotFoundException;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.commons.io.output.NullWriter;
import org.geotools.data.FileDataStore;
import org.geotools.data.FileDataStoreFinder;
import org.geotools.data.collection.CollectionFeatureSource;
import org.geotools.data.collection.ListFeatureCollection;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.feature.NameImpl;
import org.geotools.map.FeatureLayer;
import org.geotools.map.Layer;
import org.geotools.map.MapContent;
import org.geotools.styling.SLD;
import org.geotools.styling.Style;
import org.jooq.lambda.Unchecked;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.feature.type.Name;
import com.fasterxml.jackson.databind.SequenceWriter;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.github.ansell.csv.stream.CSVStream;
import com.github.ansell.csv.sum.CSVSummariser;
import com.github.ansell.csv.util.CSVUtil;

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

		final OptionSpec<String> destJDBCOption = parser.accepts("dest-jdbc").withRequiredArg().ofType(String.class)
				.required().describedAs("The JDBC connection string for the destination database");
		final OptionSpec<String> destUsernameOption = parser.accepts("dest-username").withRequiredArg()
				.ofType(String.class).required().describedAs("The JDBC username for the destination database");
		final OptionSpec<String> destPasswordOption = parser.accepts("dest-password").withRequiredArg()
				.ofType(String.class).required().describedAs("The JDBC password for the destination database");

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

		String destJDBCUrl = destJDBCOption.value(options);
		String destUsername = destUsernameOption.value(options);
		String destPassword = destPasswordOption.value(options);

		try (Connection sourceConn = DriverManager.getConnection(sourceJDBCUrl, sourceUsername, sourcePassword);
				Connection destConn = DriverManager.getConnection(destJDBCUrl, destUsername, destPassword);) {

		}
	}
}
