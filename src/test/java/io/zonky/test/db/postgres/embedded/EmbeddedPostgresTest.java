/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zonky.test.db.postgres.embedded;

import static org.junit.Assert.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.postgresql.ds.PGConnectionPoolDataSource;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class EmbeddedPostgresTest {
	@TempDir
	public Path tf;

	@Test
	public void testEmbeddedPg() throws Exception {
		try (EmbeddedPostgres pg = EmbeddedPostgres.start();
				Connection c = pg.getPostgresDatabase().getConnection()) {
			Statement s = c.createStatement();
			ResultSet rs = s.executeQuery("SELECT 1");
			assertTrue(rs.next());
			assertEquals(1, rs.getInt(1));
			assertFalse(rs.next());
		}
	}

    	@Test
	public void testEmbeddedPgCreateUserAndDb() throws Exception {
	    try (EmbeddedPostgres pg = EmbeddedPostgres.builder().setUsername("alice").setDbName("first_db").start();
				Connection c = pg.getPostgresDatabase().getConnection()) {
			Statement s = c.createStatement();
			ResultSet rs = s.executeQuery("SELECT 1");
			assertTrue(rs.next());
			assertEquals(1, rs.getInt(1));
			assertFalse(rs.next());
		}
	}

	@Test
	public void testEmbeddedPgWithConnectionPooling() throws Exception {
		EmbeddedPostgres pg = EmbeddedPostgres.builder().setPooling(true).start();
		HikariConfig config = new HikariConfig();
		config.setJdbcUrl(pg.getJdbcUrl("postgres", "postgres"));
		config.setMaximumPoolSize(5);
		config.setDriverClassName("org.postgresql.Driver");

		try (HikariDataSource ds = new HikariDataSource(config);
				Connection c = ds.getConnection()) {
			Statement s = c.createStatement();
			ResultSet rs = s.executeQuery("SELECT 1");
			assertTrue(rs.next());
			assertEquals(1, rs.getInt(1));
			assertFalse(rs.next());
		} finally {
			pg.close();
		}
	}

	@Test
	public void testEmbeddedPgCreationWithNestedDataDirectory() throws Exception {
		try (EmbeddedPostgres pg = EmbeddedPostgres.builder()
				.setDataDirectory(Files.createDirectories(tf.resolve("data-dir-parent").resolve("data-dir")))
				.start()) {
			// nothing to do
		}
	}

	@Test
	public void testMultipleInstancesWithPooling() throws Exception {
		EmbeddedPostgres pg = EmbeddedPostgres.builder().setPooling(true).setDbName("first_db").start();
		HikariConfig config = new HikariConfig();
		config.setJdbcUrl(pg.getJdbcUrl("postgres", "first_db"));
		config.setMaximumPoolSize(5);
		config.setConnectionTestQuery("SELECT 1");
		config.setUsername("user1");
		config.setPassword("postgres");
		config.setDriverClassName("org.postgresql.Driver");

		EmbeddedPostgres pg2 = EmbeddedPostgres.builder().setPooling(true).setDbName("second_db").start();
		HikariConfig config2 = new HikariConfig();
		config2.setJdbcUrl(pg2.getJdbcUrl("postgres", "second_db"));
		config2.setMaximumPoolSize(5);
		config2.setConnectionTestQuery("SELECT 1");
		config2.setUsername("user2");
		config2.setPassword("postgres");
		config2.setDriverClassName("org.postgresql.Driver");

		try (HikariDataSource ds = new HikariDataSource(config);
				HikariDataSource ds2 = new HikariDataSource(config2);
				Connection c = ds.getConnection();
				Connection c2 = ds2.getConnection();
				Connection c2_2 = ds2.getConnection();) {
			Statement s = c.createStatement();
			ResultSet rs = s.executeQuery("SELECT datname FROM pg_database");
			Set<String> databaseNames = new HashSet<>();
			while (rs.next()) {
				databaseNames.add(rs.getString(1));
			}
			assertTrue(databaseNames.contains("first_db"));
			assertFalse(databaseNames.contains("second_db"));

			Statement s2 = c2.createStatement();
			ResultSet rs2 = s2.executeQuery("SELECT datname FROM pg_database");
			Set<String> databaseNames2 = new HashSet<>();
			while (rs2.next()) {
				databaseNames2.add(rs2.getString(1));
			}
			assertTrue(databaseNames2.contains("second_db"));
			assertFalse(databaseNames2.contains("first_db"));

			Statement s2_2 = c2_2.createStatement();
			ResultSet rs2_2 = s2_2.executeQuery("SELECT datname FROM pg_database");
			Set<String> databaseNames2_2 = new HashSet<>();
			while (rs2_2.next()) {
				databaseNames2_2.add(rs2_2.getString(1));
			}
			assertTrue(databaseNames2_2.contains("second_db"));
			assertFalse(databaseNames2_2.contains("first_db"));
		} finally {
			pg.close();
			pg2.close();
		}
	}
}
