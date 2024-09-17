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
	public void testEmbeddedPgWithConnectionPooling() throws Exception {
		EmbeddedPostgres pg = EmbeddedPostgres.builder().setPooling(true).start();
		HikariConfig config = new HikariConfig();
		config.setJdbcUrl(pg.getJdbcUrl("postgres", "postgres"));
		config.setMaximumPoolSize(5);
		config.setConnectionTestQuery("SELECT 1");
		config.setUsername("postgres");
		config.setPassword("postgres");
		config.setDriverClassName("org.postgresql.Driver");

		try (
				HikariDataSource ds = new HikariDataSource(config);
				Connection c = ds.getConnection()) {
			Statement s = c.createStatement();
			ResultSet rs = s.executeQuery("SELECT 1");
			assertTrue(rs.next());
			assertEquals(1, rs.getInt(1));
			assertFalse(rs.next());
		}

		pg.close();
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
		EmbeddedPostgres pg1 = EmbeddedPostgres.builder().setPooling(true)
				.setUsername("user1").setDbName("db1")
				.start();
		EmbeddedPostgres pg2 = EmbeddedPostgres.builder().setPooling(true)
				.setUsername("user2").setDbName("db2")
				.start();
		try {
			PGConnectionPoolDataSource ds1 = pg1.getPostgresPooledDatabase();
			PGConnectionPoolDataSource ds2 = pg2.getPostgresPooledDatabase();
			assertNotEquals(pg1.getPort(), pg2.getPort());
			assertEquals("user1", ds1.getUser());
			assertEquals("db1", ds1.getDatabaseName());
			assertEquals("user2", ds2.getUser());
			assertEquals("db2", ds2.getDatabaseName());
			try (Connection conn1 = ds1.getConnection();
					Connection conn2 = ds2.getConnection()) {
				assertTrue(conn1.isValid(5));
				assertTrue(conn2.isValid(5));
			}
		} finally {
			pg1.close();
			pg2.close();
		}
	}
}
