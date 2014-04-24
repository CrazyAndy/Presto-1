/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.mysql;

import com.facebook.presto.spi.SchemaNotFoundException;
import com.facebook.presto.spi.TableNotFoundException;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.airlift.units.Duration;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.mysql.MockMySQLSession.BAD_SCHEMA;
import static com.facebook.presto.mysql.MockMySQLSession.TEST_SCHEMA;
import static com.facebook.presto.mysql.MockMySQLSession.TEST_TABLE;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

@Test(singleThreaded = true)
public class TestCachingMySQLSchemaProvider
{
    private static final String CONNECTOR_ID = "test-cassandra";
    private MockMySQLSession mockSession;
    private CachingMySQLSchemaProvider schemaProvider;

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        mockSession = new MockMySQLSession(CONNECTOR_ID);
        ListeningExecutorService executor = listeningDecorator(newCachedThreadPool(new ThreadFactoryBuilder().setDaemon(true).build()));
        schemaProvider = new CachingMySQLSchemaProvider(
                CONNECTOR_ID,
                mockSession,
                executor,
                new Duration(5, TimeUnit.MINUTES),
                new Duration(1, TimeUnit.MINUTES));
    }

    @Test
    public void testGetAllDatabases()
            throws Exception
    {
        assertEquals(mockSession.getAccessCount(), 0);
        assertEquals(schemaProvider.getAllSchemas(), ImmutableList.of(TEST_SCHEMA));
        assertEquals(mockSession.getAccessCount(), 1);
        assertEquals(schemaProvider.getAllSchemas(), ImmutableList.of(TEST_SCHEMA));
        assertEquals(mockSession.getAccessCount(), 1);

        schemaProvider.flushCache();

        assertEquals(schemaProvider.getAllSchemas(), ImmutableList.of(TEST_SCHEMA));
        assertEquals(mockSession.getAccessCount(), 2);
    }

    @Test
    public void testGetAllTable()
            throws Exception
    {
        assertEquals(mockSession.getAccessCount(), 0);
        assertEquals(schemaProvider.getAllTables(TEST_SCHEMA), ImmutableList.of(TEST_TABLE));
        assertEquals(mockSession.getAccessCount(), 2);
        assertEquals(schemaProvider.getAllTables(TEST_SCHEMA), ImmutableList.of(TEST_TABLE));
        assertEquals(mockSession.getAccessCount(), 2);

        schemaProvider.flushCache();

        assertEquals(schemaProvider.getAllTables(TEST_SCHEMA), ImmutableList.of(TEST_TABLE));
        assertEquals(mockSession.getAccessCount(), 4);
    }

    @Test(expectedExceptions = SchemaNotFoundException.class)
    public void testInvalidDbGetAllTAbles()
            throws Exception
    {
        schemaProvider.getAllTables(BAD_SCHEMA);
    }

    @Test
    public void testGetTable()
            throws Exception
    {
        MySQLTableHandle tableHandle = new MySQLTableHandle(CONNECTOR_ID, TEST_SCHEMA, TEST_TABLE);
        assertEquals(mockSession.getAccessCount(), 0);
        assertNotNull(schemaProvider.getTable(tableHandle));
        assertEquals(mockSession.getAccessCount(), 1);
        assertNotNull(schemaProvider.getTable(tableHandle));
        assertEquals(mockSession.getAccessCount(), 1);

        schemaProvider.flushCache();

        assertNotNull(schemaProvider.getTable(tableHandle));
        assertEquals(mockSession.getAccessCount(), 2);
    }

    @Test(expectedExceptions = TableNotFoundException.class)
    public void testInvalidDbGetTable()
            throws Exception
    {
        MySQLTableHandle tableHandle = new MySQLTableHandle(CONNECTOR_ID, BAD_SCHEMA, TEST_TABLE);
        schemaProvider.getTable(tableHandle);
    }

    @Test
    public void testGetPartitions()
            throws Exception
    {
        MySQLTableHandle tableHandle = new MySQLTableHandle(CONNECTOR_ID, TEST_SCHEMA, TEST_TABLE);
        assertEquals(mockSession.getAccessCount(), 0);

        MySQLTable table = schemaProvider.getTable(tableHandle);
        assertNotNull(table);

        String expectedList = "[column1 = 'testpartition1', column1 = 'testpartition2']";

        List<Comparable<?>> empty = ImmutableList.of();
        assertEquals(mockSession.getAccessCount(), 1);
        assertEquals(expectedList, schemaProvider.getPartitions(table, empty).toString());
        assertEquals(mockSession.getAccessCount(), 2);
        assertEquals(expectedList, schemaProvider.getPartitions(table, empty).toString());
        assertEquals(mockSession.getAccessCount(), 2);

        schemaProvider.flushCache();

        assertEquals(expectedList, schemaProvider.getPartitions(table, empty).toString());
        assertEquals(mockSession.getAccessCount(), 3);
    }

    @Test
    public void testNoCacheExceptions()
            throws Exception
    {
        // Throw exceptions on usage
        mockSession.setThrowException(true);
        try {
            schemaProvider.getAllSchemas();
        }
        catch (RuntimeException ignored) {
        }
        assertEquals(mockSession.getAccessCount(), 1);

        // Second try should hit the client again
        try {
            schemaProvider.getAllSchemas();
        }
        catch (RuntimeException ignored) {
        }
        assertEquals(mockSession.getAccessCount(), 2);
    }
}
