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

import com.facebook.presto.mysql.util.MySQLHost;
/*import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row; */
import com.facebook.presto.spi.SchemaNotFoundException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/*import static com.datastax.driver.core.RowUtil.createSingleStringRow;*/

public class MockMySQLSession
        extends MySQLSession
{
    static final String TEST_SCHEMA = "testkeyspace";
    static final String BAD_SCHEMA = "badkeyspace";
    static final String TEST_TABLE = "testtbl";
    static final String TEST_COLUMN1 = "column1";
    static final String TEST_COLUMN2 = "column2";
    static final String TEST_PARTITION_KEY1 = "testpartition1";
    static final String TEST_PARTITION_KEY2 = "testpartition2";

    private final AtomicInteger accessCount = new AtomicInteger();
    private boolean throwException;

    public MockMySQLSession(String connectorId)
    {
       super(connectorId, new MySQLClientConfig());
       /* super(connectorId,
                null,
                new MySQLClientConfig().getFetchSizeForPartitionKeySelect(),
                new MySQLClientConfig().getLimitForPartitionKeySelect());*/
    }

    public void setThrowException(boolean throwException)
    {
        this.throwException = throwException;
    }

    public int getAccessCount()
    {
        return accessCount.get();
    }

    @Override
    public List<String> getAllSchemas()
    {
        accessCount.incrementAndGet();

        if (throwException) {
            throw new IllegalStateException();
        }
        return ImmutableList.of(TEST_SCHEMA);
    }

    @Override
    public List<String> getAllTables(String schema)
            throws SchemaNotFoundException
    {
        accessCount.incrementAndGet();
        if (throwException) {
            throw new IllegalStateException();
        }

        if (schema.equals(TEST_SCHEMA)) {
            return ImmutableList.of(TEST_TABLE);
        }
        throw new SchemaNotFoundException(schema);
    }

    @Override
    public void getSchema(String schema)
            throws SchemaNotFoundException
    {
        accessCount.incrementAndGet();
        if (throwException) {
            throw new IllegalStateException();
        }

        if (!schema.equals(TEST_SCHEMA)) {
            throw new SchemaNotFoundException(schema);
        }
    }

    @Override
    public MySQLTable getTable(SchemaTableName tableName)
            throws TableNotFoundException
    {
        accessCount.incrementAndGet();
        if (throwException) {
            throw new IllegalStateException();
        }

        if (tableName.getSchemaName().equals(TEST_SCHEMA) && tableName.getTableName().equals(TEST_TABLE)) {
            return new MySQLTable(
                    new MySQLTableHandle(connectorId, TEST_SCHEMA, TEST_TABLE),
                    ImmutableList.of(
                            new MySQLColumnHandle(connectorId, TEST_COLUMN1, 0, MYSQLType.VARCHAR, null, true, false),
                            new MySQLColumnHandle(connectorId, TEST_COLUMN2, 0, MYSQLType.INT, null, false, false)));
        }
        throw new TableNotFoundException(tableName);
    }

    @Override
    public List<MySQLPartition> getPartitions(MySQLTable table, List<Comparable<?>> filterPrefix)
    {
        accessCount.incrementAndGet();
        if (throwException) {
            throw new IllegalStateException();
        }

        return super.getPartitions(table, filterPrefix);
    }
/*
    @Override
    protected List<Row> queryPartitionKeys(MySQLTable table, List<Comparable<?>> filterPrefix)
    {
        MySQLTableHandle tableHandle = table.getTableHandle();
        if (tableHandle.getSchemaName().equals(TEST_SCHEMA) && tableHandle.getTableName().equals(TEST_TABLE)) {
            return ImmutableList.of(
                    createSingleStringRow(TEST_PARTITION_KEY1),
                    createSingleStringRow(TEST_PARTITION_KEY2));
        }
        throw new IllegalStateException();
    }*/


/*    @Override
    public ResultSet executeQuery(String cql)
    {
        throw new IllegalStateException("unexpected CQL query: " + cql);
    }*/
}
