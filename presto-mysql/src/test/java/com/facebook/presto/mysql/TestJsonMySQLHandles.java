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

import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import io.airlift.json.ObjectMapperProvider;

import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.testing.Assertions.assertEqualsIgnoreOrder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test
public class TestJsonMySQLHandles
{
    private static final Map<String, Object> TABLE_HANDLE_AS_MAP = ImmutableMap.<String, Object>of(
            "connectorId", "cassandra",
            "schemaName", "cassandra_schema",
            "tableName", "cassandra_table");

    private static final Map<String, Object> COLUMN_HANDLE_AS_MAP = ImmutableMap.<String, Object>builder()
            .put("connectorId", "cassandra")
            .put("name", "column")
            .put("ordinalPosition", 42)
            .put("cassandraType", "BIGINT")
            .put("partitionKey", false)
            .put("clusteringKey", true)
            .build();

    private static final Map<String, Object> COLUMN2_HANDLE_AS_MAP = ImmutableMap.<String, Object>builder()
            .put("connectorId", "cassandra")
            .put("name", "column2")
            .put("ordinalPosition", 0)
            .put("cassandraType", "SET")
            .put("typeArguments", ImmutableList.of("INT"))
            .put("partitionKey", false)
            .put("clusteringKey", false)
            .build();

    private final ObjectMapper objectMapper = new ObjectMapperProvider().get();

    @Test
    public void testTableHandleSerialize()
            throws Exception
    {
        MySQLTableHandle tableHandle = new MySQLTableHandle("cassandra", "cassandra_schema", "cassandra_table");

        assertTrue(objectMapper.canSerialize(MySQLTableHandle.class));
        String json = objectMapper.writeValueAsString(tableHandle);
        testJsonEquals(json, TABLE_HANDLE_AS_MAP);
    }

    @Test
    public void testTableHandleDeserialize()
            throws Exception
    {
        String json = objectMapper.writeValueAsString(TABLE_HANDLE_AS_MAP);

        MySQLTableHandle tableHandle = objectMapper.readValue(json, MySQLTableHandle.class);

        assertEquals(tableHandle.getConnectorId(), "cassandra");
        assertEquals(tableHandle.getSchemaName(), "cassandra_schema");
        assertEquals(tableHandle.getTableName(), "cassandra_table");
        assertEquals(tableHandle.getSchemaTableName(), new SchemaTableName("cassandra_schema", "cassandra_table"));
    }

    @Test
    public void testColumnHandleSerialize()
            throws Exception
    {
        MySQLColumnHandle columnHandle = new MySQLColumnHandle("cassandra", "column", 42, MYSQLType.BIGINT, null, false, true);

        assertTrue(objectMapper.canSerialize(MySQLColumnHandle.class));
        String json = objectMapper.writeValueAsString(columnHandle);
        testJsonEquals(json, COLUMN_HANDLE_AS_MAP);
    }

    @Test
    public void testColumn2HandleSerialize()
            throws Exception
    {
        MySQLColumnHandle columnHandle = new MySQLColumnHandle(
                "cassandra",
                "column2",
                0,
                MYSQLType.SET,
                ImmutableList.of(MYSQLType.INT),
                false,
                false);

        assertTrue(objectMapper.canSerialize(MySQLColumnHandle.class));
        String json = objectMapper.writeValueAsString(columnHandle);
        testJsonEquals(json, COLUMN2_HANDLE_AS_MAP);
    }

    @Test
    public void testColumnHandleDeserialize()
            throws Exception
    {
        String json = objectMapper.writeValueAsString(COLUMN_HANDLE_AS_MAP);

        MySQLColumnHandle columnHandle = objectMapper.readValue(json, MySQLColumnHandle.class);

        assertEquals(columnHandle.getName(), "column");
        assertEquals(columnHandle.getOrdinalPosition(), 42);
        assertEquals(columnHandle.getMySQLType(), MYSQLType.BIGINT);
        assertEquals(columnHandle.getTypeArguments(), null);
        assertEquals(columnHandle.isPartitionKey(), false);
        assertEquals(columnHandle.isClusteringKey(), true);
    }

    @Test
    public void testColumn2HandleDeserialize()
            throws Exception
    {
        String json = objectMapper.writeValueAsString(COLUMN2_HANDLE_AS_MAP);

        MySQLColumnHandle columnHandle = objectMapper.readValue(json, MySQLColumnHandle.class);

        assertEquals(columnHandle.getName(), "column2");
        assertEquals(columnHandle.getOrdinalPosition(), 0);
        assertEquals(columnHandle.getMySQLType(), MYSQLType.SET);
        assertEquals(columnHandle.getTypeArguments(), ImmutableList.of(MYSQLType.INT));
        assertEquals(columnHandle.isPartitionKey(), false);
        assertEquals(columnHandle.isClusteringKey(), false);
    }

    private void testJsonEquals(String json, Map<String, Object> expectedMap)
            throws Exception
    {
        Map<String, Object> jsonMap = objectMapper.readValue(json, new TypeReference<Map<String, Object>>() {});
        assertEqualsIgnoreOrder(jsonMap.entrySet(), expectedMap.entrySet());
    }
}
