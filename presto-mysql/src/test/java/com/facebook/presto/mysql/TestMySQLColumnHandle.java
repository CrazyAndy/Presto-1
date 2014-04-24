package com.facebook.presto.mysql;

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

import com.google.common.collect.ImmutableList;

import io.airlift.json.JsonCodec;

import org.testng.annotations.Test;

import static io.airlift.json.JsonCodec.jsonCodec;
import static org.testng.Assert.assertEquals;

public class TestMySQLColumnHandle
{
    private final JsonCodec<MySQLColumnHandle> codec = jsonCodec(MySQLColumnHandle.class);

    @Test
    public void testRoundTrip()
    {
        MySQLColumnHandle expected = new MySQLColumnHandle("connector", "name", 42, MYSQLType.FLOAT, null, true, false);

        String json = codec.toJson(expected);
        MySQLColumnHandle actual = codec.fromJson(json);

        assertEquals(actual.getConnectorId(), expected.getConnectorId());
        assertEquals(actual.getName(), expected.getName());
        assertEquals(actual.getOrdinalPosition(), expected.getOrdinalPosition());
        assertEquals(actual.getMySQLType(), expected.getMySQLType());
        assertEquals(actual.isPartitionKey(), expected.isPartitionKey());
        assertEquals(actual.isClusteringKey(), expected.isClusteringKey());
    }

    @Test
    public void testRoundTrip2()
    {
        MySQLColumnHandle expected = new MySQLColumnHandle(
                "connector",
                "name2",
                1,
                MYSQLType.MAP,
                ImmutableList.of(MYSQLType.VARCHAR, MYSQLType.UUID),
                false,
                true);

        String json = codec.toJson(expected);
        MySQLColumnHandle actual = codec.fromJson(json);

        assertEquals(actual.getConnectorId(), expected.getConnectorId());
        assertEquals(actual.getName(), expected.getName());
        assertEquals(actual.getOrdinalPosition(), expected.getOrdinalPosition());
        assertEquals(actual.getMySQLType(), expected.getMySQLType());
        assertEquals(actual.isPartitionKey(), expected.isPartitionKey());
        assertEquals(actual.isClusteringKey(), expected.isClusteringKey());
    }
}
