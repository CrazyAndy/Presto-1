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

import com.facebook.presto.spi.HostAddress;
import com.google.common.collect.ImmutableList;

import io.airlift.json.JsonCodec;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TestMySQLSplit
{
    private final JsonCodec<MySQLSplit> codec = JsonCodec.jsonCodec(MySQLSplit.class);

    private final ImmutableList<HostAddress> addresses = ImmutableList.of(
            HostAddress.fromParts("127.0.0.1", 44),
            HostAddress.fromParts("127.0.0.1", 45));

    @Test
    public void testJsonRoundTrip()
    {
        MySQLSplit expected = new MySQLSplit("connectorId", "schema1", "table1", "partitionId", "condition", addresses);

        String json = codec.toJson(expected);
        MySQLSplit actual = codec.fromJson(json);

        assertEquals(actual.getConnectorId(), expected.getConnectorId());
        assertEquals(actual.getSchema(), expected.getSchema());
        assertEquals(actual.getTable(), expected.getTable());
        assertEquals(actual.getSplitCondition(), expected.getSplitCondition());
        assertEquals(actual.getAddresses(), expected.getAddresses());
    }

    @Test
    public void testWhereClause()
    {
        MySQLSplit split;
        split = new MySQLSplit(
                "connectorId",
                "schema1",
                "table1",
                MySQLPartition.UNPARTITIONED_ID,
                "token(k) >= 0 AND token(k) <= 2",
                addresses);
        assertEquals(split.getWhereClause(), " WHERE token(k) >= 0 AND token(k) <= 2");

        split = new MySQLSplit(
                "connectorId",
                "schema1",
                "table1",
                "key = 123",
                null,
                addresses);
        assertEquals(split.getWhereClause(), " WHERE key = 123");
    }
}
