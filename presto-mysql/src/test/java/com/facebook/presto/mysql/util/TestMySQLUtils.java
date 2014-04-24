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
package com.facebook.presto.mysql.util;

import com.facebook.presto.mysql.MYSQLType;
import com.facebook.presto.mysql.MySQLColumnHandle;
import com.google.common.collect.ImmutableList;

import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.mysql.util.MySQLUtils.quoteStringLiteral;
import static com.facebook.presto.mysql.util.MySQLUtils.validColumnName;
import static com.facebook.presto.mysql.util.MySQLUtils.validSchemaName;
import static com.facebook.presto.mysql.util.MySQLUtils.validTableName;
import static org.testng.Assert.assertEquals;

public class TestMySQLUtils
{
    @Test
    public void testValidSchemaName()
    {
        assertEquals("foo", validSchemaName("foo"));
        assertEquals("\"select\"", validSchemaName("select"));
    }

    @Test
    public void testValidTableName()
    {
        assertEquals("foo", validTableName("foo"));
        assertEquals("\"Foo\"", validTableName("Foo"));
        assertEquals("\"select\"", validTableName("select"));
    }

    @Test
    public void testValidColumnName()
    {
        assertEquals("foo", validColumnName("foo"));
        assertEquals("\"\"", validColumnName(MySQLUtils.EMPTY_COLUMN_NAME));
        assertEquals("\"\"", validColumnName(""));
        assertEquals("\"select\"", validColumnName("select"));
    }

    @Test
    public void testQuote()
    {
        assertEquals("'foo'", quoteStringLiteral("foo"));
        assertEquals("'Presto''s'", quoteStringLiteral("Presto's"));
    }

    @Test
    public void testAppendSelectColumns()
    {
        List<MySQLColumnHandle> columns = ImmutableList.of(
                new MySQLColumnHandle("", "foo", 0, MYSQLType.VARCHAR, null, false, false),
                new MySQLColumnHandle("", "bar", 0, MYSQLType.VARCHAR, null, false, false),
                new MySQLColumnHandle("", "table", 0, MYSQLType.VARCHAR, null, false, false));

        StringBuilder sb = new StringBuilder();
        MySQLUtils.appendSelectColumns(sb, columns);
        String str = sb.toString();

        assertEquals("foo,bar,\"table\"", str);
    }
}
