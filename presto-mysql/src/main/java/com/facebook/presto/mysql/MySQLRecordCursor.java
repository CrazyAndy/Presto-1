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

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.facebook.presto.spi.ColumnType;
import com.facebook.presto.spi.RecordCursor;
import com.google.common.base.Charsets;

import java.util.List;

public class MySQLRecordCursor
        implements RecordCursor
{
    private final List<FullMySQLType> fullMySQLTypes;
    private final ResultSet rs;
    private Row currentRow;
    private long atLeastCount;
    private long count;

    public MySQLRecordCursor(MySQLSession mySQLSession,
            List<FullMySQLType> fullMySQLTypes, String cql)
    {
        this.fullMySQLTypes = fullMySQLTypes;
        rs = (ResultSet) mySQLSession.executeQuery(cql);
        currentRow = null;
        atLeastCount = rs.getAvailableWithoutFetching();
    }

    @Override
    public boolean advanceNextPosition()
    {
        if (!rs.isExhausted()) {
            currentRow = rs.one();
            count++;
            atLeastCount = count + rs.getAvailableWithoutFetching();
            return true;
        }
        return false;
    }

    @Override
    public void close()
    {
    }

    @Override
    public boolean getBoolean(int i)
    {
        return currentRow.getBool(i);
    }

    @Override
    public long getCompletedBytes()
    {
        return count;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public double getDouble(int i)
    {
        switch (getMySQLType(i)) {
            case DOUBLE:
                return currentRow.getDouble(i);
            case FLOAT:
                return currentRow.getFloat(i);
            case DECIMAL:
                return currentRow.getDecimal(i).doubleValue();
            default:
                throw new IllegalStateException("Cannot retrieve double for " + getMySQLType(i));
        }
    }

    @Override
    public long getLong(int i)
    {
        switch (getMySQLType(i)) {
            case INT:
                return currentRow.getInt(i);
            case BIGINT:
            case COUNTER:
                return currentRow.getLong(i);
            case TIMESTAMP:
                return currentRow.getDate(i).getTime();
            default:
                throw new IllegalStateException("Cannot retrieve long for " + getMySQLType(i));
        }
    }

    private MYSQLType getMySQLType(int i)
    {
        return fullMySQLTypes.get(i).getMySQLType();
    }

    @Override
    public byte[] getString(int i)
    {
        String str = MYSQLType.getColumnValue(currentRow, i, fullMySQLTypes.get(i)).toString();
        return str.getBytes(Charsets.UTF_8);
    }

    @Override
    public long getTotalBytes()
    {
        return atLeastCount;
    }

    @Override
    public ColumnType getType(int i)
    {
        return getMySQLType(i).getNativeType();
    }

    @Override
    public boolean isNull(int i)
    {
        return currentRow.isNull(i);
    }
}
