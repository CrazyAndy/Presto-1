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

import com.facebook.presto.spi.ColumnType;
import com.facebook.presto.spi.RecordCursor;
import com.google.common.base.Charsets;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class MySQLRecordCursor
        implements RecordCursor
{
    private final List<FullMySQLType> fullMySQLTypes;
    private final ResultSet rs;
    private long atLeastCount;
    private long count;

    public MySQLRecordCursor(MySQLSession mySQLSession,
            List<FullMySQLType> fullMySQLTypes, String cql)
    {
        this.fullMySQLTypes = fullMySQLTypes;
        rs = (ResultSet) mySQLSession.executeQuery(cql);
    }

    @Override
    public boolean advanceNextPosition()
    {
        try {
           return rs.next();
        }
        catch (SQLException e) {
           e.printStackTrace();
           return false;
        }
    }

    @Override
    public void close()
    {
    }

    @Override
    public boolean getBoolean(int i)
    {
        try {
           return rs.getBoolean(i);
        }
        catch (SQLException e) {
          e.printStackTrace();
          return false;
        }
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
            case DECIMAL:
            try {
                return rs.getDouble(i);
            }
            catch (SQLException e) {
               e.printStackTrace();
               throw new IllegalStateException("Cannot retrieve double for " + getMySQLType(i));
            }
            case FLOAT:
            try {
               return rs.getFloat(i);
            }
            catch (SQLException e) {
              e.printStackTrace();
              throw new IllegalStateException("Cannot retrieve double for " + getMySQLType(i));
            }
            default:
                throw new IllegalStateException("Cannot retrieve double for " + getMySQLType(i));
        }
    }

    @Override
    public long getLong(int i)
    {
        int x = i + 1;
        try {
          switch (getMySQLType(i)) {
            case INT:
                return rs.getInt(x);
            case BIGINT:
            case COUNTER:
                return rs.getLong(x);
            case TIMESTAMP:
                return rs.getDate(x).getTime();
            default:
                throw new IllegalStateException("Cannot retrieve long for " + getMySQLType(i));
           }
        }
        catch (SQLException e) {
            e.printStackTrace();
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
        int x = i + 1;
        
        Comparable<?> value = MYSQLType.getMySQLColumnValue(rs, x, getMySQLType(i), null);
        
        String str = value == null ? "" : value.toString();
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
        return (rs == null);
    }
}
