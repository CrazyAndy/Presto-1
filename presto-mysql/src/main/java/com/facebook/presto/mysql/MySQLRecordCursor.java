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

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.util.List;

import com.facebook.presto.spi.ColumnType;
import com.facebook.presto.spi.RecordCursor;
import com.google.common.base.Charsets;

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
    	
    	try {
    		if (rs != null)
    		{
    			rs.close();
    		}
		} catch (SQLException e) {
			e.printStackTrace();
		}
    }

    @Override
    public boolean getBoolean(int i)
    {
        try {
           return rs.getBoolean(i + 1);
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
            try {
                return rs.getDouble(i + 1);
            }
            catch (SQLException e) {
               e.printStackTrace();
               throw new IllegalStateException("Cannot retrieve double for " + getMySQLType(i));
            }
            
            case DECIMAL:
            case FLOAT:
            try {
               return rs.getFloat(i + 1);
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
            case INTEGER:
            case SMALLINT:
            case TINYINT:
            case MEDIUMINT:
                return rs.getInt(x);
            case BIGINT:
            case COUNTER:
                return rs.getLong(x);
            case DATE:
            case TIMESTAMP:
            case DATETIME:
            	
            	Date dt = rs.getDate(x);
            	
            	return dt != null ? dt.getTime() : 0;
            	
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
