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
import com.facebook.presto.spi.RecordSet;

import java.util.List;

import static com.facebook.presto.mysql.MySQLColumnHandle.mySQLFullTypeGetter;
import static com.facebook.presto.mysql.MySQLColumnHandle.nativeTypeGetter;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Lists.transform;

public class MySQLRecordSet
        implements RecordSet
{
    private final String cql;
    private final List<FullMySQLType> mySQLTypes;
    private final List<ColumnType> columnTypes;
    private final MySQLSession mySQLSession;

    public MySQLRecordSet(MySQLSession mySQLSession, String cql, List<MySQLColumnHandle> mySQLColumns)
    {
        this.mySQLSession = checkNotNull(mySQLSession, "mySQLSession is null");
        this.cql = checkNotNull(cql, "cql is null");
        checkNotNull(mySQLColumns, "mySQLColumns is null");
        this.mySQLTypes = transform(mySQLColumns, mySQLFullTypeGetter());
        this.columnTypes = transform(mySQLColumns, nativeTypeGetter());
    }

    @Override
    public List<ColumnType> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        return new MySQLRecordCursor(mySQLSession, mySQLTypes, cql);
    }
}
