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

import com.facebook.presto.mysql.util.MySQLUtils;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.Split;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;

import io.airlift.log.Logger;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.transform;

public class MySQLRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private static final Logger log = Logger.get(ConnectorRecordSetProvider.class);

    private final String connectorId;
    private final MySQLSession mySQLSession;

    @Inject
    public MySQLRecordSetProvider(MYSQLConnectorId connectorId, MySQLSession mySQLSession)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null").toString();
        this.mySQLSession = checkNotNull(mySQLSession, "mySQLSession is null");
    }

    @Override
    public RecordSet getRecordSet(Split split, List<? extends ColumnHandle> columns)
    {
        checkNotNull(split, "split is null");
        checkArgument(split instanceof MySQLSplit, "expected instance of %s: %s", MySQLSplit.class, split.getClass());
        MySQLSplit mySQLSplit = (MySQLSplit) split;

        checkNotNull(columns, "columns is null");
        List<MySQLColumnHandle> mySQLColumns = ImmutableList.copyOf(transform(columns, MySQLColumnHandle.mySQLColumnHandle()));

        String selectCql = MySQLUtils.selectFrom(mySQLSplit.getMySQLTableHandle(), mySQLColumns).getQueryString();
        StringBuilder sb = new StringBuilder(selectCql);
        if (sb.charAt(sb.length() - 1) == ';') {
            sb.setLength(sb.length() - 1);
        }
        sb.append(mySQLSplit.getWhereClause());
        String cql = sb.toString();
        log.debug("Creating record set: %s", cql);

        return new MySQLRecordSet(mySQLSession, cql, mySQLColumns);
    }

    @Override
    public boolean canHandle(Split split)
    {
        return split instanceof MySQLSplit && ((MySQLSplit) split).getConnectorId().equals(connectorId);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("connectorId", connectorId)
                .toString();
    }
}
