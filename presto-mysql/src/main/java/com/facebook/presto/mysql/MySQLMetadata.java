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
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.NotFoundException;
import com.facebook.presto.spi.ReadOnlyConnectorMetadata;
import com.facebook.presto.spi.SchemaNotFoundException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.TableHandle;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.mysql.MySQLColumnHandle.columnMetadataGetter;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.transform;

public class MySQLMetadata
    extends ReadOnlyConnectorMetadata
{
    private final String connectorId;
    private final CachingMySQLSchemaProvider schemaProvider;

    @Inject
    public MySQLMetadata(MYSQLConnectorId connectorId, CachingMySQLSchemaProvider schemaProvider)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null").toString();
        this.schemaProvider = checkNotNull(schemaProvider, "schemaProvider is null");
    }

    @Override
    public List<String> listSchemaNames()
    {
        return schemaProvider.getAllSchemas();
    }

    @Override
    public MySQLTableHandle getTableHandle(SchemaTableName tableName)
    {
        checkNotNull(tableName, "tableName is null");
        try {
            MySQLTableHandle tableHandle = schemaProvider.getTableHandle(tableName);
            schemaProvider.getTable(tableHandle);
            return tableHandle;
        }
        catch (NotFoundException e) {
            // table was not found
            return null;
        }
    }

    private static SchemaTableName getTableName(TableHandle tableHandle)
    {
        checkArgument(tableHandle instanceof MySQLTableHandle, "tableHandle is not an instance of CassandraTableHandle");
        return ((MySQLTableHandle) tableHandle).getSchemaTableName();
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(TableHandle tableHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        SchemaTableName tableName = getTableName(tableHandle);
        return getTableMetadata(tableName);
    }

    private ConnectorTableMetadata getTableMetadata(SchemaTableName tableName)
    {
        MySQLTableHandle tableHandle = schemaProvider.getTableHandle(tableName);
        MySQLTable table = schemaProvider.getTable(tableHandle);
        List<ColumnMetadata> columns = ImmutableList.copyOf(transform(table.getColumns(), columnMetadataGetter()));
        return new ConnectorTableMetadata(tableName, columns);
    }

    @Override
    public List<SchemaTableName> listTables(String schemaNameOrNull)
    {
        ImmutableList.Builder<SchemaTableName> tableNames = ImmutableList.builder();
        for (String schemaName : listSchemas(schemaNameOrNull)) {
            try {
                for (String tableName : schemaProvider.getAllTables(schemaName)) {
                    tableNames.add(new SchemaTableName(schemaName, tableName.toLowerCase()));
                }
            }
            catch (SchemaNotFoundException e) {
                // schema disappeared during listing operation
            }
        }
        return tableNames.build();
    }

    private List<String> listSchemas(String schemaNameOrNull)
    {
        if (schemaNameOrNull == null) {
            return listSchemaNames();
        }
        return ImmutableList.of(schemaNameOrNull);
    }

    @Override
    public ColumnHandle getColumnHandle(TableHandle tableHandle, String columnName)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkNotNull(columnName, "columnName is null");
        return getColumnHandles(tableHandle).get(columnName);
    }

    @Override
    public ColumnHandle getSampleWeightColumnHandle(TableHandle tableHandle)
    {
        return null;
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(TableHandle tableHandle)
    {
        MySQLTable table = schemaProvider.getTable((MySQLTableHandle) tableHandle);
        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        for (MySQLColumnHandle columnHandle : table.getColumns()) {
            columnHandles.put(MySQLUtils.cqlNameToSqlName(columnHandle.getName()).toLowerCase(), columnHandle);
        }
        return columnHandles.build();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(SchemaTablePrefix prefix)
    {
        checkNotNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(prefix)) {
            try {
                columns.put(tableName, getTableMetadata(tableName).getColumns());
            }
            catch (NotFoundException e) {
                // table disappeared during listing operation
            }
        }
        return columns.build();
    }

    private List<SchemaTableName> listTables(SchemaTablePrefix prefix)
    {
        if (prefix.getSchemaName() == null) {
            return listTables(prefix.getSchemaName());
        }
        return ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
    }

    @Override
    public ColumnMetadata getColumnMetadata(TableHandle tableHandle, ColumnHandle columnHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkNotNull(columnHandle, "columnHandle is null");
        checkArgument(tableHandle instanceof MySQLTableHandle, "tableHandle is not an instance of MySQLTableHandle");
        checkArgument(columnHandle instanceof MySQLColumnHandle, "columnHandle is not an instance of MySQLColumnHandle");
        return ((MySQLColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public boolean canHandle(TableHandle tableHandle)
    {
        return tableHandle instanceof MySQLTableHandle && ((MySQLTableHandle) tableHandle).getConnectorId().equals(connectorId);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("connectorId", connectorId)
                .toString();
    }
}
