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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Select.Where;
import com.facebook.presto.mysql.util.MySQLHost;
import com.facebook.presto.mysql.util.MySQLUtils;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.collect.ImmutableList;

public class MySQLSession
{
    private Connection session = null;
    private Statement statement = null;
    protected final String connectorId;
    private final String connectionString = "jdbc:mysql://localhost:3306";
    private final String user = "root";
    private final String password = "";
    private final int limitForPartitionKeySelect;
    private final int fetchSizeForPartitionKeySelect;

    public MySQLSession(String connectorId, int fetchSizeForPartitionKeySelect, int limitForPartitionKeySelect)
    {
        this.connectorId = connectorId;
        this.fetchSizeForPartitionKeySelect = fetchSizeForPartitionKeySelect;
        this.limitForPartitionKeySelect = limitForPartitionKeySelect;
        try {
          Class.forName("com.mysql.jdbc.Driver");
        }
        catch (ClassNotFoundException e) {
            System.out.println("MySQL JDBC Driver Not Found");
            e.printStackTrace();
            return;
        }
        try {
            session = DriverManager.getConnection(connectionString, user , password);
        }
        catch (SQLException e) {
            System.out.println("Connection Failed! Check output console");
            e.printStackTrace();
            return;
        }
    }

    public ResultSet executeQuery(String cql)
    {
        try {
          statement = session.createStatement();
        }
        catch (SQLException e1) {
          e1.printStackTrace();
        }
        try {
          return statement.executeQuery(cql);
        }
        catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    public Collection<MySQLHost> getAllHosts()
    {
       List<MySQLHost> hosts = new ArrayList<MySQLHost>();
       try {
        MySQLHost localHost = new MySQLHost(InetAddress.getLocalHost());
        hosts.add(localHost);
       }
       catch (UnknownHostException e) {
        e.printStackTrace();
       }
       return hosts;
    }

    public Set<MySQLHost> getReplicas(String schema)
    {
        Set<MySQLHost> hosts = new HashSet<MySQLHost>();
        try {
         MySQLHost localHost = new MySQLHost(InetAddress.getLocalHost());
         hosts.add(localHost);
        }
        catch (UnknownHostException e) {
         e.printStackTrace();
        }
        return hosts;
    }

    public Iterable<String> getAllSchemas()
    {
        List<String> schemas = new ArrayList<String>();
        try {
            ResultSet rs = session.getMetaData().getCatalogs();
            while (rs.next()) {
              schemas.add(rs.getString("TABLE_CAT"));
            }
           }
           catch (Exception e) {
            e.printStackTrace();
           }
           return schemas;
    }

    public List<String> getAllTables(String caseSensitiveDatabaseName)
    {
        List<String> tables = new ArrayList<String>();
        try {
            DatabaseMetaData md = session.getMetaData();
            ResultSet rs = md.getTables(caseSensitiveDatabaseName, null, "%", null);
            while (rs.next()) {
              tables.add(rs.getString(3));
            }
           }
           catch (Exception e) {
            e.printStackTrace();
           }
           return tables;
    }

    public void getSchema(String databaseName)
    {
       try {
            DatabaseMetaData md = session.getMetaData();
            ResultSet rs = md.getSchemas(databaseName, null);
           }
           catch (Exception e) {
            e.printStackTrace();
           }
    }

    public MySQLTable getTable(SchemaTableName tableName)
    {
       MySQLTableHandle tableHandle = new MySQLTableHandle(connectorId, tableName.getSchemaName(), tableName.getTableName());
       List<MySQLColumnHandle> columnHandles = new ArrayList<MySQLColumnHandle>();
       try {
           // add primary keys first
           Set<String> primaryKeySet = new HashSet<>();
           int index = 0;
           Statement pKeySt = session.createStatement();
           String pKeyQuerySt = "select COLUMN_NAME, DATA_TYPE from information_schema.COLUMNS where (TABLE_SCHEMA = '" + tableName.getSchemaName() + "') AND (TABLE_NAME= '" + tableName.getTableName() + "') AND (COLUMN_KEY='PRI')";
           ResultSet rsetPKQuery = pKeySt.executeQuery(pKeyQuerySt);
           String colName = null;
           String colType = null;
           while (rsetPKQuery.next()) {
               colName = rsetPKQuery.getString("COLUMN_NAME");
               colType = rsetPKQuery.getString("DATA_TYPE");
               primaryKeySet.add(colName);
               MySQLColumnHandle columnHandle = buildColumnHandle(colName, colType, true, false, index++);
               columnHandles.add(columnHandle);
           }
           //add other columns next
           Statement st = session.createStatement();
           /*String querySt = "SELECT * FROM " + tableName.getSchemaName() + "." + tableName.getTableName();
           ResultSet rset = st.executeQuery(querySt);
           ResultSetMetaData md = rset.getMetaData();
           for (int i = 1; i <= md.getColumnCount(); i++) {
             colName = md.getColumnName(i);
             colType = md.getColumnTypeName(i);
             colTypeNum = md.getColumnType(i);
             System.out.println(colType);
             System.out.println(colName);
             System.out.println(colTypeNum);
             if (primaryKeySet.contains(colName)) {
               continue;
             }
             MySQLColumnHandle columnHandle = buildColumnHandle(colName, colTypeNum, false, false, index++);*/
             String querySt = "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '" + tableName.getSchemaName() + "' AND TABLE_NAME = '" + tableName.getTableName() + "'";
             ResultSet rset = st.executeQuery(querySt);
             while (rset.next()) {
                 colName = rset.getString("COLUMN_NAME");
                 colType = rset.getString("DATA_TYPE");
                 if (primaryKeySet.contains(colName)) {
                   continue;
                 }
                 MySQLColumnHandle columnHandle = buildColumnHandle(colName, colType, false, false, index++);
                 columnHandles.add(columnHandle);
             }
       }
       catch (Exception e) {
           e.printStackTrace();
       }
       MySQLTable returnTable = new MySQLTable(tableHandle, columnHandles);
       return returnTable;
    }

    private MySQLColumnHandle buildColumnHandle(String colName, String colType, boolean partitionKey, boolean clusteringKey, int index)
    {
        MYSQLType mySQLTypes = MYSQLType.getMySQLType(colType.toUpperCase());
        List<MYSQLType> typeArguments = null;
        /*MYSQLType mySQLTypes = MYSQLType.getMySQLType(columnMeta.getType().getName());
        if (mySQLTypes != null && mySQLTypes.getTypeArgumentSize() > 0) {
            List<DataType> typeArgs = columnMeta.getType().getTypeArguments();
            switch (mySQLTypes.getTypeArgumentSize()) {
                case 1:
                    typeArguments = ImmutableList.of(MYSQLType.getMySQLType(typeArgs.get(0).getName()));
                    break;
                case 2:
                    typeArguments = ImmutableList.of(MYSQLType.getMySQLType(typeArgs.get(0).getName()), MYSQLType.getMySQLType(typeArgs.get(1).getName()));
                    break;
                default:
                    throw new IllegalArgumentException("Invalid type arguments: " + typeArgs);
            }
        }*/
        return new MySQLColumnHandle(connectorId, colName, index, mySQLTypes, typeArguments, partitionKey, clusteringKey);
    }

    public List<MySQLPartition> getPartitions(MySQLTable table, List<Comparable<?>> filterPrefix)
    {
        ResultSet rows = queryPartitionKeys(table, filterPrefix);
        if (rows == null) {
            // just split the whole partition range
            return ImmutableList.of(MySQLPartition.UNPARTITIONED);
        }

        List<MySQLColumnHandle> partitionKeyColumns = table.getPartitionKeyColumns();

        HashMap<ColumnHandle, Comparable<?>> map = new HashMap<>();
        Set<String> uniquePartitionIds = new HashSet<>();
        StringBuilder stringBuilder = new StringBuilder();

        boolean isComposite = partitionKeyColumns.size() > 1;

        ImmutableList.Builder<MySQLPartition> partitions = ImmutableList.builder();
        try {
          while (rows.next()) {
                map.clear();
                stringBuilder.setLength(0);
                for (int i = 0; i < partitionKeyColumns.size(); i++) {
                    MySQLColumnHandle columnHandle = partitionKeyColumns.get(i);
                    Comparable<?> keyPart = MYSQLType.getMySQLColumnValue(rows, i + 1, columnHandle.getMySQLType(), columnHandle.getTypeArguments());
                    map.put(columnHandle, keyPart);
                    if (i > 0) {
                        stringBuilder.append(" AND ");
                    }
                    stringBuilder.append(MySQLUtils.validColumnName(columnHandle.getName()));
                    stringBuilder.append(" = ");
                    stringBuilder.append(MYSQLType.getMySQLColumnStringValue(rows, i + 1, columnHandle.getMySQLType()));
                }
                TupleDomain tupleDomain = TupleDomain.withFixedValues(map);
                String partitionId = stringBuilder.toString();
                if (uniquePartitionIds.add(partitionId)) {
                    partitions.add(new MySQLPartition(partitionId, tupleDomain));
                }
            }
        }
        catch (Exception e) {
          e.printStackTrace();
        }
        return partitions.build();
    }

    protected ResultSet queryPartitionKeys(MySQLTable table, List<Comparable<?>> filterPrefix)
    {
        MySQLTableHandle tableHandle = table.getTableHandle();
        List<MySQLColumnHandle> partitionKeyColumns = table.getPartitionKeyColumns();
        boolean fullPartitionKey = filterPrefix.size() == partitionKeyColumns.size();
        ResultSet countRS = null;
        ResultSet partitionKey = null;
        try {
            if (!fullPartitionKey) {
                Select countAll = MySQLUtils.selectCountAllFrom(tableHandle).limit(limitForPartitionKeySelect);
                countRS = session.createStatement().executeQuery(countAll.getQueryString());
            }
            else {
                // no need to count if partition key is completely known
                countRS = null;
            }

            int limit = fullPartitionKey ? 1 : limitForPartitionKeySelect;
            Select partitionKeys = MySQLUtils.selectDistinctFrom(tableHandle, partitionKeyColumns);
            partitionKeys.limit(limit);
            partitionKeys.setFetchSize(fetchSizeForPartitionKeySelect);
            addWhereClause(partitionKeys.where(), partitionKeyColumns, filterPrefix);
            partitionKey = session.createStatement().executeQuery(partitionKeys.getQueryString());
            if (!fullPartitionKey) {
                long count;
                countRS.first();
                count = countRS.getLong("count(*)");
                if (count == limitForPartitionKeySelect) {
                    partitionKey.cancelRowUpdates();
                    return null; // too much effort to query all partition keys
                }
            }
        }
        catch (Exception e) {
          e.printStackTrace();
        }
        return partitionKey;
    }

    private void addWhereClause(Where where, List<MySQLColumnHandle> partitionKeyColumns, List<Comparable<?>> filterPrefix)
    {
        for (int i = 0; i < filterPrefix.size(); i++) {
            MySQLColumnHandle column = partitionKeyColumns.get(i);
            Object value = column.getMySQLType().getJavaValue(filterPrefix.get(i));
            Clause clause = QueryBuilder.eq(MySQLUtils.validColumnName(column.getName()), value);
            where.and(clause);
        }
    }
}
