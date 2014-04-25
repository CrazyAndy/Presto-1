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
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.facebook.presto.mysql.util.MySQLHost;
import com.facebook.presto.spi.SchemaTableName;

public class MySQLSession
{
    private Connection session = null;
    private Statement statement = null;
    protected final String connectorId;
    public MySQLSession(String connectorId)
    {
        this.connectorId = connectorId;
        try {
          Class.forName("com.mysql.jdbc.Driver");
        }
        catch (ClassNotFoundException e) {
            System.out.println("MySQL JDBC Driver Not Found");
            e.printStackTrace();
            return;
        }
        try {
            session = DriverManager.getConnection("jdbc:mysql://localhost:3306", "root", "");
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

    public Set<MySQLHost> getReplicas(String schema, ByteBuffer keyAsByteBuffer)
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
            ResultSet rs = md.getTables(null, null, "%", null);
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
    {}

    public MySQLTable getTable(SchemaTableName tableName)
    {
      return null;
    }

    public List<MySQLPartition> getPartitions(MySQLTable table, List<Comparable<?>> filterPrefix)
    {
      return null;
    }
}
