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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;

import javax.inject.Singleton;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.airlift.configuration.ConfigurationModule.bindConfig;
import static org.weakref.jmx.ObjectNames.generatedNameOf;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class MySQLClientModule
        implements Module
{
    private final String connectorId;

    public MySQLClientModule(String connectorId)
    {
        this.connectorId = connectorId;
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(MYSQLConnectorId.class).toInstance(new MYSQLConnectorId(connectorId));
        binder.bind(MySQLConnector.class).in(Scopes.SINGLETON);
        binder.bind(MySQLMetadata.class).in(Scopes.SINGLETON);
        binder.bind(MySQLSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(MySQLRecordSetProvider.class).in(Scopes.SINGLETON);
        binder.bind(MySQLHandleResolver.class).in(Scopes.SINGLETON);

        bindConfig(binder).to(MySQLClientConfig.class);

        binder.bind(CachingMySQLSchemaProvider.class).in(Scopes.SINGLETON);
        newExporter(binder).export(CachingMySQLSchemaProvider.class).as(generatedNameOf(CachingMySQLSchemaProvider.class, connectorId));

        binder.bind(MySQLSessionFactory.class).in(Scopes.SINGLETON);
    }

    @ForMySQLSchema
    @Singleton
    @Provides
    public ExecutorService createCachingMySQLSchemaExecutor(MYSQLConnectorId clientId, MySQLClientConfig mysqlClientConfig)
    {
        return Executors.newFixedThreadPool(
                mysqlClientConfig.getMaxSchemaRefreshThreads(),
                new ThreadFactoryBuilder().setDaemon(true)
                        .setNameFormat("mysql-schema-" + clientId + "-%d").build());
    }

    @Singleton
    @Provides
    public MySQLSession createMySQLSession(MYSQLConnectorId connectorId, MySQLClientConfig config)
    {
        MySQLSessionFactory factory = new MySQLSessionFactory(connectorId, config);
        return factory.create();
    }
}
