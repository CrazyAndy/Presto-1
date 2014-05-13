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

import com.datastax.driver.core.ConsistencyLevel;
import javax.inject.Inject;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class MySQLSessionFactory
{
    private final MYSQLConnectorId connectorId;
    private final MySQLClientConfig config;

    @Inject
    public MySQLSessionFactory(MYSQLConnectorId connectorId, MySQLClientConfig config)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null");
        checkNotNull(config, "config is null");

        this.config = config;
    }

    public MySQLSession create()
    {
        return new MySQLSession(connectorId.toString(), config);
    }
}
