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
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import io.airlift.configuration.Config;
import io.airlift.units.Duration;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class MySQLClientConfig
{
    private static final Splitter SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();

    private Duration schemaCacheTtl = new Duration(1, TimeUnit.HOURS);
    private Duration schemaRefreshInterval = new Duration(2, TimeUnit.MINUTES);
    private int maxSchemaRefreshThreads = 10;
    private int limitForPartitionKeySelect = 100_000;
    private int fetchSizeForPartitionKeySelect = 20_000;
    private int unpartitionedSplits = 1_000;
    private ConsistencyLevel consistencyLevel = ConsistencyLevel.ONE;
    private int fetchSize = 5_000;
    private String jdbcConnectionString;
    private String jdbcClassName;
    private String jdbcUserName;
    private String jdbcPassword;
    private String connectorName;

    @Min(0)
    public int getLimitForPartitionKeySelect()
    {
        return limitForPartitionKeySelect;
    }

    @Config("mysql.limit-for-partition-key-select")
    public MySQLClientConfig setLimitForPartitionKeySelect(int limitForPartitionKeySelect)
    {
        this.limitForPartitionKeySelect = limitForPartitionKeySelect;
        return this;
    }

    @Min(1)
    public int getUnpartitionedSplits()
    {
        return unpartitionedSplits;
    }

    @Config("mysql.unpartitioned-splits")
    public MySQLClientConfig setUnpartitionedSplits(int unpartitionedSplits)
    {
        this.unpartitionedSplits = unpartitionedSplits;
        return this;
    }

    @Min(1)
    public int getMaxSchemaRefreshThreads()
    {
        return maxSchemaRefreshThreads;
    }

    @Config("mysql.max-schema-refresh-threads")
    public MySQLClientConfig setMaxSchemaRefreshThreads(int maxSchemaRefreshThreads)
    {
        this.maxSchemaRefreshThreads = maxSchemaRefreshThreads;
        return this;
    }

    @NotNull
    public Duration getSchemaCacheTtl()
    {
        return schemaCacheTtl;
    }

    @Config("mysql.schema-cache-ttl")
    public MySQLClientConfig setSchemaCacheTtl(Duration schemaCacheTtl)
    {
        this.schemaCacheTtl = schemaCacheTtl;
        return this;
    }

    @NotNull
    public Duration getSchemaRefreshInterval()
    {
        return schemaRefreshInterval;
    }

    @Config("mysql.schema-refresh-interval")
    public MySQLClientConfig setSchemaRefreshInterval(Duration schemaRefreshInterval)
    {
        this.schemaRefreshInterval = schemaRefreshInterval;
        return this;
    }

    @NotNull
    public ConsistencyLevel getConsistencyLevel()
    {
        return consistencyLevel;
    }

    @Config("mysql.consistency-level")
    public MySQLClientConfig setConsistencyLevel(ConsistencyLevel level)
    {
        this.consistencyLevel = level;
        return this;
    }

    @Min(1)
    public int getFetchSize()
    {
        return fetchSize;
    }

    @Config("mysql.fetch-size")
    public MySQLClientConfig setFetchSize(int fetchSize)
    {
        this.fetchSize = fetchSize;
        return this;
    }

    @Min(1)
    public int getFetchSizeForPartitionKeySelect()
    {
        return fetchSizeForPartitionKeySelect;
    }

    @Config("mysql.fetch-size-for-partition-key-select")
    public MySQLClientConfig setFetchSizeForPartitionKeySelect(int fetchSizeForPartitionKeySelect)
    {
        this.fetchSizeForPartitionKeySelect = fetchSizeForPartitionKeySelect;
        return this;
    }

    @NotNull
	public String getJdbcConnectionString()
    {
		return jdbcConnectionString;
	}

	@Config("jdbc.connection-string")
	public MySQLClientConfig setJdbcConnectionString(String jdbcUrl)
	{
		this.jdbcConnectionString = jdbcUrl;
		return this;
	}

	@NotNull
	public String getJdbcClassName()
	{
		return jdbcClassName;
	}

	@Config("jdbc.class-name")
	public void setJdbcClassName(String jdbcClassName)
	{
		this.jdbcClassName = jdbcClassName;
	}

	@NotNull
	public String getJdbcUserName()
	{
		return jdbcUserName;
	}

	@Config("jdbc.user-name")
	public void setJdbcUserName(String jdbcUserName)
	{
		this.jdbcUserName = jdbcUserName;
	}

	@NotNull
	public String getJdbcPassword() {
		return jdbcPassword;
	}

	@Config("jdbc.password")
	public void setJdbcPassword(String jdbcPassword) {
		this.jdbcPassword = jdbcPassword;
	}

	@NotNull
	public String getConnectorName() {
		return connectorName;
	}

	@Config("database.type")
	public void setConnectorName(String connectorString) {
		this.connectorName = connectorString;
	}
}