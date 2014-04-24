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
import com.facebook.presto.spi.ColumnType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Objects.ToStringHelper;

import javax.annotation.Nullable;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class MySQLColumnHandle
        implements ColumnHandle
{
    private final String connectorId;
    private final String name;
    private final int ordinalPosition;
    private final MYSQLType mysqlType;
    private final List<MYSQLType> typeArguments;
    private final boolean partitionKey;
    private final boolean clusteringKey;

    @JsonCreator
    public MySQLColumnHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("name") String name,
            @JsonProperty("ordinalPosition") int ordinalPosition,
            @JsonProperty("mysqlType") MYSQLType mysqlType,
            @Nullable @JsonProperty("typeArguments") List<MYSQLType> typeArguments,
            @JsonProperty("partitionKey") boolean partitionKey,
            @JsonProperty("clusteringKey") boolean clusteringKey)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null");
        this.name = checkNotNull(name, "name is null");
        checkArgument(ordinalPosition >= 0, "ordinalPosition is negative");
        this.ordinalPosition = ordinalPosition;
        this.mysqlType = checkNotNull(mysqlType, "mysqlType is null");
        int typeArgsSize = mysqlType.getTypeArgumentSize();
        if (typeArgsSize > 0) {
            this.typeArguments = checkNotNull(typeArguments, "typeArguments is null");
            checkArgument(typeArguments.size() == typeArgsSize, mysqlType
                    + " must provide " + typeArgsSize + " type arguments");
        }
        else {
            this.typeArguments = null;
        }
        this.partitionKey = partitionKey;
        this.clusteringKey = clusteringKey;
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public int getOrdinalPosition()
    {
        return ordinalPosition;
    }

    @JsonProperty
    public MYSQLType getMySQLType()
    {
        return mysqlType;
    }

    @JsonProperty
    public List<MYSQLType> getTypeArguments()
    {
        return typeArguments;
    }

    @JsonProperty
    public boolean isPartitionKey()
    {
        return partitionKey;
    }

    @JsonProperty
    public boolean isClusteringKey()
    {
        return clusteringKey;
    }

    public ColumnMetadata getColumnMetadata()
    {
        return new ColumnMetadata(MySQLUtils.cqlNameToSqlName(name), mysqlType.getNativeType(), ordinalPosition, partitionKey);
    }

    public ColumnType getType()
    {
        return mysqlType.getNativeType();
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(
                connectorId,
                name,
                ordinalPosition,
                mysqlType,
                typeArguments,
                partitionKey,
                clusteringKey);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        MySQLColumnHandle other = (MySQLColumnHandle) obj;
        return Objects.equal(this.connectorId, other.connectorId)
                && Objects.equal(this.name, other.name)
                && Objects.equal(this.ordinalPosition, other.ordinalPosition)
                && Objects.equal(this.mysqlType, other.mysqlType)
                && Objects.equal(this.typeArguments, other.typeArguments)
                && Objects.equal(this.partitionKey, other.partitionKey)
                && Objects.equal(this.clusteringKey, other.clusteringKey);
    }

    @Override
    public String toString()
    {
        ToStringHelper helper = Objects.toStringHelper(this)
                .add("connectorId", connectorId)
                .add("name", name)
                .add("ordinalPosition", ordinalPosition)
                .add("mysqlType", mysqlType);

        if (typeArguments != null && !typeArguments.isEmpty()) {
            helper.add("typeArguments", typeArguments);
        }

        helper.add("partitionKey", partitionKey)
                .add("clusteringKey", clusteringKey);

        return helper.toString();
    }

    public static Function<ColumnHandle, MySQLColumnHandle> mySQLColumnHandle()
    {
        return new Function<ColumnHandle, MySQLColumnHandle>()
        {
            @Override
            public MySQLColumnHandle apply(ColumnHandle columnHandle)
            {
                checkNotNull(columnHandle, "columnHandle is null");
                checkArgument(columnHandle instanceof MySQLColumnHandle,
                        "columnHandle is not an instance of CassandraColumnHandle");
                return (MySQLColumnHandle) columnHandle;
            }
        };
    }

    public static Function<MySQLColumnHandle, ColumnMetadata> columnMetadataGetter()
    {
        return new Function<MySQLColumnHandle, ColumnMetadata>()
        {
            @Override
            public ColumnMetadata apply(MySQLColumnHandle input)
            {
                return input.getColumnMetadata();
            }
        };
    }

    public static Function<MySQLColumnHandle, ColumnType> nativeTypeGetter()
    {
        return new Function<MySQLColumnHandle, ColumnType>()
        {
            @Override
            public ColumnType apply(MySQLColumnHandle input)
            {
                return input.getType();
            }
        };
    }

    public static Function<MySQLColumnHandle, FullMySQLType> mySQLFullTypeGetter()
    {
        return new Function<MySQLColumnHandle, FullMySQLType>()
        {
            @Override
            public FullMySQLType apply(MySQLColumnHandle input)
            {
                if (input.getMySQLType().getTypeArgumentSize() == 0) {
                    return input.getMySQLType();
                }
                else {
                    return new MySQLTypeWithTypeArguments(input.getMySQLType(), input.getTypeArguments());
                }
            }
        };
    }
}
