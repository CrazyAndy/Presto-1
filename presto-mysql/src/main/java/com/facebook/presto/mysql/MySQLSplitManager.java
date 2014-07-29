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

import com.facebook.presto.mysql.util.HostAddressFactory;
import com.facebook.presto.mysql.util.MySQLHost;
import com.facebook.presto.mysql.util.MySQLUtils;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.Domain;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.Partition;
import com.facebook.presto.spi.PartitionResult;
import com.facebook.presto.spi.Range;
import com.facebook.presto.spi.Split;
import com.facebook.presto.spi.SplitSource;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.inject.Inject;

import io.airlift.log.Logger;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;

public class MySQLSplitManager
        implements ConnectorSplitManager
{
    private static final Logger log = Logger.get(ConnectorSplitManager.class);

    private final String connectorId;
    private final MySQLSession mySQLSession;
    private final CachingMySQLSchemaProvider schemaProvider;
    private final int unpartitionedSplits;
    private final String connector;
    private final String[] clusterNodes;

    @Inject
    public MySQLSplitManager(MYSQLConnectorId connectorId,
            MySQLClientConfig mySQLClientConfig,
            MySQLSession mySQLSession,
            CachingMySQLSchemaProvider schemaProvider)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null").toString();
        this.schemaProvider = checkNotNull(schemaProvider, "schemaProvider is null");
        this.mySQLSession = checkNotNull(mySQLSession, "mySQLSession is null");
        this.unpartitionedSplits = mySQLClientConfig.getUnpartitionedSplits();
        this.connector = mySQLClientConfig.getConnectorName();
        this.clusterNodes = mySQLClientConfig.getClusterNodes();
    }

    @Override
    public String getConnectorId()
    {
        return connectorId;
    }

    @Override
    public boolean canHandle(TableHandle tableHandle)
    {
        return tableHandle instanceof MySQLTableHandle && ((MySQLTableHandle) tableHandle).getConnectorId().equals(connectorId);
    }

    @Override
    public PartitionResult getPartitions(TableHandle tableHandle, TupleDomain tupleDomain)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkNotNull(tupleDomain, "tupleDomain is null");
        
        if (this.connector.equalsIgnoreCase("mysql"))
        {
        	return new PartitionResult(ImmutableList.of((Partition)MySQLPartition.UNPARTITIONED), tupleDomain);
        }
        
        MySQLTableHandle mySQLTableHandle = (MySQLTableHandle) tableHandle;

        MySQLTable table = schemaProvider.getTable(mySQLTableHandle);

        //List<MySQLColumnHandle> partitionKeys = table.getPartitionKeyColumns();
//        List<Comparable<?>> filterPrefix = new ArrayList<>();
//        for (int i = 0; i < partitionKeys.size(); i++) {
//            MySQLColumnHandle columnHandle = partitionKeys.get(i);
//
//            // only add to prefix if all previous keys have a value
//            if (filterPrefix.size() == i && !tupleDomain.isNone()) {
//                Domain domain = tupleDomain.getDomains().get(columnHandle);
//                if (domain != null && domain.getRanges().getRangeCount() == 1) {
//                    // We intentionally ignore whether NULL is in the domain since partition keys can never be NULL
//                    Range range = Iterables.getOnlyElement(domain.getRanges());
//                    if (range.isSingleValue()) {
//                        Comparable<?> value = range.getLow().getValue();
//                        checkArgument(value instanceof Boolean || value instanceof String || value instanceof Double || value instanceof Long,
//                                "Only Boolean, String, Double and Long partition keys are supported");
//                        filterPrefix.add(value);
//                    }
//                }
//            }
//        }

        HashMap<ColumnHandle, Comparable<?>> map = new HashMap<>();
        ImmutableList.Builder<MySQLPartition> partitionBuilder = ImmutableList.builder();
        
        List<ColumnHandle> columns = new ArrayList<ColumnHandle>();
        StringBuilder stringBuilder = new StringBuilder();
        int i = 0;
        for (Map.Entry<ColumnHandle, Domain> entry : tupleDomain.getDomains().entrySet())
        {
        	Domain domain = entry.getValue();
        	
        	if (domain != null && domain.getRanges().getRangeCount() == 1)
        	{
                Range range = Iterables.getOnlyElement(domain.getRanges());
                MySQLColumnHandle columnHandle = (MySQLColumnHandle)entry.getKey();
                if (!range.isSingleValue())
                {
                	continue;
                }
                
                Comparable<?> value = range.getLow().getValue();
                checkArgument(value instanceof Boolean || value instanceof String || value instanceof Double || value instanceof Long,
                        "Only Boolean, String, Double and Long partition keys are supported");
               
                columns.add(columnHandle);
                
                map.put(columnHandle, value);
                if (i > 0) {
                    stringBuilder.append(" AND ");
                }
                stringBuilder.append(MySQLUtils.validColumnName(columnHandle.getName()));
                stringBuilder.append(" = ");
                stringBuilder.append(MYSQLType.getMySQLColumnStringValue(value.toString(), columnHandle.getMySQLType()));
                i++;
            }
        }
        
        if (stringBuilder.length()> 0)
        {
        	TupleDomain tuple = TupleDomain.withFixedValues(map);
            String partitionId = stringBuilder.toString();
            partitionBuilder.add(new MySQLPartition(partitionId, tuple));
        }
        else
        {
        	partitionBuilder.add(MySQLPartition.UNPARTITIONED);
        }
        
        
        List<MySQLPartition> allPartitions = partitionBuilder.build();

        log.debug("%s.%s #partitions: %d", mySQLTableHandle.getSchemaName(), mySQLTableHandle.getTableName(), allPartitions.size());

        // do a final pass to filter based on fields that could not be used to build the prefix
//        List<Partition> partitions = FluentIterable.from(allPartitions)
//                .filter(partitionMatches(tupleDomain))
//                .filter(P artition.class)
//                .toList();

      List<Partition> partitions = FluentIterable.from(allPartitions)
      .filter(Partition.class)
      .toList();
        
        // All partition key domains will be fully evaluated, so we don't need to include those
        TupleDomain remainingTupleDomain = TupleDomain.none();
        if (!tupleDomain.isNone()) {
            remainingTupleDomain = TupleDomain.withColumnDomains(Maps.filterKeys(tupleDomain.getDomains(), not(in(columns))));
        }

        return new PartitionResult(partitions, remainingTupleDomain);
    }

    @Override
    public SplitSource getPartitionSplits(TableHandle tableHandle, List<Partition> partitions)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkArgument(tableHandle instanceof MySQLTableHandle, "tableHandle is not an instance of CassandraTableHandle");
        
        MySQLTableHandle mySQLTableHandle = (MySQLTableHandle) tableHandle;

        checkNotNull(partitions, "partitions is null");
        if (partitions.isEmpty()) {
            return new FixedSplitSource(connectorId, ImmutableList.<Split>of());
        }

        // if this is an unpartitioned table, split into equal ranges
        if (partitions.size() == 1) {
            Partition partition = partitions.get(0);
            checkArgument(partition instanceof MySQLPartition, "partitions are no mySQLPartitions");
            MySQLPartition mySQLPartition = (MySQLPartition) partition;

            if (mySQLPartition.isUnpartitioned()) {
                MySQLTable table = schemaProvider.getTable(mySQLTableHandle);
                List<Split> splits = getSplitsByTokenRange(table, mySQLPartition.getPartitionId());
                return new FixedSplitSource(connectorId, splits);
            }
        }

        return new FixedSplitSource(connectorId, getSplitsForPartitions(mySQLTableHandle, partitions));
    }

    private List<Split> getSplitsByTokenRange(MySQLTable table, String partitionId)
    {
        String schema = table.getTableHandle().getSchemaName();
        String tableName = table.getTableHandle().getTableName();
        String tokenExpression = table.getTokenExpression();

        List<HostAddress> addresses = new ArrayList<HostAddress>();
        
        ImmutableList.Builder<Split> builder = ImmutableList.builder();
        if (!partitionId.equalsIgnoreCase(MySQLPartition.UNPARTITIONED_ID)) {
            BigInteger start = BigInteger.valueOf(Long.MIN_VALUE);
            BigInteger end = BigInteger.valueOf(Long.MAX_VALUE);
            BigInteger one = BigInteger.valueOf(1);
            BigInteger splits = BigInteger.valueOf(unpartitionedSplits);
            long delta = end.subtract(start).subtract(one).divide(splits).longValue();
            long startToken = start.longValue();

            for (int i = 0; i < unpartitionedSplits - 1; i++) {
                long endToken = startToken + delta;
                String condition = buildTokenCondition(tokenExpression, startToken, endToken);
                MySQLSplit split = new MySQLSplit(connectorId, schema, tableName, partitionId, condition, addresses);
                builder.add(split);
                startToken = endToken + 1;
            }
            // special handling for last split
            String condition = buildTokenCondition(tokenExpression, startToken, end.longValue());
            MySQLSplit split = new MySQLSplit(connectorId, schema, tableName, partitionId, condition, addresses);
            builder.add(split);
        }
        else if (this.clusterNodes == null || this.clusterNodes.length == 0)
        {
          builder.add(new MySQLSplit(connectorId, schema, tableName, partitionId, null, addresses));
        }
        else
        {
        	for(String node : this.clusterNodes)
        	{
        		List<HostAddress> addr = new ArrayList<HostAddress>();
        		
        		addr.add(HostAddress.fromString(node));
        		System.out.println("Adding split to" + addr);
        		builder.add(new MySQLSplit(connectorId, schema, tableName, partitionId, null, addr));
        	}
        }

        return builder.build();
    }

    private static String buildTokenCondition(String tokenExpression, long startToken, long endToken)
    {
        return tokenExpression + " >= " + startToken + " AND " + tokenExpression + " <= " + endToken;
    }

    private List<Split> getSplitsForPartitions(MySQLTableHandle mySQLTableHandle, List<Partition> partitions)
    {
        String schema = mySQLTableHandle.getSchemaName();
        String table = mySQLTableHandle.getTableName();
        HostAddressFactory hostAddressFactory = new HostAddressFactory();
        ImmutableList.Builder<Split> builder = ImmutableList.builder();
        for (Partition partition : partitions) {
            checkArgument(partition instanceof MySQLPartition, "partitions are no MySQLPartitions");
            MySQLPartition mySQLPartition = (MySQLPartition) partition;

            MySQLSplit split = new MySQLSplit(connectorId, schema, table, mySQLPartition.getPartitionId(), null, new ArrayList<HostAddress>());
            builder.add(split);
        }
        return builder.build();
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("clientId", connectorId)
                .toString();
    }

    public static Predicate<MySQLPartition> partitionMatches(final TupleDomain tupleDomain)
    {
        return new Predicate<MySQLPartition>()
        {
            @Override
            public boolean apply(MySQLPartition partition)
            {
                return tupleDomain.overlaps(partition.getTupleDomain());
            }
        };
    }
}
