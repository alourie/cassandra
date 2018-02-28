/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.dht;

import java.util.*;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import org.apache.cassandra.locator.*;
import org.apache.cassandra.locator.VirtualEndpoint;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.IFailureDetector;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.streaming.StreamPlan;
import org.apache.cassandra.streaming.StreamResultFuture;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Assists in streaming ranges to a node.
 */
public class RangeStreamer
{
    private static final Logger logger = LoggerFactory.getLogger(RangeStreamer.class);

    /* bootstrap tokens. can be null if replacing the node. */
    private final Collection<Token> tokens;
    /* current token ring */
    private final TokenMetadata metadata;
    /* address of this node */
    private final VirtualEndpoint address;
    /* streaming description */
    private final String description;
    private final Multimap<String, Map.Entry<VirtualEndpoint, Collection<Range<Token>>>> toFetch = HashMultimap.create();
    private final Set<ISourceFilter> sourceFilters = new HashSet<>();
    private final StreamPlan streamPlan;
    private final boolean useStrictConsistency;
    private final IEndpointSnitch snitch;
    private final StreamStateStore stateStore;

    /**
     * A filter applied to sources to stream from when constructing a fetch map.
     */
    public static interface ISourceFilter
    {
        public boolean shouldInclude(VirtualEndpoint endpoint);
    }

    /**
     * Source filter which excludes any endpoints that are not alive according to a
     * failure detector.
     */
    public static class FailureDetectorSourceFilter implements ISourceFilter
    {
        private final IFailureDetector fd;

        public FailureDetectorSourceFilter(IFailureDetector fd)
        {
            this.fd = fd;
        }

        public boolean shouldInclude(VirtualEndpoint endpoint)
        {
            return fd.isAlive(endpoint);
        }
    }

    /**
     * Source filter which excludes any endpoints that are not in a specific data center.
     */
    public static class SingleDatacenterFilter implements ISourceFilter
    {
        private final String sourceDc;
        private final IEndpointSnitch snitch;

        public SingleDatacenterFilter(IEndpointSnitch snitch, String sourceDc)
        {
            this.sourceDc = sourceDc;
            this.snitch = snitch;
        }

        public boolean shouldInclude(VirtualEndpoint endpoint)
        {
            return snitch.getDatacenter(endpoint).equals(sourceDc);
        }
    }

    /**
     * Source filter which excludes the current node from source calculations
     */
    public static class ExcludeLocalNodeFilter implements ISourceFilter
    {
        public boolean shouldInclude(VirtualEndpoint endpoint)
        {
            return !FBUtilities.getBroadcastAddressAndPort().equals(endpoint);
        }
    }

    /**
     * Source filter which only includes endpoints contained within a provided set.
     */
    public static class WhitelistedSourcesFilter implements ISourceFilter
    {
        private final Set<VirtualEndpoint> whitelistedSources;

        public WhitelistedSourcesFilter(Set<VirtualEndpoint> whitelistedSources)
        {
            this.whitelistedSources = whitelistedSources;
        }

        public boolean shouldInclude(VirtualEndpoint endpoint)
        {
            return whitelistedSources.contains(endpoint);
        }
    }

    public RangeStreamer(TokenMetadata metadata,
                         Collection<Token> tokens,
                         VirtualEndpoint address,
                         StreamOperation streamOperation,
                         boolean useStrictConsistency,
                         IEndpointSnitch snitch,
                         StreamStateStore stateStore,
                         boolean connectSequentially,
                         int connectionsPerHost)
    {
        this.metadata = metadata;
        this.tokens = tokens;
        this.address = address;
        this.description = streamOperation.getDescription();
        this.streamPlan = new StreamPlan(streamOperation, connectionsPerHost, true, connectSequentially, null, PreviewKind.NONE);
        this.useStrictConsistency = useStrictConsistency;
        this.snitch = snitch;
        this.stateStore = stateStore;
        streamPlan.listeners(this.stateStore);
    }

    public void addSourceFilter(ISourceFilter filter)
    {
        sourceFilters.add(filter);
    }

    /**
     * Add ranges to be streamed for given keyspace.
     *
     * @param keyspaceName keyspace name
     * @param ranges ranges to be streamed
     */
    public void addRanges(String keyspaceName, Collection<Range<Token>> ranges)
    {
        if(Keyspace.open(keyspaceName).getReplicationStrategy() instanceof LocalStrategy)
        {
            logger.info("Not adding ranges for Local Strategy keyspace={}", keyspaceName);
            return;
        }

        boolean useStrictSource = useStrictSourcesForRanges(keyspaceName);
        Multimap<Range<Token>, VirtualEndpoint> rangesForKeyspace = useStrictSource
                ? getAllRangesWithStrictSourcesFor(keyspaceName, ranges) : getAllRangesWithSourcesFor(keyspaceName, ranges);

        for (Map.Entry<Range<Token>, VirtualEndpoint> entry : rangesForKeyspace.entries())
            logger.info("{}: range {} exists on {} for keyspace {}", description, entry.getKey(), entry.getValue(), keyspaceName);

        AbstractReplicationStrategy strat = Keyspace.open(keyspaceName).getReplicationStrategy();
        Multimap<VirtualEndpoint, Range<Token>> rangeFetchMap = useStrictSource || strat == null || strat.getReplicationFactor() == 1
                                                            ? getRangeFetchMap(rangesForKeyspace, sourceFilters, keyspaceName, useStrictConsistency)
                                                            : getOptimizedRangeFetchMap(rangesForKeyspace, sourceFilters, keyspaceName);

        for (Map.Entry<VirtualEndpoint, Collection<Range<Token>>> entry : rangeFetchMap.asMap().entrySet())
        {
            if (logger.isTraceEnabled())
            {
                for (Range<Token> r : entry.getValue())
                    logger.trace("{}: range {} from source {} for keyspace {}", description, r, entry.getKey(), keyspaceName);
            }
            toFetch.put(keyspaceName, entry);
        }
    }

    /**
     * @param keyspaceName keyspace name to check
     * @return true when the node is bootstrapping, useStrictConsistency is true and # of nodes in the cluster is more than # of replica
     */
    private boolean useStrictSourcesForRanges(String keyspaceName)
    {
        AbstractReplicationStrategy strat = Keyspace.open(keyspaceName).getReplicationStrategy();
        return useStrictConsistency
                && tokens != null
                && metadata.getSizeOfAllEndpoints() != strat.getReplicationFactor();
    }

    /**
     * Get a map of all ranges and their respective sources that are candidates for streaming the given ranges
     * to us. For each range, the list of sources is sorted by proximity relative to the given destAddress.
     *
     * @throws java.lang.IllegalStateException when there is no source to get data streamed
     */
    private Multimap<Range<Token>, VirtualEndpoint> getAllRangesWithSourcesFor(String keyspaceName, Collection<Range<Token>> desiredRanges)
    {
        AbstractReplicationStrategy strat = Keyspace.open(keyspaceName).getReplicationStrategy();
        Multimap<Range<Token>, VirtualEndpoint> rangeAddresses = strat.getRangeAddresses(metadata.cloneOnlyTokenMap());

        Multimap<Range<Token>, VirtualEndpoint> rangeSources = ArrayListMultimap.create();
        for (Range<Token> desiredRange : desiredRanges)
        {
            for (Range<Token> range : rangeAddresses.keySet())
            {
                if (range.contains(desiredRange))
                {
                    List<VirtualEndpoint> preferred = snitch.getSortedListByProximity(address, rangeAddresses.get(range));
                    rangeSources.putAll(desiredRange, preferred);
                    break;
                }
            }

            if (!rangeSources.keySet().contains(desiredRange))
                throw new IllegalStateException("No sources found for " + desiredRange);
        }

        return rangeSources;
    }

    /**
     * Get a map of all ranges and the source that will be cleaned up once this bootstrapped node is added for the given ranges.
     * For each range, the list should only contain a single source. This allows us to consistently migrate data without violating
     * consistency.
     *
     * @throws java.lang.IllegalStateException when there is no source to get data streamed, or more than 1 source found.
     */
    private Multimap<Range<Token>, VirtualEndpoint> getAllRangesWithStrictSourcesFor(String keyspace, Collection<Range<Token>> desiredRanges)
    {
        assert tokens != null;
        AbstractReplicationStrategy strat = Keyspace.open(keyspace).getReplicationStrategy();

        // Active ranges
        TokenMetadata metadataClone = metadata.cloneOnlyTokenMap();
        Multimap<Range<Token>, VirtualEndpoint> addressRanges = strat.getRangeAddresses(metadataClone);

        // Pending ranges
        metadataClone.updateNormalTokens(tokens, address);
        Multimap<Range<Token>, VirtualEndpoint> pendingRangeAddresses = strat.getRangeAddresses(metadataClone);

        // Collects the source that will have its range moved to the new node
        Multimap<Range<Token>, VirtualEndpoint> rangeSources = ArrayListMultimap.create();

        for (Range<Token> desiredRange : desiredRanges)
        {
            for (Map.Entry<Range<Token>, Collection<VirtualEndpoint>> preEntry : addressRanges.asMap().entrySet())
            {
                if (preEntry.getKey().contains(desiredRange))
                {
                    Set<VirtualEndpoint> oldEndpoints = Sets.newHashSet(preEntry.getValue());
                    Set<VirtualEndpoint> newEndpoints = Sets.newHashSet(pendingRangeAddresses.get(desiredRange));

                    // Due to CASSANDRA-5953 we can have a higher RF then we have endpoints.
                    // So we need to be careful to only be strict when endpoints == RF
                    if (oldEndpoints.size() == strat.getReplicationFactor())
                    {
                        oldEndpoints.removeAll(newEndpoints);
                        assert oldEndpoints.size() == 1 : "Expected 1 endpoint but found " + oldEndpoints.size();
                    }

                    rangeSources.put(desiredRange, oldEndpoints.iterator().next());
                }
            }

            // Validate
            Collection<VirtualEndpoint> addressList = rangeSources.get(desiredRange);
            if (addressList == null || addressList.isEmpty())
                throw new IllegalStateException("No sources found for " + desiredRange);

            if (addressList.size() > 1)
                throw new IllegalStateException("Multiple endpoints found for " + desiredRange);

            VirtualEndpoint sourceIp = addressList.iterator().next();
            EndpointState sourceState = Gossiper.instance.getEndpointStateForEndpoint(sourceIp);
            if (Gossiper.instance.isEnabled() && (sourceState == null || !sourceState.isAlive()))
                throw new RuntimeException("A node required to move the data consistently is down (" + sourceIp + "). " +
                                           "If you wish to move the data from a potentially inconsistent replica, restart the node with -Dcassandra.consistent.rangemovement=false");
        }

        return rangeSources;
    }

    /**
     * @param rangesWithSources The ranges we want to fetch (key) and their potential sources (value)
     * @param sourceFilters A (possibly empty) collection of source filters to apply. In addition to any filters given
     *                      here, we always exclude ourselves.
     * @param keyspace keyspace name
     * @return Map of source endpoint to collection of ranges
     */
    private static Multimap<VirtualEndpoint, Range<Token>> getRangeFetchMap(Multimap<Range<Token>, VirtualEndpoint> rangesWithSources,
                                                                            Collection<ISourceFilter> sourceFilters, String keyspace,
                                                                            boolean useStrictConsistency)
    {
        Multimap<VirtualEndpoint, Range<Token>> rangeFetchMapMap = HashMultimap.create();
        for (Range<Token> range : rangesWithSources.keySet())
        {
            boolean foundSource = false;

            outer:
            for (VirtualEndpoint address : rangesWithSources.get(range))
            {
                for (ISourceFilter filter : sourceFilters)
                {
                    if (!filter.shouldInclude(address))
                        continue outer;
                }

                if (address.equals(FBUtilities.getBroadcastAddressAndPort()))
                {
                    // If localhost is a source, we have found one, but we don't add it to the map to avoid streaming locally
                    foundSource = true;
                    continue;
                }

                rangeFetchMapMap.put(address, range);
                foundSource = true;
                break; // ensure we only stream from one other node for each range
            }

            if (!foundSource)
            {
                AbstractReplicationStrategy strat = Keyspace.open(keyspace).getReplicationStrategy();
                if (strat != null && strat.getReplicationFactor() == 1)
                {
                    if (useStrictConsistency)
                        throw new IllegalStateException("Unable to find sufficient sources for streaming range " + range + " in keyspace " + keyspace + " with RF=1. " +
                                                        "Ensure this keyspace contains replicas in the source datacenter.");
                    else
                        logger.warn("Unable to find sufficient sources for streaming range {} in keyspace {} with RF=1. " +
                                    "Keyspace might be missing data.", range, keyspace);
                }
                else
                    throw new IllegalStateException("Unable to find sufficient sources for streaming range " + range + " in keyspace " + keyspace);
            }
        }

        return rangeFetchMapMap;
    }


    private static Multimap<VirtualEndpoint, Range<Token>> getOptimizedRangeFetchMap(Multimap<Range<Token>, VirtualEndpoint> rangesWithSources,
                                                                                     Collection<ISourceFilter> sourceFilters, String keyspace)
    {
        RangeFetchMapCalculator calculator = new RangeFetchMapCalculator(rangesWithSources, sourceFilters, keyspace);
        Multimap<VirtualEndpoint, Range<Token>> rangeFetchMapMap = calculator.getRangeFetchMap();
        logger.info("Output from RangeFetchMapCalculator for keyspace {}", keyspace);
        validateRangeFetchMap(rangesWithSources, rangeFetchMapMap, keyspace);
        return rangeFetchMapMap;
    }

    /**
     * Verify that source returned for each range is correct
     * @param rangesWithSources
     * @param rangeFetchMapMap
     * @param keyspace
     */
    private static void validateRangeFetchMap(Multimap<Range<Token>, VirtualEndpoint> rangesWithSources, Multimap<VirtualEndpoint, Range<Token>> rangeFetchMapMap, String keyspace)
    {
        for (Map.Entry<VirtualEndpoint, Range<Token>> entry : rangeFetchMapMap.entries())
        {
            if(entry.getKey().equals(FBUtilities.getBroadcastAddressAndPort()))
            {
                throw new IllegalStateException("Trying to stream locally. Range: " + entry.getValue()
                                        + " in keyspace " + keyspace);
            }

            if (!rangesWithSources.get(entry.getValue()).contains(entry.getKey()))
            {
                throw new IllegalStateException("Trying to stream from wrong endpoint. Range: " + entry.getValue()
                                                + " in keyspace " + keyspace + " from endpoint: " + entry.getKey());
            }

            logger.info("Streaming range {} from endpoint {} for keyspace {}", entry.getValue(), entry.getKey(), keyspace);
        }
    }

    public static Multimap<VirtualEndpoint, Range<Token>> getWorkMap(Multimap<Range<Token>, VirtualEndpoint> rangesWithSourceTarget, String keyspace,
                                                                     IFailureDetector fd, boolean useStrictConsistency)
    {
        return getRangeFetchMap(rangesWithSourceTarget, Collections.<ISourceFilter>singleton(new FailureDetectorSourceFilter(fd)), keyspace, useStrictConsistency);
    }

    // For testing purposes
    @VisibleForTesting
    Multimap<String, Map.Entry<VirtualEndpoint, Collection<Range<Token>>>> toFetch()
    {
        return toFetch;
    }

    public StreamResultFuture fetchAsync()
    {
        for (Map.Entry<String, Map.Entry<VirtualEndpoint, Collection<Range<Token>>>> entry : toFetch.entries())
        {
            String keyspace = entry.getKey();
            VirtualEndpoint source = entry.getValue().getKey();
            VirtualEndpoint preferred = SystemKeyspace.getPreferredIP(source);
            Collection<Range<Token>> ranges = entry.getValue().getValue();

            // filter out already streamed ranges
            Set<Range<Token>> availableRanges = stateStore.getAvailableRanges(keyspace, StorageService.instance.getTokenMetadata().partitioner);
            if (ranges.removeAll(availableRanges))
            {
                logger.info("Some ranges of {} are already available. Skipping streaming those ranges.", availableRanges);
            }

            if (logger.isTraceEnabled())
                logger.trace("{}ing from {} ranges {}", description, source, StringUtils.join(ranges, ", "));
            /* Send messages to respective folks to stream data over to me */
            streamPlan.requestRanges(source, preferred, keyspace, ranges);
        }

        return streamPlan.execute();
    }
}
