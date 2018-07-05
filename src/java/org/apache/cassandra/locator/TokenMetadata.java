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
package org.apache.cassandra.locator;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.*;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.BiMultiValMap;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.SortedBiMultiValMap;

public class TokenMetadata
{
    private static final Logger logger = LoggerFactory.getLogger(TokenMetadata.class);

    /**
     * Maintains token to endpoint map of every node in the cluster.
     * Each Token is associated with exactly one Address, but each Address may have
     * multiple tokens.  Hence, the BiMultiValMap collection.
     */
    private final BiMultiValMap<Token, Endpoint> tokenToEndpointMap;

    /** Maintains the list of all endpoints (representing every node in the cluster) */
    private final Set<Endpoint> allEndpoints;

    // Prior to CASSANDRA-603, we just had <tt>Map<Range, Endpoint> pendingRanges<tt>,
    // which was added to when a node began bootstrap and removed from when it finished.
    //
    // This is inadequate when multiple changes are allowed simultaneously.  For example,
    // suppose that there is a ring of nodes A, C and E, with replication factor 3.
    // Node D bootstraps between C and E, so its pending ranges will be E-A, A-C and C-D.
    // Now suppose node B bootstraps between A and C at the same time. Its pending ranges
    // would be C-E, E-A and A-B. Now both nodes need to be assigned pending range E-A,
    // which we would be unable to represent with the old Map.  The same thing happens
    // even more obviously for any nodes that boot simultaneously between same two nodes.
    //
    // So, we made two changes:
    //
    // First, we changed pendingRanges to a <tt>Multimap<Range, Endpoint></tt> (now
    // <tt>Map<String, Multimap<Range, Endpoint>></tt>, because replication strategy
    // and options are per-KeySpace).
    //
    // Second, we added the bootstrapTokens and leavingEndpoints collections, so we can
    // rebuild pendingRanges from the complete information of what is going on, when
    // additional changes are made mid-operation.
    //
    // Finally, note that recording the tokens of joining nodes in bootstrapTokens also
    // means we can detect and reject the addition of multiple nodes at the same token
    // before one becomes part of the ring.
    private final BiMultiValMap<Token, Endpoint> bootstrapTokens = new BiMultiValMap<>();

    private final BiMap<Endpoint, Endpoint> replacementToOriginal = HashBiMap.create();

    // (don't need to record Token here since it's still part of tokenToEndpointMap until it's done leaving)
    private final Set<Endpoint> leavingEndpoints = new HashSet<>();
    // this is a cache of the calculation from {tokenToEndpointMap, bootstrapTokens, leavingEndpoints}
    private final ConcurrentMap<String, PendingRangeMaps> pendingRanges = new ConcurrentHashMap<String, PendingRangeMaps>();

    // nodes which are migrating to the new tokens in the ring
    private final Set<Pair<Token, Endpoint>> movingEndpoints = new HashSet<>();

    /* Use this lock for manipulating the token map */
    private final ReadWriteLock lock = new ReentrantReadWriteLock(true);
    private volatile ArrayList<Token> sortedTokens;

    private final Topology topology;
    public final IPartitioner partitioner;

    // signals replication strategies that nodes have joined or left the ring and they need to recompute ownership
    private volatile long ringVersion = 0;

    public TokenMetadata()
    {
        this(SortedBiMultiValMap.<Token, Endpoint>create(),
             new HashSet<>(),
             new Topology(),
             DatabaseDescriptor.getPartitioner());
    }

    private TokenMetadata(BiMultiValMap<Token, Endpoint> tokenToEndpointMap, Set<Endpoint> endpoints, Topology topology, IPartitioner partitioner)
    {
        this.tokenToEndpointMap = tokenToEndpointMap;
        this.topology = topology;
        this.partitioner = partitioner;
        allEndpoints = endpoints;
        sortedTokens = sortTokens();
    }

    /**
     * To be used by tests only (via {@link org.apache.cassandra.service.StorageService#setPartitionerUnsafe}).
     */
    @VisibleForTesting
    public TokenMetadata cloneWithNewPartitioner(IPartitioner newPartitioner)
    {
        return new TokenMetadata(tokenToEndpointMap, allEndpoints, topology, newPartitioner);
    }

    private ArrayList<Token> sortTokens()
    {
        return new ArrayList<>(tokenToEndpointMap.keySet());
    }

    /** @return the number of nodes bootstrapping into source's primary range */
    public int pendingRangeChanges(Endpoint source)
    {
        int n = 0;
        Collection<Range<Token>> sourceRanges = getPrimaryRangesFor(getTokens(source));
        lock.readLock().lock();
        try
        {
            for (Token token : bootstrapTokens.keySet())
                for (Range<Token> range : sourceRanges)
                    if (range.contains(token))
                        n++;
        }
        finally
        {
            lock.readLock().unlock();
        }
        return n;
    }

    /**
     * Update token map with a single token/endpoint pair in normal state.
     */
    public void updateNormalToken(Token token, Endpoint endpoint)
    {
        updateNormalTokens(Collections.singleton(token), endpoint);
    }

    public void updateNormalTokens(Collection<Token> tokens, Endpoint endpoint)
    {
        Multimap<Endpoint, Token> endpointTokens = HashMultimap.create();
        for (Token token : tokens)
            endpointTokens.put(endpoint, token);
        updateNormalTokens(endpointTokens);
    }

    /**
     * Update token map with a set of token/endpoint pairs in normal state.
     *
     * Prefer this whenever there are multiple pairs to update, as each update (whether a single or multiple)
     * is expensive (CASSANDRA-3831).
     */
    public void updateNormalTokens(Multimap<Endpoint, Token> endpointTokens)
    {
        if (endpointTokens.isEmpty())
            return;

        lock.writeLock().lock();
        try
        {
            boolean shouldSortTokens = false;
            for (Endpoint endpoint : endpointTokens.keySet())
            {
                Collection<Token> tokens = endpointTokens.get(endpoint);

                assert tokens != null && !tokens.isEmpty();

                bootstrapTokens.removeValue(endpoint);
                tokenToEndpointMap.removeValue(endpoint);
                topology.addEndpoint(endpoint);
                leavingEndpoints.remove(endpoint);
                replacementToOriginal.remove(endpoint);
                removeFromMoving(endpoint); // also removing this endpoint from moving

                for (Token token : tokens)
                {
                    Endpoint prev = tokenToEndpointMap.put(token, endpoint);
                    if (!endpoint.equals(prev))
                    {
                        if (prev != null)
                            logger.warn("Token {} changing ownership from {} to {}", token, prev, endpoint);
                        shouldSortTokens = true;
                    }
                }
            }

            if (shouldSortTokens)
                sortedTokens = sortTokens();
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    /**
     * Update an end-point to host ID mapping.  Each ID must be unique, and
     * cannot be changed after the fact.
     */

    // TODO: make it something like updateEndpointEntry
    public void updateHostId(UUID hostId, InetAddressAndPort endpointAddress)
    {
        Endpoint endpoint = getEndpointForAddress(endpointAddress);
        if (endpoint == null)
            // initial addition of this specific address/uuid pair, so creating a new Endpoint object
            endpoint = new Endpoint(endpointAddress, null, null, hostId);
        updateHostId(hostId, endpoint);
    }

    // TODO: make it something like updateEndpointEntry
    public void updateHostId(UUID hostId, Endpoint endpointToUpdate)
    {
        assert hostId != null;
        assert endpointToUpdate != null;

        // Create a new object with an updated hostId
        final Endpoint newEndpoint = endpointToUpdate.clone();
        newEndpoint.hostId = hostId;


        lock.writeLock().lock();
        try
        {
            if (!allEndpoints.contains(endpointToUpdate))
            {
                logger.info("The provided endpoint " + endpointToUpdate.toString() + " doesn't exists in TokenMetadata, adding new");
                allEndpoints.add(newEndpoint);
                return;
            }

            // Check if there's another node with the same ID
            Set<Endpoint> storedEndpointsWithId = allEndpoints.stream().filter(e -> e.hostId.equals(hostId)).collect(Collectors.toSet());
            for (Endpoint storedEndpoint : storedEndpointsWithId)
                if (FailureDetector.instance.isAlive(storedEndpoint) && !storedEndpoint.equals(newEndpoint))
                    throw new RuntimeException(String.format("Host ID collision between active endpoint %s and a new %s",
                                                             storedEndpoint,
                                                             newEndpoint));


            // If no problems found, update the hostID
            if (!allEndpoints.contains(newEndpoint))
            {
                    allEndpoints.remove(endpointToUpdate);
                    allEndpoints.add(newEndpoint);
            }
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    /** Return the end-point for a unique host ID */
    public Endpoint getEndpointForHostId(UUID otherHostId)
    {
        lock.readLock().lock();
        try
        {
            final Optional<Endpoint> endpoint = allEndpoints.stream().filter(e -> e.hostId.equals(otherHostId)).findFirst();
            return endpoint.orElse(null);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    public Endpoint getEndpointForAddress(final InetAddressAndPort address)
    {
        lock.readLock().lock();
        try
        {
            // will throw an exception if there are 2 endpoints having a same address/port pair
            isAddressTaken(address);

            // Find a first Endpoint that has the provided address in any of it's address fields.
            // The allEndpoints set doesn't include more than 1 such endpoint as per the check above
            return allEndpoints.stream().filter(e -> e.hasAddress(address)).findFirst().orElse(null);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    private boolean isAddressTaken(final InetAddressAndPort address)
    {
        final Collection<Endpoint> endpoints = allEndpoints.stream()
                                                           .filter(e -> e.hasAddress(address))
                                                           .filter(e -> FailureDetector.instance.isAlive(e))
                                                           .collect(Collectors.toList());
        if (endpoints.size() > 1)
            throw new IllegalStateException("Bad state: Found at least 2 endpoints with same address: " + address.toString(true));

        return endpoints.size() == 1;
    }

    private boolean isHostIdTaken(final UUID hostId)
    {
        final Collection<Endpoint> endpoints = allEndpoints.stream()
                                                           .filter(e -> e.hostId.equals(hostId))
                                                           .filter(e -> FailureDetector.instance.isAlive(e))
                                                           .collect(Collectors.toList());
        if (endpoints.size() > 1)
            throw new IllegalStateException("Bad state: Found at least 2 endpoints with same UUID: " + hostId;

        return endpoints.size() == 1;
    }

    private void checkEndpointUnique(final Endpoint endpoint)
    {}

    /** @return a copy of the endpoint-to-id map for read-only operations */
    public Map<Endpoint, UUID> getEndpointToHostIdMapForReading()
    {
        lock.readLock().lock();
        try
        {
            return allEndpoints.stream().collect(Collectors.toMap(e -> e, e -> e.hostId));
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    @Deprecated
    public void addBootstrapToken(Token token, Endpoint endpoint)
    {
        addBootstrapTokens(Collections.singleton(token), endpoint);
    }

    public void addBootstrapTokens(Collection<Token> tokens, Endpoint endpoint)
    {
        addBootstrapTokens(tokens, endpoint, null);
    }

    private void addBootstrapTokens(Collection<Token> tokens, Endpoint endpoint, Endpoint original)
    {
        assert tokens != null && !tokens.isEmpty();
        assert endpoint != null;

        lock.writeLock().lock();
        try
        {

            Endpoint oldEndpoint;

            for (Token token : tokens)
            {
                oldEndpoint = bootstrapTokens.get(token);
                if (oldEndpoint != null && !oldEndpoint.equals(endpoint))
                    throw new RuntimeException("Bootstrap Token collision between " + oldEndpoint + " and " + endpoint + " (token " + token);

                oldEndpoint = tokenToEndpointMap.get(token);
                if (oldEndpoint != null && !oldEndpoint.equals(endpoint) && !oldEndpoint.equals(original))
                    throw new RuntimeException("Bootstrap Token collision between " + oldEndpoint + " and " + endpoint + " (token " + token);
            }

            bootstrapTokens.removeValue(endpoint);

            for (Token token : tokens)
                bootstrapTokens.put(token, endpoint);
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    public void addReplaceTokens(Collection<Token> replacingTokens, Endpoint newNode, Endpoint oldNode)
    {
        assert replacingTokens != null && !replacingTokens.isEmpty();
        assert newNode != null && oldNode != null;

        lock.writeLock().lock();
        try
        {
            Collection<Token> oldNodeTokens = tokenToEndpointMap.inverse().get(oldNode);
            if (!replacingTokens.containsAll(oldNodeTokens) || !oldNodeTokens.containsAll(replacingTokens))
            {
                throw new RuntimeException(String.format("Node %s is trying to replace node %s with tokens %s with a " +
                                                         "different set of tokens %s.", newNode, oldNode, oldNodeTokens,
                                                         replacingTokens));
            }

            logger.debug("Replacing {} with {}", newNode, oldNode);
            replacementToOriginal.put(newNode, oldNode);

            addBootstrapTokens(replacingTokens, newNode, oldNode);
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    public Optional<Endpoint> getReplacementNode(Endpoint endpoint)
    {
        return Optional.ofNullable(replacementToOriginal.inverse().get(endpoint));
    }

    public Optional<Endpoint> getReplacingNode(Endpoint endpoint)
    {
        return Optional.ofNullable((replacementToOriginal.get(endpoint)));
    }

    public void removeBootstrapTokens(Collection<Token> tokens)
    {
        assert tokens != null && !tokens.isEmpty();

        lock.writeLock().lock();
        try
        {
            for (Token token : tokens)
                bootstrapTokens.remove(token);
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    public void addLeavingEndpoint(Endpoint endpoint)
    {
        assert endpoint != null;

        lock.writeLock().lock();
        try
        {
            leavingEndpoints.add(endpoint);
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    /**
     * Add a new moving endpoint
     * @param token token which is node moving to
     * @param endpoint address of the moving node
     */
    public void addMovingEndpoint(Token token, Endpoint endpoint)
    {
        assert endpoint != null;

        lock.writeLock().lock();

        try
        {
            movingEndpoints.add(Pair.create(token, endpoint));
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    public void removeEndpoint(Endpoint endpoint)
    {
        assert endpoint != null;

        lock.writeLock().lock();
        try
        {
            bootstrapTokens.removeValue(endpoint);
            tokenToEndpointMap.removeValue(endpoint);
            topology.removeEndpoint(endpoint);
            leavingEndpoints.remove(endpoint);
            if (replacementToOriginal.remove(endpoint) != null)
            {
                logger.debug("Node {} failed during replace.", endpoint);
            }
            allEndpoints.remove(endpoint);
            sortedTokens = sortTokens();
            invalidateCachedRings();
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    /**
     * This is called when the snitch properties for this endpoint are updated, see CASSANDRA-10238.
     */
    public void updateTopology(Endpoint endpoint)
    {
        assert endpoint != null;

        lock.writeLock().lock();
        try
        {
            logger.info("Updating topology for {}", endpoint);
            topology.updateEndpoint(endpoint);
            invalidateCachedRings();
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    /**
     * This is called when the snitch properties for many endpoints are updated, it will update
     * the topology mappings of any endpoints whose snitch has changed, see CASSANDRA-10238.
     */
    public void updateTopology()
    {
        lock.writeLock().lock();
        try
        {
            logger.info("Updating topology for all endpoints that have changed");
            topology.updateEndpoints();
            invalidateCachedRings();
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    /**
     * Remove pair of token/address from moving endpoints
     * @param endpoint address of the moving node
     */
    public void removeFromMoving(Endpoint endpoint)
    {
        assert endpoint != null;

        lock.writeLock().lock();
        try
        {
            for (Pair<Token, Endpoint> pair : movingEndpoints)
            {
                if (pair.right.equals(endpoint))
                {
                    movingEndpoints.remove(pair);
                    break;
                }
            }

            invalidateCachedRings();
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    public Collection<Token> getTokens(Endpoint endpoint)
    {
        assert endpoint != null;
        assert isMember(endpoint); // don't want to return nulls

        lock.readLock().lock();
        try
        {
            return new ArrayList<>(tokenToEndpointMap.inverse().get(endpoint));
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    @Deprecated
    public Token getToken(Endpoint endpoint)
    {
        return getTokens(endpoint).iterator().next();
    }

    public boolean isMember(Endpoint endpoint)
    {
        assert endpoint != null;

        lock.readLock().lock();
        try
        {
            return tokenToEndpointMap.inverse().containsKey(endpoint);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    public boolean isLeaving(Endpoint endpoint)
    {
        assert endpoint != null;

        lock.readLock().lock();
        try
        {
            return leavingEndpoints.contains(endpoint);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    public boolean isMoving(Endpoint endpoint)
    {
        assert endpoint != null;

        lock.readLock().lock();

        try
        {
            for (Pair<Token, Endpoint> pair : movingEndpoints)
            {
                if (pair.right.equals(endpoint))
                    return true;
            }

            return false;
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    private final AtomicReference<TokenMetadata> cachedTokenMap = new AtomicReference<>();

    /**
     * Create a copy of TokenMetadata with only tokenToEndpointMap. That is, pending ranges,
     * bootstrap tokens and leaving endpoints are not included in the copy.
     */
    public TokenMetadata cloneOnlyTokenMap()
    {
        lock.readLock().lock();
        try
        {
            return new TokenMetadata(SortedBiMultiValMap.create(tokenToEndpointMap),
                                     new HashSet<>(allEndpoints),
                                     new Topology(topology),
                                     partitioner);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    /**
     * Return a cached TokenMetadata with only tokenToEndpointMap, i.e., the same as cloneOnlyTokenMap but
     * uses a cached copy that is invalided when the ring changes, so in the common case
     * no extra locking is required.
     *
     * Callers must *NOT* mutate the returned metadata object.
     */
    public TokenMetadata cachedOnlyTokenMap()
    {
        TokenMetadata tm = cachedTokenMap.get();
        if (tm != null)
            return tm;

        // synchronize to prevent thundering herd (CASSANDRA-6345)
        synchronized (this)
        {
            if ((tm = cachedTokenMap.get()) != null)
                return tm;

            tm = cloneOnlyTokenMap();
            cachedTokenMap.set(tm);
            return tm;
        }
    }

    /**
     * Create a copy of TokenMetadata with tokenToEndpointMap reflecting situation after all
     * current leave operations have finished.
     *
     * @return new token metadata
     */
    public TokenMetadata cloneAfterAllLeft()
    {
        lock.readLock().lock();
        try
        {
            return removeEndpoints(cloneOnlyTokenMap(), leavingEndpoints);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    private static TokenMetadata removeEndpoints(TokenMetadata allLeftMetadata, Set<Endpoint> leavingEndpoints)
    {
        for (Endpoint endpoint : leavingEndpoints)
            allLeftMetadata.removeEndpoint(endpoint);

        return allLeftMetadata;
    }

    /**
     * Create a copy of TokenMetadata with tokenToEndpointMap reflecting situation after all
     * current leave, and move operations have finished.
     *
     * @return new token metadata
     */
    public TokenMetadata cloneAfterAllSettled()
    {
        lock.readLock().lock();

        try
        {
            TokenMetadata metadata = cloneOnlyTokenMap();

            for (Endpoint endpoint : leavingEndpoints)
                metadata.removeEndpoint(endpoint);


            for (Pair<Token, Endpoint> pair : movingEndpoints)
                metadata.updateNormalToken(pair.left, pair.right);

            return metadata;
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    public Endpoint getEndpoint(Token token)
    {
        lock.readLock().lock();
        try
        {
            return tokenToEndpointMap.get(token);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    public Collection<Range<Token>> getPrimaryRangesFor(Collection<Token> tokens)
    {
        Collection<Range<Token>> ranges = new ArrayList<>(tokens.size());
        for (Token right : tokens)
            ranges.add(new Range<>(getPredecessor(right), right));
        return ranges;
    }

    @Deprecated
    public Range<Token> getPrimaryRangeFor(Token right)
    {
        return getPrimaryRangesFor(Arrays.asList(right)).iterator().next();
    }

    public ArrayList<Token> sortedTokens()
    {
        return sortedTokens;
    }

    public Multimap<Range<Token>, Endpoint> getPendingRangesMM(String keyspaceName)
    {
        Multimap<Range<Token>, Endpoint> map = HashMultimap.create();
        PendingRangeMaps pendingRangeMaps = this.pendingRanges.get(keyspaceName);

        if (pendingRangeMaps != null)
        {
            for (Map.Entry<Range<Token>, List<Endpoint>> entry : pendingRangeMaps)
            {
                Range<Token> range = entry.getKey();
                for (Endpoint address : entry.getValue())
                {
                    map.put(range, address);
                }
            }
        }

        return map;
    }

    /** a mutable map may be returned but caller should not modify it */
    public PendingRangeMaps getPendingRanges(String keyspaceName)
    {
        return this.pendingRanges.get(keyspaceName);
    }

    public List<Range<Token>> getPendingRanges(String keyspaceName, Endpoint endpoint)
    {
        List<Range<Token>> ranges = new ArrayList<>();
        for (Map.Entry<Range<Token>, Endpoint> entry : getPendingRangesMM(keyspaceName).entries())
        {
            if (entry.getValue().equals(endpoint))
            {
                ranges.add(entry.getKey());
            }
        }
        return ranges;
    }

     /**
     * Calculate pending ranges according to bootsrapping and leaving nodes. Reasoning is:
     *
     * (1) When in doubt, it is better to write too much to a node than too little. That is, if
     * there are multiple nodes moving, calculate the biggest ranges a node could have. Cleaning
     * up unneeded data afterwards is better than missing writes during movement.
     * (2) When a node leaves, ranges for other nodes can only grow (a node might get additional
     * ranges, but it will not lose any of its current ranges as a result of a leave). Therefore
     * we will first remove _all_ leaving tokens for the sake of calculation and then check what
     * ranges would go where if all nodes are to leave. This way we get the biggest possible
     * ranges with regard current leave operations, covering all subsets of possible final range
     * values.
     * (3) When a node bootstraps, ranges of other nodes can only get smaller. Without doing
     * complex calculations to see if multiple bootstraps overlap, we simply base calculations
     * on the same token ring used before (reflecting situation after all leave operations have
     * completed). Bootstrapping nodes will be added and removed one by one to that metadata and
     * checked what their ranges would be. This will give us the biggest possible ranges the
     * node could have. It might be that other bootstraps make our actual final ranges smaller,
     * but it does not matter as we can clean up the data afterwards.
     *
     * NOTE: This is heavy and ineffective operation. This will be done only once when a node
     * changes state in the cluster, so it should be manageable.
     */
    public void calculatePendingRanges(AbstractReplicationStrategy strategy, String keyspaceName)
    {
        // avoid race between both branches - do not use a lock here as this will block any other unrelated operations!
        synchronized (pendingRanges)
        {
            if (bootstrapTokens.isEmpty() && leavingEndpoints.isEmpty() && movingEndpoints.isEmpty())
            {
                if (logger.isTraceEnabled())
                    logger.trace("No bootstrapping, leaving or moving nodes -> empty pending ranges for {}", keyspaceName);

                pendingRanges.put(keyspaceName, new PendingRangeMaps());
            }
            else
            {
                if (logger.isDebugEnabled())
                    logger.debug("Starting pending range calculation for {}", keyspaceName);

                long startedAt = System.currentTimeMillis();

                // create clone of current state
                BiMultiValMap<Token, Endpoint> bootstrapTokensClone = new BiMultiValMap<>();
                Set<Endpoint> leavingEndpointsClone = new HashSet<>();
                Set<Pair<Token, Endpoint>> movingEndpointsClone = new HashSet<>();
                TokenMetadata metadata;

                lock.readLock().lock();
                try
                {
                    bootstrapTokensClone.putAll(this.bootstrapTokens);
                    leavingEndpointsClone.addAll(this.leavingEndpoints);
                    movingEndpointsClone.addAll(this.movingEndpoints);
                    metadata = this.cloneOnlyTokenMap();
                }
                finally
                {
                    lock.readLock().unlock();
                }

                pendingRanges.put(keyspaceName, calculatePendingRanges(strategy, metadata, bootstrapTokensClone,
                                                                       leavingEndpointsClone, movingEndpointsClone));
                long took = System.currentTimeMillis() - startedAt;

                if (logger.isDebugEnabled())
                    logger.debug("Pending range calculation for {} completed (took: {}ms)", keyspaceName, took);
                if (logger.isTraceEnabled())
                    logger.trace("Calculated pending ranges for {}:\n{}", keyspaceName, (pendingRanges.isEmpty() ? "<empty>" : printPendingRanges()));
            }
        }
    }

    /**
     * @see TokenMetadata#calculatePendingRanges(AbstractReplicationStrategy, String)
     */
    private static PendingRangeMaps calculatePendingRanges(AbstractReplicationStrategy strategy,
                                                           TokenMetadata metadata,
                                                           BiMultiValMap<Token, Endpoint> bootstrapTokens,
                                                           Set<Endpoint> leavingEndpoints,
                                                           Set<Pair<Token, Endpoint>> movingEndpoints)
    {
        PendingRangeMaps newPendingRanges = new PendingRangeMaps();

        Multimap<Endpoint, Range<Token>> endpointRanges = strategy.getAddressRanges(metadata);

        // Copy of metadata reflecting the situation after all leave operations are finished.
        TokenMetadata allLeftMetadata = removeEndpoints(metadata.cloneOnlyTokenMap(), leavingEndpoints);

        // get all ranges that will be affected by leaving nodes
        Set<Range<Token>> affectedRanges = new HashSet<Range<Token>>();
        for (Endpoint endpoint : leavingEndpoints)
            affectedRanges.addAll(endpointRanges.get(endpoint));

        // for each of those ranges, find what new nodes will be responsible for the range when
        // all leaving nodes are gone.
        for (Range<Token> range : affectedRanges)
        {
            Set<Endpoint> currentEndpoints = ImmutableSet.copyOf(strategy.calculateNaturalEndpoints(range.right, metadata));
            Set<Endpoint> newEndpoints = ImmutableSet.copyOf(strategy.calculateNaturalEndpoints(range.right, allLeftMetadata));
            for (Endpoint address : Sets.difference(newEndpoints, currentEndpoints))
            {
                newPendingRanges.addPendingRange(range, address);
            }
        }

        // At this stage newPendingRanges has been updated according to leave operations. We can
        // now continue the calculation by checking bootstrapping nodes.

        // For each of the bootstrapping nodes, simply add and remove them one by one to
        // allLeftMetadata and check in between what their ranges would be.
        Multimap<Endpoint, Token> bootstrapAddresses = bootstrapTokens.inverse();
        for (Endpoint endpoint : bootstrapAddresses.keySet())
        {
            Collection<Token> tokens = bootstrapAddresses.get(endpoint);

            allLeftMetadata.updateNormalTokens(tokens, endpoint);
            for (Range<Token> range : strategy.getAddressRanges(allLeftMetadata).get(endpoint))
            {
                newPendingRanges.addPendingRange(range, endpoint);
            }
            allLeftMetadata.removeEndpoint(endpoint);
        }

        // At this stage newPendingRanges has been updated according to leaving and bootstrapping nodes.
        // We can now finish the calculation by checking moving nodes.

        // For each of the moving nodes, we do the same thing we did for bootstrapping:
        // simply add and remove them one by one to allLeftMetadata and check in between what their ranges would be.
        for (Pair<Token, Endpoint> moving : movingEndpoints)
        {
            //Calculate all the ranges which will could be affected. This will include the ranges before and after the move.
            Set<Range<Token>> moveAffectedRanges = new HashSet<>();
            Endpoint endpoint = moving.right; // address of the moving node
            //Add ranges before the move
            for (Range<Token> range : strategy.getAddressRanges(allLeftMetadata).get(endpoint))
            {
                moveAffectedRanges.add(range);
            }

            allLeftMetadata.updateNormalToken(moving.left, endpoint);
            //Add ranges after the move
            for (Range<Token> range : strategy.getAddressRanges(allLeftMetadata).get(endpoint))
            {
                moveAffectedRanges.add(range);
            }

            for(Range<Token> range : moveAffectedRanges)
            {
                Set<Endpoint> currentEndpoints = ImmutableSet.copyOf(strategy.calculateNaturalEndpoints(range.right, metadata));
                Set<Endpoint> newEndpoints = ImmutableSet.copyOf(strategy.calculateNaturalEndpoints(range.right, allLeftMetadata));
                Set<Endpoint> difference = Sets.difference(newEndpoints, currentEndpoints);
                for(final Endpoint address : difference)
                {
                    Collection<Range<Token>> newRanges = strategy.getAddressRanges(allLeftMetadata).get(address);
                    Collection<Range<Token>> oldRanges = strategy.getAddressRanges(metadata).get(address);
                    //We want to get rid of any ranges which the node is currently getting.
                    newRanges.removeAll(oldRanges);

                    for(Range<Token> newRange : newRanges)
                    {
                        for(Range<Token> pendingRange : newRange.subtractAll(oldRanges))
                        {
                            newPendingRanges.addPendingRange(pendingRange, address);
                        }
                    }
                }
            }

            allLeftMetadata.removeEndpoint(endpoint);
        }

        return newPendingRanges;
    }

    public Token getPredecessor(Token token)
    {
        List<Token> tokens = sortedTokens();
        int index = Collections.binarySearch(tokens, token);
        assert index >= 0 : token + " not found in " + StringUtils.join(tokenToEndpointMap.keySet(), ", ");
        return index == 0 ? tokens.get(tokens.size() - 1) : tokens.get(index - 1);
    }

    public Token getSuccessor(Token token)
    {
        List<Token> tokens = sortedTokens();
        int index = Collections.binarySearch(tokens, token);
        assert index >= 0 : token + " not found in " + StringUtils.join(tokenToEndpointMap.keySet(), ", ");
        return (index == (tokens.size() - 1)) ? tokens.get(0) : tokens.get(index + 1);
    }

    /** @return a copy of the bootstrapping tokens map */
    public BiMultiValMap<Token, Endpoint> getBootstrapTokens()
    {
        lock.readLock().lock();
        try
        {
            return new BiMultiValMap<>(bootstrapTokens);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    public Set<Endpoint> getAllEndpoints()
    {
        lock.readLock().lock();
        try
        {
            return ImmutableSet.copyOf(allEndpoints);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    /**
     * We think the size() operation is safe enough, so we call it without the read lock on purpose.
     *
     * see CASSANDRA-12999
     */
    public int getSizeOfAllEndpoints()
    {
        return allEndpoints.size();
    }

    /** caller should not modify leavingEndpoints */
    public Set<Endpoint> getLeavingEndpoints()
    {
        lock.readLock().lock();
        try
        {
            return ImmutableSet.copyOf(leavingEndpoints);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    /**
     * We think the size() operation is safe enough, so we call it without the read lock on purpose.
     *
     * see CASSANDRA-12999
     */
    public int getSizeOfLeavingEndpoints()
    {
        return leavingEndpoints.size();
    }

    /**
     * Endpoints which are migrating to the new tokens
     * @return set of addresses of moving endpoints
     */
    public Set<Pair<Token, Endpoint>> getMovingEndpoints()
    {
        lock.readLock().lock();
        try
        {
            return ImmutableSet.copyOf(movingEndpoints);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    /**
     * We think the size() operation is safe enough, so we call it without the read lock on purpose.
     *
     * see CASSANDRA-12999
     */
    public int getSizeOfMovingEndpoints()
    {
        return movingEndpoints.size();
    }

    public static int firstTokenIndex(final ArrayList<Token> ring, Token start, boolean insertMin)
    {
        assert ring.size() > 0;
        // insert the minimum token (at index == -1) if we were asked to include it and it isn't a member of the ring
        int i = Collections.binarySearch(ring, start);
        if (i < 0)
        {
            i = (i + 1) * (-1);
            if (i >= ring.size())
                i = insertMin ? -1 : 0;
        }
        return i;
    }

    public static Token firstToken(final ArrayList<Token> ring, Token start)
    {
        return ring.get(firstTokenIndex(ring, start, false));
    }

    /**
     * iterator over the Tokens in the given ring, starting with the token for the node owning start
     * (which does not have to be a Token in the ring)
     * @param includeMin True if the minimum token should be returned in the ring even if it has no owner.
     */
    public static Iterator<Token> ringIterator(final ArrayList<Token> ring, Token start, boolean includeMin)
    {
        if (ring.isEmpty())
            return includeMin ? Iterators.singletonIterator(start.getPartitioner().getMinimumToken())
                              : Collections.emptyIterator();

        final boolean insertMin = includeMin && !ring.get(0).isMinimum();
        final int startIndex = firstTokenIndex(ring, start, insertMin);
        return new AbstractIterator<Token>()
        {
            int j = startIndex;
            protected Token computeNext()
            {
                if (j < -1)
                    return endOfData();
                try
                {
                    // return minimum for index == -1
                    if (j == -1)
                        return start.getPartitioner().getMinimumToken();
                    // return ring token for other indexes
                    return ring.get(j);
                }
                finally
                {
                    j++;
                    if (j == ring.size())
                        j = insertMin ? -1 : 0;
                    if (j == startIndex)
                        // end iteration
                        j = -2;
                }
            }
        };
    }

    /** used by tests */
    public void clearUnsafe()
    {
        lock.writeLock().lock();
        try
        {
            tokenToEndpointMap.clear();
            allEndpoints.clear();
            bootstrapTokens.clear();
            leavingEndpoints.clear();
            pendingRanges.clear();
            movingEndpoints.clear();
            sortedTokens.clear();
            topology.clear();
            invalidateCachedRings();
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        lock.readLock().lock();
        try
        {
            Multimap<Endpoint, Token> endpointToTokenMap = tokenToEndpointMap.inverse();
            Set<Endpoint> eps = endpointToTokenMap.keySet();

            if (!eps.isEmpty())
            {
                sb.append("Normal Tokens:");
                sb.append(System.getProperty("line.separator"));
                for (Endpoint ep : eps)
                {
                    sb.append(ep.getPreferredAddress().toString(true)); // getting the preferred address for the printout
                    sb.append(':');
                    sb.append(endpointToTokenMap.get(ep));
                    sb.append(System.getProperty("line.separator"));
                }
            }

            if (!bootstrapTokens.isEmpty())
            {
                sb.append("Bootstrapping Tokens:" );
                sb.append(System.getProperty("line.separator"));
                for (Map.Entry<Token, Endpoint> entry : bootstrapTokens.entrySet())
                {
                    sb.append(entry.getValue()).append(':').append(entry.getKey());
                    sb.append(System.getProperty("line.separator"));
                }
            }

            if (!leavingEndpoints.isEmpty())
            {
                sb.append("Leaving Endpoints:");
                sb.append(System.getProperty("line.separator"));
                for (Endpoint ep : leavingEndpoints)
                {
                    sb.append(ep);
                    sb.append(System.getProperty("line.separator"));
                }
            }

            if (!pendingRanges.isEmpty())
            {
                sb.append("Pending Ranges:");
                sb.append(System.getProperty("line.separator"));
                sb.append(printPendingRanges());
            }
        }
        finally
        {
            lock.readLock().unlock();
        }

        return sb.toString();
    }

    private String printPendingRanges()
    {
        StringBuilder sb = new StringBuilder();

        for (PendingRangeMaps pendingRangeMaps : pendingRanges.values())
        {
            sb.append(pendingRangeMaps.printPendingRanges());
        }

        return sb.toString();
    }

    public Collection<Endpoint> pendingEndpointsFor(Token token, String keyspaceName)
    {
        PendingRangeMaps pendingRangeMaps = this.pendingRanges.get(keyspaceName);
        if (pendingRangeMaps == null)
            return Collections.emptyList();

        return pendingRangeMaps.pendingEndpointsFor(token);
    }

    /**
     * @deprecated retained for benefit of old tests
     */
    public Collection<Endpoint> getWriteEndpoints(Token token, String keyspaceName, Collection<Endpoint> naturalEndpoints)
    {
        return ImmutableList.copyOf(Iterables.concat(naturalEndpoints, pendingEndpointsFor(token, keyspaceName)));
    }

    /** @return an endpoint to token multimap representation of tokenToEndpointMap (a copy) */
    public Multimap<Endpoint, Token> getEndpointToTokenMapForReading()
    {
        lock.readLock().lock();
        try
        {
            Multimap<Endpoint, Token> cloned = HashMultimap.create();
            for (Map.Entry<Token, Endpoint> entry : tokenToEndpointMap.entrySet())
                cloned.put(entry.getValue(), entry.getKey());
            return cloned;
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    /**
     * @return a (stable copy, won't be modified) Token to Endpoint map for all the normal and bootstrapping nodes
     *         in the cluster.
     */
    public Map<Token, Endpoint> getNormalAndBootstrappingTokenToEndpointMap()
    {
        lock.readLock().lock();
        try
        {
            Map<Token, Endpoint> map = new HashMap<>(tokenToEndpointMap.size() + bootstrapTokens.size());
            map.putAll(tokenToEndpointMap);
            map.putAll(bootstrapTokens);
            return map;
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    /**
     * @return the Topology map of nodes to DCs + Racks
     *
     * This is only allowed when a copy has been made of TokenMetadata, to avoid concurrent modifications
     * when Topology methods are subsequently used by the caller.
     */
    public Topology getTopology()
    {
        assert this != StorageService.instance.getTokenMetadata();
        return topology;
    }

    public long getRingVersion()
    {
        return ringVersion;
    }

    public void invalidateCachedRings()
    {
        ringVersion++;
        cachedTokenMap.set(null);
    }

    public DecoratedKey decorateKey(ByteBuffer key)
    {
        return partitioner.decorateKey(key);
    }

    /**
     * Tracks the assignment of racks and endpoints in each datacenter for all the "normal" endpoints
     * in this TokenMetadata. This allows faster calculation of endpoints in NetworkTopologyStrategy.
     */
    public static class Topology
    {
        /** multi-map of DC to endpoints in that DC */
        private final Multimap<String, Endpoint> dcEndpoints;
        /** map of DC to multi-map of rack to endpoints in that rack */
        private final Map<String, Multimap<String, Endpoint>> dcRacks;
        /** reverse-lookup map for endpoint to current known dc/rack assignment */
        private final Map<Endpoint, Pair<String, String>> currentLocations;

        Topology()
        {
            dcEndpoints = HashMultimap.create();
            dcRacks = new HashMap<>();
            currentLocations = new HashMap<>();
        }

        void clear()
        {
            dcEndpoints.clear();
            dcRacks.clear();
            currentLocations.clear();
        }

        /**
         * construct deep-copy of other
         */
        Topology(Topology other)
        {
            dcEndpoints = HashMultimap.create(other.dcEndpoints);
            dcRacks = new HashMap<>();
            for (String dc : other.dcRacks.keySet())
                dcRacks.put(dc, HashMultimap.create(other.dcRacks.get(dc)));
            currentLocations = new HashMap<>(other.currentLocations);
        }

        /**
         * Stores current DC/rack assignment for ep
         */
        void addEndpoint(Endpoint ep)
        {
            IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
            String dc = snitch.getDatacenter(ep);
            String rack = snitch.getRack(ep);
            Pair<String, String> current = currentLocations.get(ep);
            if (current != null)
            {
                if (current.left.equals(dc) && current.right.equals(rack))
                    return;
                doRemoveEndpoint(ep, current);
            }

            doAddEndpoint(ep, dc, rack);
        }

        private void doAddEndpoint(Endpoint ep, String dc, String rack)
        {
            dcEndpoints.put(dc, ep);

            if (!dcRacks.containsKey(dc))
                dcRacks.put(dc, HashMultimap.create());
            dcRacks.get(dc).put(rack, ep);

            currentLocations.put(ep, Pair.create(dc, rack));
        }

        /**
         * Removes current DC/rack assignment for ep
         */
        void removeEndpoint(Endpoint ep)
        {
            if (!currentLocations.containsKey(ep))
                return;

            doRemoveEndpoint(ep, currentLocations.remove(ep));
        }

        private void doRemoveEndpoint(Endpoint ep, Pair<String, String> current)
        {
            dcRacks.get(current.left).remove(current.right, ep);
            dcEndpoints.remove(current.left, ep);
        }

        void updateEndpoint(Endpoint ep)
        {
            IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
            if (snitch == null || !currentLocations.containsKey(ep))
                return;

           updateEndpoint(ep, snitch);
        }

        void updateEndpoints()
        {
            IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
            if (snitch == null)
                return;

            for (Endpoint ep : currentLocations.keySet())
                updateEndpoint(ep, snitch);
        }

        private void updateEndpoint(Endpoint ep, IEndpointSnitch snitch)
        {
            Pair<String, String> current = currentLocations.get(ep);
            String dc = snitch.getDatacenter(ep);
            String rack = snitch.getRack(ep);
            if (dc.equals(current.left) && rack.equals(current.right))
                return;

            doRemoveEndpoint(ep, current);
            doAddEndpoint(ep, dc, rack);
        }

        /**
         * @return multi-map of DC to endpoints in that DC
         */
        public Multimap<String, Endpoint> getDatacenterEndpoints()
        {
            return dcEndpoints;
        }

        /**
         * @return map of DC to multi-map of rack to endpoints in that rack
         */
        public Map<String, Multimap<String, Endpoint>> getDatacenterRacks()
        {
            return dcRacks;
        }

        /**
         * @return The DC and rack of the given endpoint.
         */
        public Pair<String, String> getLocation(Endpoint addr)
        {
            return currentLocations.get(addr);
        }

    }
}
