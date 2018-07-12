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
package org.apache.cassandra.gms;

import java.lang.management.ManagementFactory;
import java.net.UnknownHostException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import javax.annotation.Nullable;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Uninterruptibles;

import org.apache.cassandra.locator.Endpoint;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.DebuggableScheduledThreadPoolExecutor;
import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutor;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.net.IAsyncCallback;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;

/**
 * This module is responsible for Gossiping information for the local endpoint. This abstraction
 * maintains the list of live and dead endpoints. Periodically i.e. every 1 second this module
 * chooses a random node and initiates a round of Gossip with it. A round of Gossip involves 3
 * rounds of messaging. For instance if node A wants to initiate a round of Gossip with node B
 * it starts off by sending node B a GossipDigestSynMessage. Node B on receipt of this message
 * sends node A a GossipDigestAckMessage. On receipt of this message node A sends node B a
 * GossipDigestAck2Message which completes a round of Gossip. This module as and when it hears one
 * of the three above mentioned messages updates the Failure Detector with the liveness information.
 * Upon hearing a GossipShutdownMessage, this module will instantly mark the remote node as down in
 * the Failure Detector.
 */

public class Gossiper implements IFailureDetectionEventListener, GossiperMBean
{
    public static final String MBEAN_NAME = "org.apache.cassandra.net:type=Gossiper";

    private static final DebuggableScheduledThreadPoolExecutor executor = new DebuggableScheduledThreadPoolExecutor("GossipTasks");

    private volatile ScheduledFuture<?> scheduledGossipTask;
    private static final ReentrantLock taskLock = new ReentrantLock();
    public final static int intervalInMillis = 1000;
    public final static int QUARANTINE_DELAY = StorageService.RING_DELAY * 2;
    private static final Logger logger = LoggerFactory.getLogger(Gossiper.class);
    public static final Gossiper instance = new Gossiper(true);

    // Timestamp to prevent processing any in-flight messages for we've not send any SYN yet, see CASSANDRA-12653.
    volatile long firstSynSendAt = 0L;

    public static final long aVeryLongTime = 259200 * 1000; // 3 days

    // Maximimum difference between generation value and local time we are willing to accept about a peer
    static final int MAX_GENERATION_DIFFERENCE = 86400 * 365;
    private long fatClientTimeout;
    private final Random random = new Random();

    /* subscribers for interest in EndpointState change */
    private final List<IEndpointStateChangeSubscriber> subscribers = new CopyOnWriteArrayList<IEndpointStateChangeSubscriber>();

    /* live member set */
    private final Set<Endpoint> liveEndpoints = new ConcurrentSkipListSet<>();

    /* unreachable member set */
    private final Map<Endpoint, Long> unreachableEndpoints = new ConcurrentHashMap<>();

    /* initial seeds for joining the cluster */
    @VisibleForTesting
    final Set<Endpoint> seeds = new ConcurrentSkipListSet<>();

    /* map where key is the endpoint and value is the state associated with the endpoint */
    //final ConcurrentMap<Endpoint, EndpointState> gossipEndpoints = new ConcurrentHashMap<>();
    final ConcurrentSkipListSet<Endpoint> gossipEndpoints = new ConcurrentSkipListSet<>();

    /* map where key is endpoint and value is timestamp when this endpoint was removed from
     * gossip. We will ignore any gossip regarding these endpoints for QUARANTINE_DELAY time
     * after removal to prevent nodes from falsely reincarnating during the time when removal
     * gossip gets propagated to all nodes */
    private final Map<Endpoint, Long> justRemovedEndpoints = new ConcurrentHashMap<>();

    private final Map<Endpoint, Long> expireTimeEndpointMap = new ConcurrentHashMap<>();

    private volatile boolean inShadowRound = false;
    // seeds gathered during shadow round that indicated to be in the shadow round phase as well
    private final Set<Endpoint> seedsInShadowRound = new ConcurrentSkipListSet<>();
    // endpoint states as gathered during shadow round
    private final ConcurrentSkipListSet<Endpoint> shadowRoundEndpoints = new ConcurrentSkipListSet<>();

    private volatile long lastProcessedMessageAt = System.currentTimeMillis();

    private class GossipTask implements Runnable
    {
        public void run()
        {
            try
            {
                //wait on messaging service to start listening
                MessagingService.instance().waitUntilListening();

                taskLock.lock();

                /* Update the local heartbeat counter. */
                final Endpoint localEndpoint = Endpoint.getLocalEndpoint();
                localEndpoint.state.getHeartBeatState().updateHeartBeat();
                if (logger.isTraceEnabled())
                    logger.trace("My heartbeat is now {}", localEndpoint.state.getHeartBeatState().getHeartBeatVersion());
                final List<GossipDigest> gDigests = new ArrayList<GossipDigest>();
                Gossiper.instance.makeRandomGossipDigest(gDigests);

                if (!gDigests.isEmpty())
                {
                    GossipDigestSyn digestSynMessage = new GossipDigestSyn(DatabaseDescriptor.getClusterName(),
                                                                           DatabaseDescriptor.getPartitionerName(),
                                                                           gDigests);
                    MessageOut<GossipDigestSyn> message = new MessageOut<GossipDigestSyn>(MessagingService.Verb.GOSSIP_DIGEST_SYN,
                                                                                          digestSynMessage,
                                                                                          GossipDigestSyn.serializer);
                    /* Gossip to some random live member */
                    boolean gossipedToSeed = doGossipToLiveMember(message);

                    /* Gossip to some unreachable member with some probability to check if he is back up */
                    maybeGossipToUnreachableMember(message);

                    /* Gossip to a seed if we did not do so above, or we have seen less nodes
                       than there are seeds.  This prevents partitions where each group of nodes
                       is only gossiping to a subset of the seeds.

                       The most straightforward check would be to check that all the seeds have been
                       verified either as live or unreachable.  To avoid that computation each round,
                       we reason that:

                       either all the live nodes are seeds, in which case non-seeds that come online
                       will introduce themselves to a member of the ring by definition,

                       or there is at least one non-seed node in the list, in which case eventually
                       someone will gossip to it, and then do a gossip to a random seed from the
                       gossipedToSeed check.

                       See CASSANDRA-150 for more exposition. */
                    if (!gossipedToSeed || liveEndpoints.size() < seeds.size())
                        maybeGossipToSeed(message);

                    doStatusCheck();
                }
            }
            catch (Exception e)
            {
                JVMStabilityInspector.inspectThrowable(e);
                logger.error("Gossip error", e);
            }
            finally
            {
                taskLock.unlock();
            }
        }
    }

    Gossiper(boolean registerJmx)
    {
        // half of QUARATINE_DELAY, to ensure justRemovedEndpoints has enough leeway to prevent re-gossip
        fatClientTimeout = (QUARANTINE_DELAY / 2);
        /* register with the Failure Detector for receiving Failure detector events */
        FailureDetector.instance.registerFailureDetectionEventListener(this);

        // Register this instance with JMX
        if (registerJmx)
        {
            try
            {
                MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
                mbs.registerMBean(this, new ObjectName(MBEAN_NAME));
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    public void setLastProcessedMessageAt(long timeInMillis)
    {
        this.lastProcessedMessageAt = timeInMillis;
    }

    public boolean seenAnySeed()
    {
        for (Endpoint ep : gossipEndpoints)
        {
            if (seeds.contains(ep))
                return true;

            /**
             *  The following block is (supposedly) not required. There could only be 1 alive node with the IP addresses of the provided Endpoint,
             *  so checking if seeds includes another endpoint with addresses that belong to the provided endpoint doesn't make
             *  any sense.
             *

            try
            {
                VersionedValue internalIp = ep.state.getApplicationState(ApplicationState.INTERNAL_IP);
                VersionedValue internalIpAndPort = ep.state.getApplicationState(ApplicationState.INTERNAL_ADDRESS_AND_PORT);
                InetAddressAndPort endpointAddress = null;
                if (internalIpAndPort != null)
                {
                    endpointAddress = InetAddressAndPort.getByName(internalIpAndPort.value);
                }
                else if (internalIp != null)
                {
                    endpointAddress = InetAddressAndPort.getByName(internalIp.value);
                }
                if (endpointAddress != null && seedContainsAddress(endpointAddress))
                    return true;
            }
            catch (UnknownHostException e)
            {
                throw new RuntimeException(e);
            } */
        }
        return false;
    }

    /**
     * Register for interesting state changes.
     *
     * @param subscriber module which implements the IEndpointStateChangeSubscriber
     */
    public void register(IEndpointStateChangeSubscriber subscriber)
    {
        subscribers.add(subscriber);
    }

    /**
     * Unregister interest for state changes.
     *
     * @param subscriber module which implements the IEndpointStateChangeSubscriber
     */
    public void unregister(IEndpointStateChangeSubscriber subscriber)
    {
        subscribers.remove(subscriber);
    }

    /**
     * @return a list of live gossip participants, including fat clients
     */
    public Set<Endpoint> getLiveMembers()
    {
        Set<Endpoint> liveMembers = new HashSet<>(liveEndpoints);
        liveMembers.add(Endpoint.getLocalEndpoint());
        return liveMembers;
    }

    // TODO: NOT USED
    /**
     * @return a list of live ring members.
     */
    @Deprecated
    public Set<Endpoint> getLiveTokenOwners()
    {
        return StorageService.instance.getLiveRingMembers(true);
    }

    /**
     * @return a list of unreachable gossip participants, including fat clients
     */
    public Set<Endpoint> getUnreachableMembers()
    {
        return unreachableEndpoints.keySet();
    }

    /**
     * @return a list of unreachable token owners
     */
    public Set<Endpoint> getUnreachableTokenOwners()
    {
        Set<Endpoint> tokenOwners = new HashSet<>();
        for (Endpoint endpoint : unreachableEndpoints.keySet())
        {
            if (StorageService.instance.getTokenMetadata().isMember(endpoint))
                tokenOwners.add(endpoint);
        }

        return tokenOwners;
    }

    public long getEndpointDowntime(Endpoint ep)
    {
        Long downtime = unreachableEndpoints.get(ep);
        if (downtime != null)
            return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - downtime);
        else
            return 0L;
    }

    /**
     * This method is part of IFailureDetectionEventListener interface. This is invoked
     * by the Failure Detector when it convicts an end point.
     *
     * @param endpoint end point that is convicted.
     */
    public void convict(Endpoint endpoint, double phi)
    {
        if (endpoint.state == null)
            return;

        if (!endpoint.isAlive())
            return;

        logger.debug("Convicting {} with status {} - alive {}", endpoint, endpoint.getGossipStatus(), endpoint.isAlive());


        if (endpoint.isShutdown())
        {
            markAsShutdown(endpoint);
        }
        else
        {
            markDead(endpoint);
        }
    }

    /**
     * This method is used to mark a node as shutdown; that is it gracefully exited on its own and told us about it
     * @param endpoint endpoint that has shut itself down
     */
    protected void markAsShutdown(Endpoint endpoint)
    {
        if (endpoint.state == null)
            return;
        endpoint.state.addApplicationState(ApplicationState.STATUS_WITH_PORT, StorageService.instance.valueFactory.shutdown(true));
        endpoint.state.addApplicationState(ApplicationState.STATUS, StorageService.instance.valueFactory.shutdown(true));
        endpoint.state.addApplicationState(ApplicationState.RPC_READY, StorageService.instance.valueFactory.rpcReady(false));
        endpoint.state.getHeartBeatState().forceHighestPossibleVersionUnsafe();
        markDead(endpoint);
        FailureDetector.instance.forceConviction(endpoint);
    }

    /**
     * Checks if at least one of the seed endpoints (nodes) has the provided address
     * @param address
     * @return
     */
    private boolean seedContainsAddress(final InetAddressAndPort address)
    {
        return seeds.stream().filter(e -> e.hasAddress(address)).count() >= 1;
    }

    /**
     * Removes the endpoint from gossip completely
     *
     * @param endpoint endpoint to be removed from the current membership.
     */
    private void evictFromMembership(Endpoint endpoint)
    {
        unreachableEndpoints.remove(endpoint);
        gossipEndpoints.remove(endpoint);
        expireTimeEndpointMap.remove(endpoint);
        FailureDetector.instance.remove(endpoint);
        quarantineEndpoint(endpoint);
        if (logger.isDebugEnabled())
            logger.debug("evicting {} from gossip", endpoint.toString());
    }

    /**
     * Removes the endpoint from Gossip but retains endpoint state
     */
    public void removeEndpoint(Endpoint endpoint)
    {
        // do subscribers first so anything in the subscriber that depends on gossiper state won't get confused
        for (IEndpointStateChangeSubscriber subscriber : subscribers)
            subscriber.onRemove(endpoint);

        if(seeds.contains(endpoint))
        {
            buildSeedsList();
            seeds.remove(endpoint);
            logger.info("removed {} from seeds, updated seeds list = {}", endpoint, seeds);
        }

        liveEndpoints.remove(endpoint);
        unreachableEndpoints.remove(endpoint);
        MessagingService.instance().resetVersion(endpoint);
        quarantineEndpoint(endpoint);
        MessagingService.instance().destroyConnectionPool(endpoint.getPreferredAddress());
        if (logger.isDebugEnabled())
            logger.debug("removing endpoint {}", endpoint);
    }

    /**
     * Quarantines the endpoint for QUARANTINE_DELAY
     *
     * @param endpoint
     */
    private void quarantineEndpoint(Endpoint endpoint)
    {
        quarantineEndpoint(endpoint, System.currentTimeMillis());
    }

    /**
     * Quarantines the endpoint until quarantineExpiration + QUARANTINE_DELAY
     *
     * @param endpoint
     * @param quarantineExpiration
     */
    private void quarantineEndpoint(Endpoint endpoint, long quarantineExpiration)
    {
        justRemovedEndpoints.put(endpoint, quarantineExpiration);
    }

    /**
     * Quarantine endpoint specifically for replacement purposes.
     * @param endpoint
     */
    public void replacementQuarantine(Endpoint endpoint)
    {
        // remember, quarantineEndpoint will effectively already add QUARANTINE_DELAY, so this is 2x
        quarantineEndpoint(endpoint, System.currentTimeMillis() + QUARANTINE_DELAY);
    }

    /**
     * Remove the Endpoint and evict immediately, to avoid gossiping about this node.
     * This should only be called when a token is taken over by a new IP address.
     *
     * @param endpoint The endpoint that has been replaced
     */
    public void replacedEndpoint(Endpoint endpoint)
    {
        removeEndpoint(endpoint);
        evictFromMembership(endpoint);
        replacementQuarantine(endpoint);
    }

    /**
     * The gossip digest is built based on randomization
     * rather than just looping through the collection of live endpoints.
     *
     * @param gDigests list of Gossip Digests.
     */
    private void makeRandomGossipDigest(List<GossipDigest> gDigests)
    {
        int generation = 0;
        int maxVersion = 0;

        // local epstate will be part of gossipEndpoints
        List<Endpoint> endpoints = new ArrayList<>(gossipEndpoints);
        Collections.shuffle(endpoints, random);
        for (Endpoint endpoint : endpoints)
        {
            if (endpoint.state != null)
            {
                generation = endpoint.state.getHeartBeatState().getGeneration();
                maxVersion = endpoint.state.getMaxEndpointStateVersion();
            }
            gDigests.add(new GossipDigest(endpoint, generation, maxVersion));
        }

        if (logger.isTraceEnabled())
        {
            StringBuilder sb = new StringBuilder();
            for (GossipDigest gDigest : gDigests)
            {
                sb.append(gDigest);
                sb.append(" ");
            }
            logger.trace("Gossip Digests are : {}", sb);
        }
    }

    /**
     * This method will begin removing an existing endpoint from the cluster by spoofing its state
     * This should never be called unless this coordinator has had 'removenode' invoked
     *
     * @param endpointToRemove    - the endpoint being removed
     * @param localHostId - my own host ID for replication coordination
     */
    public void advertiseRemoving(Endpoint endpointToRemove, UUID localHostId)
    {
        // remember this node's generation
        int generation = endpointToRemove.state.getHeartBeatState().getGeneration();
        logger.info("Removing host: {}", endpointToRemove.getHostId());
        logger.info("Sleeping for {}ms to ensure {} does not change", StorageService.RING_DELAY, endpointToRemove.toString());
        Uninterruptibles.sleepUninterruptibly(StorageService.RING_DELAY, TimeUnit.MILLISECONDS);
        // make sure it did not change
        if (endpointToRemove.state.getHeartBeatState().getGeneration() != generation)
            throw new RuntimeException("Endpoint " + endpointToRemove.toString() + " generation changed while trying to remove it");
        // update the other node's generation to mimic it as if it had changed it itself
        logger.info("Advertising removal for {}", endpointToRemove.toString());
        endpointToRemove.state.updateTimestamp(); // make sure we don't evict it too soon
        endpointToRemove.state.getHeartBeatState().forceNewerGenerationUnsafe();
        Map<ApplicationState, VersionedValue> states = new EnumMap<>(ApplicationState.class);
        states.put(ApplicationState.STATUS_WITH_PORT, StorageService.instance.valueFactory.removingNonlocal(endpointToRemove.getHostId()));
        states.put(ApplicationState.STATUS, StorageService.instance.valueFactory.removingNonlocal(endpointToRemove.getHostId()));
        states.put(ApplicationState.REMOVAL_COORDINATOR, StorageService.instance.valueFactory.removalCoordinator(localHostId));
        endpointToRemove.state.addApplicationStates(states);
    }

    /**
     * Handles switching the endpoint's state from REMOVING_TOKEN to REMOVED_TOKEN
     * This should only be called after advertiseRemoving
     *
     * @param endpoint
     */
    public void advertiseTokenRemoved(Endpoint endpoint)
    {
        endpoint.state.updateTimestamp(); // make sure we don't evict it too soon
        endpoint.state.getHeartBeatState().forceNewerGenerationUnsafe();
        long expireTime = computeExpireTime();
        endpoint.state.addApplicationState(ApplicationState.STATUS_WITH_PORT, StorageService.instance.valueFactory.removedNonlocal(endpoint.getHostId(), expireTime));
        endpoint.state.addApplicationState(ApplicationState.STATUS, StorageService.instance.valueFactory.removedNonlocal(endpoint.getHostId(), expireTime));
        logger.info("Completing removal of {}", endpoint.toString());
        addExpireTimeForEndpoint(endpoint, expireTime);
        // ensure at least one gossip round occurs before returning
        Uninterruptibles.sleepUninterruptibly(intervalInMillis * 2, TimeUnit.MILLISECONDS);
    }

    // NOT USED
    @Deprecated
    public void unsafeAssassinateEndpoint(String address) throws UnknownHostException
    {
        logger.warn("Gossiper.unsafeAssassinateEndpoint is deprecated and will be removed in the next release; use assassinateEndpoint instead");
        assassinateEndpoint(address);
    }

    /**
     * Do not call this method unless you know what you are doing.
     * It will try extremely hard to obliterate any endpoint from the ring,
     * even if it does not know about it.
     *
     * @param address
     * @throws UnknownHostException
     */
    public void assassinateEndpoint(String address) throws UnknownHostException
    {
        InetAddressAndPort endpointAddress = InetAddressAndPort.getByName(address);
        Endpoint endpoint = StorageService.instance.getTokenMetadata().getEndpointForAddress(endpointAddress, false);

        // if no endpoints with this address, nothing to do
        if (endpoint == null)
            return;

        Collection<Token> tokens = null;
        logger.warn("Assassinating {} via gossip", endpoint.toString());

        if (endpoint.state == null)
        {
            endpoint.state = new EndpointState(new HeartBeatState((int) ((System.currentTimeMillis() + 60000) / 1000), 9999));
        }
        else
        {
            int generation = endpoint.state.getHeartBeatState().getGeneration();
            int heartbeat = endpoint.state.getHeartBeatState().getHeartBeatVersion();
            logger.info("Sleeping for {}ms to ensure {} does not change", StorageService.RING_DELAY, endpoint.toString());
            Uninterruptibles.sleepUninterruptibly(StorageService.RING_DELAY, TimeUnit.MILLISECONDS);
            // make sure it did not change
            Optional<EndpointState> newState = gossipEndpoints.stream().filter(e -> e.equals(endpoint)).map(e -> e.state).findAny();

            if (!newState.isPresent())
                logger.warn("Endpoint {} disappeared while trying to assassinate, continuing anyway", endpoint.toString());
            else if (newState.get().getHeartBeatState().getGeneration() != generation)
                throw new RuntimeException("Endpoint still alive: " + endpoint.toString() + " generation changed while trying to assassinate it");
            else if (newState.get().getHeartBeatState().getHeartBeatVersion() != heartbeat)
                throw new RuntimeException("Endpoint still alive: " + endpoint.toString() + " heartbeat changed while trying to assassinate it");
            endpoint.state.updateTimestamp(); // make sure we don't evict it too soon
            endpoint.state.getHeartBeatState().forceNewerGenerationUnsafe();
        }

        try
        {
            tokens = StorageService.instance.getTokenMetadata().getTokens(endpoint);
        }
        catch (Throwable th)
        {
            JVMStabilityInspector.inspectThrowable(th);
            // TODO this is broken
            logger.warn("Unable to calculate tokens for {}.  Will use a random one", address);
            tokens = Collections.singletonList(StorageService.instance.getTokenMetadata().partitioner.getRandomToken());
        }

        // do not pass go, do not collect 200 dollars, just gtfo
        long expireTime = computeExpireTime();
        endpoint.state.addApplicationState(ApplicationState.STATUS_WITH_PORT, StorageService.instance.valueFactory.left(tokens, expireTime));
        endpoint.state.addApplicationState(ApplicationState.STATUS, StorageService.instance.valueFactory.left(tokens, expireTime));
        handleMajorStateChange(endpoint, endpoint.state);
        Uninterruptibles.sleepUninterruptibly(intervalInMillis * 4, TimeUnit.MILLISECONDS);
        logger.warn("Finished assassinating {}", endpoint.toString());
    }

    // TODO: NOT USED
    public boolean isKnownEndpoint(Endpoint endpoint)
    {
        return gossipEndpoints.contains(endpoint);
    }

    public int getCurrentGenerationNumber(Endpoint endpoint)
    {
        return endpoint.state.getHeartBeatState().getGeneration();
    }

    /**
     * Returns true if the chosen target was also a seed. False otherwise
     *
     * @param message
     * @param epSet   a set of endpoint from which a random endpoint is chosen.
     * @return true if the chosen endpoint is also a seed.
     */
    private boolean sendGossip(MessageOut<GossipDigestSyn> message, Set<Endpoint> epSet)
    {
        List<Endpoint> liveEndpoints = ImmutableList.copyOf(epSet);

        int size = liveEndpoints.size();
        if (size < 1)
            return false;
        /* Generate a random number from 0 -> size */
        int index = (size == 1) ? 0 : random.nextInt(size);
        Endpoint to = liveEndpoints.get(index);
        if (logger.isTraceEnabled())
            logger.trace("Sending a GossipDigestSyn to {} ...", to.toString());
        if (firstSynSendAt == 0)
            firstSynSendAt = System.nanoTime();
        MessagingService.instance().sendOneWay(message, to.getPreferredAddress());
        return seeds.contains(to);
    }

    /* Sends a Gossip message to a live member and returns true if the recipient was a seed */
    private boolean doGossipToLiveMember(MessageOut<GossipDigestSyn> message)
    {
        int size = liveEndpoints.size();
        if (size == 0)
            return false;
        return sendGossip(message, liveEndpoints);
    }

    /* Sends a Gossip message to an unreachable member */
    private void maybeGossipToUnreachableMember(MessageOut<GossipDigestSyn> message)
    {
        double liveEndpointCount = liveEndpoints.size();
        double unreachableEndpointCount = unreachableEndpoints.size();
        if (unreachableEndpointCount > 0)
        {
            /* based on some probability */
            double prob = unreachableEndpointCount / (liveEndpointCount + 1);
            double randDbl = random.nextDouble();
            if (randDbl < prob)
                sendGossip(message, unreachableEndpoints.keySet());
        }
    }

    /* Possibly gossip to a seed for facilitating partition healing */
    private void maybeGossipToSeed(MessageOut<GossipDigestSyn> prod)
    {
        int size = seeds.size();
        if (size > 0)
        {
            if (size == 1 && seeds.contains(Endpoint.getLocalEndpoint()))
                return;

            if (!liveEndpoints.isEmpty())
            {
                sendGossip(prod, seeds);
            }
            else
            {
                /* Gossip with the seed with some probability. */
                double probability = seeds.size() / (double) (liveEndpoints.size() + unreachableEndpoints.size());
                double randDbl = random.nextDouble();
                if (randDbl <= probability)
                    sendGossip(prod, seeds);
            }
        }
    }

    public boolean isGossipOnlyMember(Endpoint endpoint)
    {
        if (endpoint.state == null)
            return false;

        return !endpoint.inDeadState() && !StorageService.instance.getTokenMetadata().isMember(endpoint);
    }

    /**
     * Check if this node can safely be started and join the ring.
     * If the node is bootstrapping, examines gossip state for any previous status to decide whether
     * it's safe to allow this node to start and bootstrap. If not bootstrapping, compares the host ID
     * that the node itself has (obtained by reading from system.local or generated if not present)
     * with the host ID obtained from gossip for the endpoint address (if any). This latter case
     * prevents a non-bootstrapping, new node from being started with the same address of a
     * previously started, but currently down predecessor.
     *
     * @param endpoint - the endpoint to check
     * @param localHostUUID - the host id to check
     * @param isBootstrapping - whether the node intends to bootstrap when joining
     * @param epStates - endpoint states in the cluster
     * @return true if it is safe to start the node, false otherwise
     */
    //TODO: replace endpoint + uuid with just Endpoint
    public boolean isSafeForStartup(Endpoint endpoint, UUID localHostUUID, boolean isBootstrapping,
                                    Map<Endpoint, EndpointState> epStates)
    {
        // if there's no previous state, or the node was previously removed from the cluster, we're good
        if (endpoint.state == null || endpoint.inDeadState())
            return true;

        if (isBootstrapping)
        {
            String status = endpoint.getGossipStatus();
            // these states are not allowed to join the cluster as it would not be safe
            final List<String> unsafeStatuses = new ArrayList<String>();
            unsafeStatuses.add("");                           // failed bootstrap but we did start gossiping
            unsafeStatuses.add(VersionedValue.STATUS_NORMAL); // node is legit in the cluster or it was stopped with kill -9
            unsafeStatuses.add(VersionedValue.SHUTDOWN);      // node was shutdown
            return !unsafeStatuses.contains(status);
        }
        else
        {
            // if the previous UUID matches what we currently have (i.e. what was read from
            // system.local at startup), then we're good to start up. Otherwise, something
            // is amiss and we need to replace the previous node
            return endpoint.getHostId().equals(localHostUUID);
        }
    }

    private void doStatusCheck()
    {
        if (logger.isTraceEnabled())
            logger.trace("Performing status check ...");

        long now = System.currentTimeMillis();
        long nowNano = System.nanoTime();

        long pending = ((JMXEnabledThreadPoolExecutor) StageManager.getStage(Stage.GOSSIP)).metrics.pendingTasks.getValue();
        if (pending > 0 && lastProcessedMessageAt < now - 1000)
        {
            // if some new messages just arrived, give the executor some time to work on them
            Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);

            // still behind?  something's broke
            if (lastProcessedMessageAt < now - 1000)
            {
                logger.warn("Gossip stage has {} pending tasks; skipping status check (no nodes will be marked down)", pending);
                return;
            }
        }

        for (Endpoint endpoint : gossipEndpoints)
        {
            if (endpoint.equals(Endpoint.getLocalEndpoint()))
                continue;

            FailureDetector.instance.interpret(endpoint);
            if (endpoint.state != null)
            {
                // check if this is a fat client. fat clients are removed automatically from
                // gossip after FatClientTimeout.  Do not remove dead states here.
                if (isGossipOnlyMember(endpoint)
                    && !justRemovedEndpoints.containsKey(endpoint)
                    && TimeUnit.NANOSECONDS.toMillis(nowNano - endpoint.state.getUpdateTimestamp()) > fatClientTimeout)
                {
                    logger.info("FatClient {} has been silent for {}ms, removing from gossip", endpoint, fatClientTimeout);
                    removeEndpoint(endpoint); // will put it in justRemovedEndpoints to respect quarantine delay
                    evictFromMembership(endpoint); // can get rid of the state immediately
                }

                // check for dead state removal
                long expireTime = getExpireTimeForEndpoint(endpoint);
                if (!endpoint.isAlive() && (now > expireTime)
                    && (!StorageService.instance.getTokenMetadata().isMember(endpoint)))
                {
                    if (logger.isDebugEnabled())
                    {
                        logger.debug("time is expiring for endpoint : {} ({})", endpoint, expireTime);
                    }
                    evictFromMembership(endpoint);
                }
            }
        }

        if (!justRemovedEndpoints.isEmpty())
        {
            for (Entry<Endpoint, Long> entry : justRemovedEndpoints.entrySet())
            {
                if ((now - entry.getValue()) > QUARANTINE_DELAY)
                {
                    if (logger.isDebugEnabled())
                        logger.debug("{} elapsed, {} gossip quarantine over", QUARANTINE_DELAY, entry.getKey());
                    justRemovedEndpoints.remove(entry.getKey());
                }
            }
        }
    }

    protected long getExpireTimeForEndpoint(Endpoint endpoint)
    {
        /* default expireTime is aVeryLongTime */
        Long storedTime = expireTimeEndpointMap.get(endpoint);
        return storedTime == null ? computeExpireTime() : storedTime;
    }

    public EndpointState getEndpointStateForEndpoint(Endpoint ep)
    {
        return gossipEndpoints.get(ep);
    }

    public ImmutableSet<Endpoint> getEndpoints()
    {
        return ImmutableSet.copyOf(gossipEndpoints);
    }

    public int getEndpointCount()
    {
        return gossipEndpoints.size();
    }

    @Deprecated
    public UUID getHostId(Endpoint endpoint)
    {
        return getHostId(endpoint, gossipEndpoints);
    }

    @Deprecated
    public UUID getHostId(Endpoint endpoint, Map<Endpoint, EndpointState> epStates)
    {
        //TODO: srsly?
        return UUID.fromString(epStates.get(endpoint).getApplicationState(ApplicationState.HOST_ID).value);
    }

    EndpointState getStateForVersionBiggerThan(Endpoint endpoint, int version)
    {
        EndpointState reqdEndpointState = null;

        if (endpoint.state != null)
        {
            /*
             * Here we try to include the Heart Beat state only if it is
             * greater than the version passed in. It might happen that
             * the heart beat version maybe lesser than the version passed
             * in and some application state has a version that is greater
             * than the version passed in. In this case we also send the old
             * heart beat and throw it away on the receiver if it is redundant.
            */
            HeartBeatState heartBeatState = endpoint.state.getHeartBeatState();
            int localHbGeneration = heartBeatState.getGeneration();
            int localHbVersion = heartBeatState.getHeartBeatVersion();
            if (localHbVersion > version)
            {
                reqdEndpointState = new EndpointState(new HeartBeatState(localHbGeneration, localHbVersion));
                if (logger.isTraceEnabled())
                    logger.trace("local heartbeat version {} greater than {} for {}", localHbVersion, version, endpoint.toString());
            }
            /* Accumulate all application states whose versions are greater than "version" variable */
            Map<ApplicationState, VersionedValue> states = new EnumMap<>(ApplicationState.class);
            for (Entry<ApplicationState, VersionedValue> entry : endpoint.state.states())
            {
                VersionedValue value = entry.getValue();
                if (value.version > version)
                {
                    if (reqdEndpointState == null)
                    {
                        reqdEndpointState = new EndpointState(new HeartBeatState(localHbGeneration, localHbVersion));
                    }
                    final ApplicationState key = entry.getKey();
                    if (logger.isTraceEnabled())
                        logger.trace("Adding state {}: {}" , key, value.value);

                    states.put(key, value);
                }
            }
            if (reqdEndpointState != null)
                reqdEndpointState.addApplicationStates(states);
        }
        return reqdEndpointState;
    }

    /**
     * determine which endpoint started up earlier
     */
    public int compareEndpointStartup(Endpoint addr1, Endpoint addr2)
    {
        EndpointState ep1 = getEndpointStateForEndpoint(addr1);
        EndpointState ep2 = getEndpointStateForEndpoint(addr2);
        assert ep1 != null && ep2 != null;
        return ep1.getHeartBeatState().getGeneration() - ep2.getHeartBeatState().getGeneration();
    }

    void notifyFailureDetector(Map<Endpoint, EndpointState> remoteEpStateMap)
    {
        for (Entry<Endpoint, EndpointState> entry : remoteEpStateMap.entrySet())
        {
            notifyFailureDetector(entry.getKey(), entry.getValue());
        }
    }

    void notifyFailureDetector(Endpoint endpoint, EndpointState remoteEndpointState)
    {
        EndpointState localEndpointState = gossipEndpoints.get(endpoint);
        /*
         * If the local endpoint state exists then report to the FD only
         * if the versions workout.
        */
        if (localEndpointState != null)
        {
            IFailureDetector fd = FailureDetector.instance;
            int localGeneration = localEndpointState.getHeartBeatState().getGeneration();
            int remoteGeneration = remoteEndpointState.getHeartBeatState().getGeneration();
            if (remoteGeneration > localGeneration)
            {
                localEndpointState.updateTimestamp();
                // this node was dead and the generation changed, this indicates a reboot, or possibly a takeover
                // we will clean the fd intervals for it and relearn them
                if (!localEndpointState.isAlive())
                {
                    logger.debug("Clearing interval times for {} due to generation change", endpoint.toString());
                    fd.remove(endpoint);
                }
                fd.report(endpoint);
                return;
            }

            if (remoteGeneration == localGeneration)
            {
                int localVersion = localEndpointState.getMaxEndpointStateVersion();
                int remoteVersion = remoteEndpointState.getHeartBeatState().getHeartBeatVersion();
                if (remoteVersion > localVersion)
                {
                    localEndpointState.updateTimestamp();
                    // just a version change, report to the fd
                    fd.report(endpoint);
                }
            }
        }

    }

    private void markAlive(final Endpoint endpoint, final EndpointState localState)
    {
        localState.markDead();

        MessageOut<EchoMessage> echoMessage = new MessageOut<EchoMessage>(MessagingService.Verb.ECHO, EchoMessage.instance, EchoMessage.serializer);
        logger.trace("Sending a EchoMessage to {}", endpoint.toString());
        IAsyncCallback echoHandler = new IAsyncCallback()
        {
            public boolean isLatencyForSnitch()
            {
                return false;
            }

            public void response(MessageIn msg)
            {
                // force processing of the echo response onto the gossip stage, as it comes in on the REQUEST_RESPONSE stage
                StageManager.getStage(Stage.GOSSIP).submit(() -> realMarkAlive(endpoint, localState));
            }
        };

        MessagingService.instance().sendRR(echoMessage, endpoint.getPreferredAddress(), echoHandler);
    }

    @VisibleForTesting
    public void realMarkAlive(final Endpoint addr, final EndpointState localState)
    {
        if (logger.isTraceEnabled())
            logger.trace("marking as alive {}", addr);
        localState.markAlive();
        localState.updateTimestamp(); // prevents doStatusCheck from racing us and evicting if it was down > aVeryLongTime
        liveEndpoints.add(addr);
        unreachableEndpoints.remove(addr);
        expireTimeEndpointMap.remove(addr);
        logger.debug("removing expire time for endpoint : {}", addr);
        logger.info("InetAddress {} is now UP", addr);
        for (IEndpointStateChangeSubscriber subscriber : subscribers)
            subscriber.onAlive(addr, localState);
        if (logger.isTraceEnabled())
            logger.trace("Notified {}", subscribers);
    }

    @VisibleForTesting
    public void markDead(Endpoint ep)
    {
        if (logger.isTraceEnabled())
            logger.trace("marking as down {}", ep.toString());
        ep.state.markDead();
        liveEndpoints.remove(ep);
        unreachableEndpoints.put(ep, System.nanoTime());
        logger.info("Node {} is now DOWN", ep.toString());
        for (IEndpointStateChangeSubscriber subscriber : subscribers)
            subscriber.onDead(ep);
        if (logger.isTraceEnabled())
            logger.trace("Notified {}", subscribers);
    }

    /**
     * This method is called whenever there is a "big" change in ep state (a generation change for a known node).
     *
     * @param ep      endpoint
     * @param epState EndpointState for the endpoint
     */
    // TODO: combine Endpoint and its state
    private void handleMajorStateChange(Endpoint ep, EndpointState epState)
    {
        EndpointState localEpState = gossipEndpoints.get(ep);
        if (!epState.isDeadState(epState))
        {
            if (localEpState != null)
                logger.info("Node {} has restarted, now UP", ep);
            else
                logger.info("Node {} is now part of the cluster", ep);
        }
        if (logger.isTraceEnabled())
            logger.trace("Adding endpoint state for {}", ep);
        gossipEndpoints.put(ep, epState);

        if (localEpState != null)
        {   // the node restarted: it is up to the subscriber to take whatever action is necessary
            for (IEndpointStateChangeSubscriber subscriber : subscribers)
                subscriber.onRestart(ep, localEpState);
        }

        if (!epState.isDeadState(epState))
            markAlive(ep, epState);
        else
        {
            logger.debug("Not marking {} alive due to dead state", ep);
            markDead(ep, epState);
        }
        for (IEndpointStateChangeSubscriber subscriber : subscribers)
            subscriber.onJoin(ep, epState);
        // check this at the end so nodes will learn about the endpoint
        if (ep.isShutdown())
            markAsShutdown(ep);
    }

    public boolean isAlive(Endpoint endpoint)
    {
        EndpointState epState = getEndpointStateForEndpoint(endpoint);
        return epState != null && epState.isAlive() && !epState.isDeadState(epState);
    }

    void applyStateLocally(Map<Endpoint, EndpointState> epStateMap)
    {
        Endpoint localEndpoint = StorageService.instance.getTokenMetadata().getEndpointForAddress(FBUtilities.getBroadcastAddressAndPort(), false);
        for (Entry<Endpoint, EndpointState> entry : epStateMap.entrySet())
        {
            Endpoint ep = entry.getKey();
            if ( ep.equals(localEndpoint) && !isInShadowRound())
                continue;
            if (justRemovedEndpoints.containsKey(ep))
            {
                if (logger.isTraceEnabled())
                    logger.trace("Ignoring gossip for {} because it is quarantined", ep);
                continue;
            }

            EndpointState localEpStatePtr = gossipEndpoints.get(ep);
            EndpointState remoteState = entry.getValue();

            /*
                If state does not exist just add it. If it does then add it if the remote generation is greater.
                If there is a generation tie, attempt to break it by heartbeat version.
            */
            if (localEpStatePtr != null)
            {
                int localGeneration = localEpStatePtr.getHeartBeatState().getGeneration();
                int remoteGeneration = remoteState.getHeartBeatState().getGeneration();
                long localTime = System.currentTimeMillis()/1000;
                if (logger.isTraceEnabled())
                    logger.trace("{} local generation {}, remote generation {}", ep, localGeneration, remoteGeneration);

                // We measure generation drift against local time, based on the fact that generation is initialized by time
                if (remoteGeneration > localTime + MAX_GENERATION_DIFFERENCE)
                {
                    // assume some peer has corrupted memory and is broadcasting an unbelievable generation about another peer (or itself)
                    logger.warn("received an invalid gossip generation for peer {}; local time = {}, received generation = {}", ep, localTime, remoteGeneration);
                }
                else if (remoteGeneration > localGeneration)
                {
                    if (logger.isTraceEnabled())
                        logger.trace("Updating heartbeat state generation to {} from {} for {}", remoteGeneration, localGeneration, ep);
                    // major state change will handle the update by inserting the remote state directly
                    handleMajorStateChange(ep, remoteState);
                }
                else if (remoteGeneration == localGeneration) // generation has not changed, apply new states
                {
                    /* find maximum state */
                    int localMaxVersion = localEpStatePtr.getMaxEndpointStateVersion();
                    int remoteMaxVersion = remoteState.getMaxEndpointStateVersion();
                    if (remoteMaxVersion > localMaxVersion)
                    {
                        // apply states, but do not notify since there is no major change
                        applyNewStates(ep, localEpStatePtr, remoteState);
                    }
                    else if (logger.isTraceEnabled())
                            logger.trace("Ignoring remote version {} <= {} for {}", remoteMaxVersion, localMaxVersion, ep);

                    if (!localEpStatePtr.isAlive() && !localEpStatePtr.isDeadState(localEpStatePtr)) // unless of course, it was dead
                        markAlive(ep, localEpStatePtr);
                }
                else
                {
                    if (logger.isTraceEnabled())
                        logger.trace("Ignoring remote generation {} < {}", remoteGeneration, localGeneration);
                }
            }
            else
            {
                // this is a new node, report it to the FD in case it is the first time we are seeing it AND it's not alive
                FailureDetector.instance.report(ep);
                handleMajorStateChange(ep, remoteState);
            }
        }
    }

    private void applyNewStates(Endpoint endpoint, EndpointState localState, EndpointState remoteState)
    {
        // don't assert here, since if the node restarts the version will go back to zero
        int oldVersion = localState.getHeartBeatState().getHeartBeatVersion();

        localState.setHeartBeatState(remoteState.getHeartBeatState());
        if (logger.isTraceEnabled())
            logger.trace("Updating heartbeat state version to {} from {} for {} ...", localState.getHeartBeatState().getHeartBeatVersion(), oldVersion, endpoint.toString());

        Set<Entry<ApplicationState, VersionedValue>> remoteStates = remoteState.states();
        assert remoteState.getHeartBeatState().getGeneration() == localState.getHeartBeatState().getGeneration();
        localState.addApplicationStates(remoteStates);

        //Filter out pre-4.0 versions of data for more complete 4.0 versions
        Set<Entry<ApplicationState, VersionedValue>> filtered = remoteStates.stream().filter(entry -> {
           switch (entry.getKey())
           {
               case INTERNAL_IP:
                    return remoteState.getApplicationState(ApplicationState.INTERNAL_ADDRESS_AND_PORT) == null;
               case STATUS:
                   return remoteState.getApplicationState(ApplicationState.STATUS_WITH_PORT) == null;
               case RPC_ADDRESS:
                   return remoteState.getApplicationState(ApplicationState.NATIVE_ADDRESS_AND_PORT) == null;
               default:
                   return true;
           }
        }).collect(Collectors.toSet());

        for (Entry<ApplicationState, VersionedValue> remoteEntry : filtered)
            doOnChangeNotifications(endpoint, remoteEntry.getKey(), remoteEntry.getValue());
    }

    // notify that a local application state is going to change (doesn't get triggered for remote changes)
    private void doBeforeChangeNotifications(Endpoint addr, EndpointState epState, ApplicationState apState, VersionedValue newValue)
    {
        for (IEndpointStateChangeSubscriber subscriber : subscribers)
        {
            subscriber.beforeChange(addr, epState, apState, newValue);
        }
    }

    // notify that an application state has changed
    private void doOnChangeNotifications(Endpoint endpoint, ApplicationState state, VersionedValue value)
    {
        for (IEndpointStateChangeSubscriber subscriber : subscribers)
        {
            subscriber.onChange(endpoint, state, value);
        }
    }

    /* Request all the state for the endpoint in the gDigest */
    private void requestAll(GossipDigest gDigest, List<GossipDigest> deltaGossipDigestList, int remoteGeneration)
    {
        /* We are here since we have no data for this endpoint locally so request everthing. */
        deltaGossipDigestList.add(new GossipDigest(gDigest.getEndpoint(), remoteGeneration, 0));
        if (logger.isTraceEnabled())
            logger.trace("requestAll for {}", gDigest.getEndpoint());
    }

    /* Send all the data with version greater than maxRemoteVersion */
    private void sendAll(GossipDigest gDigest, Map<Endpoint, EndpointState> deltaEpStateMap, int maxRemoteVersion)
    {
        EndpointState localEpStatePtr = getStateForVersionBiggerThan(gDigest.getEndpoint(), maxRemoteVersion);
        if (localEpStatePtr != null)
            deltaEpStateMap.put(gDigest.getEndpoint(), localEpStatePtr);
    }

    /*
        This method is used to figure the state that the Gossiper has but Gossipee doesn't. The delta digests
        and the delta state are built up.
    */
    void examineGossiper(List<GossipDigest> gDigestList, List<GossipDigest> deltaGossipDigestList, Map<Endpoint, EndpointState> deltaEpStateMap)
    {
        if (gDigestList.size() == 0)
        {
           /* we've been sent a *completely* empty syn, which should normally never happen since an endpoint will at least send a syn with itself.
              If this is happening then the node is attempting shadow gossip, and we should reply with everything we know.
            */
            logger.debug("Shadow request received, adding all states");
            for (Map.Entry<Endpoint, EndpointState> entry : gossipEndpoints.entrySet())
            {
                gDigestList.add(new GossipDigest(entry.getKey(), 0, 0));
            }
        }
        for ( GossipDigest gDigest : gDigestList )
        {
            int remoteGeneration = gDigest.getGeneration();
            int maxRemoteVersion = gDigest.getMaxVersion();
            /* Get state associated with the end point in digest */
            EndpointState epStatePtr = gossipEndpoints.get(gDigest.getEndpoint());
            /*
                Here we need to fire a GossipDigestAckMessage. If we have some data associated with this endpoint locally
                then we follow the "if" path of the logic. If we have absolutely nothing for this endpoint we need to
                request all the data for this endpoint.
            */
            if (epStatePtr != null)
            {
                int localGeneration = epStatePtr.getHeartBeatState().getGeneration();
                /* get the max version of all keys in the state associated with this endpoint */
                int maxLocalVersion = epStatePtr.getMaxEndpointStateVersion();
                if (remoteGeneration == localGeneration && maxRemoteVersion == maxLocalVersion)
                    continue;

                if (remoteGeneration > localGeneration)
                {
                    /* we request everything from the gossiper */
                    requestAll(gDigest, deltaGossipDigestList, remoteGeneration);
                }
                else if (remoteGeneration < localGeneration)
                {
                    /* send all data with generation = localgeneration and version > 0 */
                    sendAll(gDigest, deltaEpStateMap, 0);
                }
                else if (remoteGeneration == localGeneration)
                {
                    /*
                        If the max remote version is greater then we request the remote endpoint send us all the data
                        for this endpoint with version greater than the max version number we have locally for this
                        endpoint.
                        If the max remote version is lesser, then we send all the data we have locally for this endpoint
                        with version greater than the max remote version.
                    */
                    if (maxRemoteVersion > maxLocalVersion)
                    {
                        deltaGossipDigestList.add(new GossipDigest(gDigest.getEndpoint(), remoteGeneration, maxLocalVersion));
                    }
                    else if (maxRemoteVersion < maxLocalVersion)
                    {
                        /* send all data with generation = localgeneration and version > maxRemoteVersion */
                        sendAll(gDigest, deltaEpStateMap, maxRemoteVersion);
                    }
                }
            }
            else
            {
                /* We are here since we have no data for this endpoint locally so request everything. */
                requestAll(gDigest, deltaGossipDigestList, remoteGeneration);
            }
        }
    }

    public void start(int generationNumber)
    {
        start(generationNumber, new EnumMap<ApplicationState, VersionedValue>(ApplicationState.class));
    }

    /**
     * Start the gossiper with the generation number, preloading the map of application states before starting
     */
    public void start(int generationNbr, Map<ApplicationState, VersionedValue> preloadLocalStates)
    {
        buildSeedsList();
        /* initialize the heartbeat state for this localEndpoint */
        maybeInitializeLocalState(generationNbr);

        Endpoint localEndpoint = Endpoint.getLocalEndpoint();
        EndpointState localState = gossipEndpoints.get(localEndpoint);

        // if for some reason there was no local endpoint in the list of states, error out
        if (localState == null)
            throw new IllegalStateException("Error: Can't find the state for the local node!");

        localState.addApplicationStates(preloadLocalStates);

        //notify snitches that Gossiper is about to start
        DatabaseDescriptor.getEndpointSnitch().gossiperStarting();
        if (logger.isTraceEnabled())
            logger.trace("gossip started with generation {}", localState.getHeartBeatState().getGeneration());

        scheduledGossipTask = executor.scheduleWithFixedDelay(new GossipTask(),
                                                              Gossiper.intervalInMillis,
                                                              Gossiper.intervalInMillis,
                                                              TimeUnit.MILLISECONDS);
    }

    public synchronized Map<Endpoint, EndpointState> doShadowRound()
    {
        return doShadowRound(Collections.EMPTY_SET);
    }

    /**
     * Do a single 'shadow' round of gossip by retrieving endpoint states that will be stored exclusively in the
     * map return value, instead of gossipEndpoints.
     *
     * Used when preparing to join the ring:
     * <ul>
     *     <li>when replacing a node, to get and assume its tokens</li>
     *     <li>when joining, to check that the local host id matches any previous id for the endpoint address</li>
     * </ul>
     *
     * Method is synchronized, as we use an in-progress flag to indicate that shadow round must be cleared
     * again by calling {@link Gossiper#maybeFinishShadowRound(Endpoint, boolean, Map)}. This will update
     * {@link Gossiper#shadowRoundEndpoints} with received values, in order to return an immutable copy to the
     * caller of {@link Gossiper#doShadowRound()}. Therefor only a single shadow round execution is permitted at
     * the same time.
     *
     * @param peers Additional peers to try gossiping with.
     * @return endpoint states gathered during shadow round or empty map
     */
    public synchronized Map<Endpoint, EndpointState> doShadowRound(Set<Endpoint> peers)
    {
        buildSeedsList();
        // it may be that the local address is the only entry in the seed + peers
        // list in which case, attempting a shadow round is pointless
        if (seeds.isEmpty() && peers.isEmpty())
            return shadowRoundEndpoints;

        boolean isSeed = DatabaseDescriptor.getSeeds().contains(FBUtilities.getBroadcastAddressAndPort());
        // We double RING_DELAY if we're not a seed to increase chance of successful startup during a full cluster bounce,
        // giving the seeds a chance to startup before we fail the shadow round
        int shadowRoundDelay =  isSeed ? StorageService.RING_DELAY : StorageService.RING_DELAY * 2;
        seedsInShadowRound.clear();
        shadowRoundEndpoints.clear();
        // send a completely empty syn
        List<GossipDigest> gDigests = new ArrayList<GossipDigest>();
        GossipDigestSyn digestSynMessage = new GossipDigestSyn(DatabaseDescriptor.getClusterName(),
                DatabaseDescriptor.getPartitionerName(),
                gDigests);
        MessageOut<GossipDigestSyn> message = new MessageOut<GossipDigestSyn>(MessagingService.Verb.GOSSIP_DIGEST_SYN,
                digestSynMessage,
                GossipDigestSyn.serializer);

        inShadowRound = true;
        boolean includePeers = false;
        int slept = 0;
        try
        {
            while (true)
            {
                if (slept % 5000 == 0)
                { // CASSANDRA-8072, retry at the beginning and every 5 seconds
                    logger.trace("Sending shadow round GOSSIP DIGEST SYN to seeds {}", seeds);

                    for (InetAddressAndPort seedAddress : seeds.stream().map(Endpoint::getPreferredAddress).collect(Collectors.toSet()))
                        MessagingService.instance().sendOneWay(message, seedAddress);

                    // Send to any peers we already know about, but only if a seed didn't respond.
                    if (includePeers)
                    {
                        logger.trace("Sending shadow round GOSSIP DIGEST SYN to known peers {}", peers);
                        for (InetAddressAndPort peerAddress : peers.stream().map(Endpoint::getPreferredAddress).collect(Collectors.toSet()))
                            MessagingService.instance().sendOneWay(message, peerAddress);
                    }
                    includePeers = true;
                }

                Thread.sleep(1000);
                if (!inShadowRound)
                    break;

                slept += 1000;
                if (slept > shadowRoundDelay)
                {
                    // if we got here no peers could be gossiped to. If we're a seed that's OK, but otherwise we stop. See CASSANDRA-13851
                    if (!isSeed)
                        throw new RuntimeException("Unable to gossip with any peers");

                    inShadowRound = false;
                    break;
                }
            }
        }
        catch (InterruptedException wtf)
        {
            throw new RuntimeException(wtf);
        }

        return ImmutableMap.copyOf(shadowRoundEndpoints);
    }

    @VisibleForTesting
    void buildSeedsList()
    {
        // TODO: should we do this more often?
        refreshSeedEndpoints();

        for (InetAddressAndPort seedAddress : DatabaseDescriptor.getSeeds())
        {
            if (seedAddress.equals(FBUtilities.getBroadcastAddressAndPort()) || seeds.stream().anyMatch(e -> e.hasAddress(seedAddress)))
                continue;
            seeds.add(StorageService.instance.getTokenMetadata().getEndpointForAddress(seedAddress));
        }
    }

    /**
     *     Go over the seeds and update their endpoint objects with what's known in TokenMetadata
      */
    private void refreshSeedEndpoints()
    {
        for (Endpoint seed : seeds)
        {
           final Endpoint tmEndpoint = StorageService.instance.getTokenMetadata().getEndpointForAddress(seed.getPreferredAddress(), false);
           if (tmEndpoint != null)
               seed.updateValuesFrom(tmEndpoint);
        }
    }


    /**
     * JMX interface for triggering an update of the seed node list.
     */
    public List<String> reloadSeeds()
    {
        logger.trace("Triggering reload of seed node list");

        // Get the new set in the same that buildSeedsList does
        Set<Endpoint> tmp = new HashSet<>();
        try
        {
            for (InetAddressAndPort seedAddress : DatabaseDescriptor.getSeeds())
            {
                if (seedAddress.equals(FBUtilities.getBroadcastAddressAndPort()) || seeds.stream().anyMatch(e -> e.hasAddress(seedAddress)))
                    continue;
                tmp.add(StorageService.instance.getTokenMetadata().getEndpointForAddress(seedAddress));
            }
        }
        // If using the SimpleSeedProvider invalid yaml added to the config since startup could
        // cause this to throw. Additionally, third party seed providers may throw exceptions.
        // Handle the error and return a null to indicate that there was a problem.
        catch (Throwable e)
        {
            JVMStabilityInspector.inspectThrowable(e);
            logger.warn("Error while getting seed node list: {}", e.getLocalizedMessage());
            return null;
        }

        if (tmp.size() == 0)
        {
            logger.trace("New seed node list is empty. Not updating seed list.");
            return getSeeds();
        }

        if (tmp.equals(seeds))
        {
            logger.trace("New seed node list matches the existing list.");
            return getSeeds();
        }

        // Add the new entries
        seeds.addAll(tmp);
        // Remove the old entries
        seeds.retainAll(tmp);
        logger.trace("New seed node list after reload {}", seeds);
        return getSeeds();
    }

    /**
     * JMX endpoint for getting the list of seeds from the node
     */
    public List<String> getSeeds()
    {
        return seeds.stream().map(e -> e.getPreferredAddress().toString(true)).collect(Collectors.toList());
    }

    // initialize local HB state if needed, i.e., if gossiper has never been started before.
    public void maybeInitializeLocalState(int generationNbr)
    {
        Endpoint localEndpoint = Endpoint.getLocalEndpoint();
        HeartBeatState hbState = new HeartBeatState(generationNbr);
        EndpointState localState = new EndpointState(hbState);
        localState.markAlive();
        gossipEndpoints.putIfAbsent(localEndpoint, localState);
    }

    public void forceNewerGeneration()
    {
        EndpointState epstate = gossipEndpoints.get(FBUtilities.getBroadcastAddressAndPort());
        epstate.getHeartBeatState().forceNewerGenerationUnsafe();
    }


    /**
     * Add an endpoint we knew about previously, but whose state is unknown
     */
    public void addSavedEndpoint(Endpoint ep)
    {
        if (ep.equals(Endpoint.getLocalEndpoint()))
        {
            logger.debug("Attempt to add self as saved endpoint");
            return;
        }

        //preserve any previously known, in-memory data about the endpoint (such as DC, RACK, and so on)
        EndpointState epState = gossipEndpoints.get(ep);
        if (epState != null)
        {
            logger.debug("not replacing a previous epState for {}, but reusing it: {}", ep, epState);
            epState.setHeartBeatState(new HeartBeatState(0));
        }
        else
        {
            epState = new EndpointState(new HeartBeatState(0));
        }

        epState.markDead();
        gossipEndpoints.put(ep, epState);
        unreachableEndpoints.put(ep, System.nanoTime());
        if (logger.isTraceEnabled())
            logger.trace("Adding saved endpoint {} {}", ep, epState.getHeartBeatState().getGeneration());
    }

    private void addLocalApplicationStateInternal(ApplicationState state, VersionedValue value)
    {
        assert taskLock.isHeldByCurrentThread();
        Endpoint localEndpoint = Endpoint.getLocalEndpoint();
        EndpointState epState = gossipEndpoints.get(localEndpoint);
        assert epState != null;
        // Fire "before change" notifications:
        doBeforeChangeNotifications(localEndpoint, epState, state, value);
        // Notifications may have taken some time, so preventively raise the version
        // of the new value, otherwise it could be ignored by the remote node
        // if another value with a newer version was received in the meantime:
        value = StorageService.instance.valueFactory.cloneWithHigherVersion(value);
        // Add to local application state and fire "on change" notifications:
        epState.addApplicationState(state, value);
        doOnChangeNotifications(localEndpoint, state, value);
    }

    public void addLocalApplicationState(ApplicationState applicationState, VersionedValue value)
    {
        addLocalApplicationStates(Arrays.asList(Pair.create(applicationState, value)));
    }

    public void addLocalApplicationStates(List<Pair<ApplicationState, VersionedValue>> states)
    {
        taskLock.lock();
        try
        {
            for (Pair<ApplicationState, VersionedValue> pair : states)
            {
               addLocalApplicationStateInternal(pair.left, pair.right);
            }
        }
        finally
        {
            taskLock.unlock();
        }

    }

    public void stop()
    {
        EndpointState mystate = gossipEndpoints.get(FBUtilities.getBroadcastAddressAndPort());
        if (mystate != null && !mystate.isSilentShutdownState(mystate) && StorageService.instance.isJoined())
        {
            logger.info("Announcing shutdown");
            addLocalApplicationState(ApplicationState.STATUS_WITH_PORT, StorageService.instance.valueFactory.shutdown(true));
            addLocalApplicationState(ApplicationState.STATUS, StorageService.instance.valueFactory.shutdown(true));
            MessageOut message = new MessageOut(MessagingService.Verb.GOSSIP_SHUTDOWN);
            for (InetAddressAndPort ep : liveEndpoints.stream().map(Endpoint::getPreferredAddress).collect(Collectors.toSet()))
                MessagingService.instance().sendOneWay(message, ep);
            Uninterruptibles.sleepUninterruptibly(Integer.getInteger("cassandra.shutdown_announce_in_ms", 2000), TimeUnit.MILLISECONDS);
        }
        else
            logger.warn("No local state, state is in silent shutdown, or node hasn't joined, not announcing shutdown");
        if (scheduledGossipTask != null)
            scheduledGossipTask.cancel(false);
    }

    public boolean isEnabled()
    {
        return (scheduledGossipTask != null) && (!scheduledGossipTask.isCancelled());
    }

    protected void maybeFinishShadowRound(Endpoint respondent, boolean isInShadowRound, Map<Endpoint, EndpointState> epStateMap)
    {
        if (inShadowRound)
        {
            if (!isInShadowRound)
            {
                if (!seeds.contains(respondent))
                    logger.warn("Received an ack from {}, who isn't a seed. Ensure your seed list includes a live node. Exiting shadow round",
                                respondent);
                logger.debug("Received a regular ack from {}, can now exit shadow round", respondent);
                // respondent sent back a full ack, so we can exit our shadow round
                shadowRoundEndpoints.putAll(epStateMap);
                inShadowRound = false;
                seedsInShadowRound.clear();
            }
            else
            {
                // respondent indicates it too is in a shadow round, if all seeds
                // are in this state then we can exit our shadow round. Otherwise,
                // we keep retrying the SR until one responds with a full ACK or
                // we learn that all seeds are in SR.
                logger.debug("Received an ack from {} indicating it is also in shadow round", respondent);
                seedsInShadowRound.add(respondent);
                if (seedsInShadowRound.containsAll(seeds))
                {
                    logger.debug("All seeds are in a shadow round, clearing this node to exit its own");
                    inShadowRound = false;
                    seedsInShadowRound.clear();
                }
            }
        }
    }

    protected boolean isInShadowRound()
    {
        return inShadowRound;
    }

    @VisibleForTesting
    public void initializeNodeUnsafe(InetAddressAndPort addr, UUID uuid, int generationNbr)
    {
        HeartBeatState hbState = new HeartBeatState(generationNbr);
        EndpointState newState = new EndpointState(hbState);
        Endpoint endpoint = StorageService.instance.getTokenMetadata().getEndpointForAddress(addr, false);
        if (endpoint == null)
           endpoint = new Endpoint(addr, null, null, null, uuid);
        newState.markAlive();
        EndpointState oldState = gossipEndpoints.putIfAbsent(endpoint, newState);
        EndpointState localState = oldState == null ? newState : oldState;

        // always add the version state
        Map<ApplicationState, VersionedValue> states = new EnumMap<>(ApplicationState.class);
        states.put(ApplicationState.NET_VERSION, StorageService.instance.valueFactory.networkVersion());
        states.put(ApplicationState.HOST_ID, StorageService.instance.valueFactory.hostId(uuid));
        localState.addApplicationStates(states);
    }

    @VisibleForTesting
    public void injectApplicationState(InetAddressAndPort endpoint, ApplicationState state, VersionedValue value)
    {
        EndpointState localState = gossipEndpoints.get(endpoint);
        localState.addApplicationState(state, value);
    }

    public long getEndpointDowntime(String address) throws UnknownHostException
    {
        return getEndpointDowntime(StorageService.instance.getTokenMetadata().getEndpointForAddress(InetAddressAndPort.getByName(address)));
    }

    public int getCurrentGenerationNumber(String address) throws UnknownHostException
    {
        return getCurrentGenerationNumber(StorageService.instance.getTokenMetadata().getEndpointForAddress(InetAddressAndPort.getByName(address)));
    }

    public void addExpireTimeForEndpoint(Endpoint endpoint, long expireTime)
    {
        if (logger.isDebugEnabled())
        {
            logger.debug("adding expire time for endpoint : {} ({})", endpoint, expireTime);
        }
        expireTimeEndpointMap.put(endpoint, expireTime);
    }

    public static long computeExpireTime()
    {
        return System.currentTimeMillis() + Gossiper.aVeryLongTime;
    }

    @Nullable
    public CassandraVersion getReleaseVersion(Endpoint ep)
    {
        EndpointState state = getEndpointStateForEndpoint(ep);
        return state != null ? state.getReleaseVersion() : null;
    }

    public Map<String, List<String>> getReleaseVersionsWithPort()
    {
        Map<String, List<String>> results = new HashMap<String, List<String>>();
        Iterable<Endpoint> allHosts = Iterables.concat(Gossiper.instance.getLiveMembers(), Gossiper.instance.getUnreachableMembers());

        for (Endpoint host : allHosts)
        {
            CassandraVersion version = getReleaseVersion(host);
            String stringVersion = version == null ? "" : version.toString();
            List<String> hosts = results.computeIfAbsent(stringVersion, k -> new ArrayList<>());
            hosts.add(host.getPreferredAddress().getHostAddress(true));
        }

        return results;
    }

    @Nullable
    public UUID getSchemaVersion(Endpoint ep)
    {
        EndpointState state = getEndpointStateForEndpoint(ep);
        return state != null ? state.getSchemaVersion() : null;
    }

    public static void waitToSettle()
    {
        int forceAfter = Integer.getInteger("cassandra.skip_wait_for_gossip_to_settle", -1);
        if (forceAfter == 0)
        {
            return;
        }
        final int GOSSIP_SETTLE_MIN_WAIT_MS = 5000;
        final int GOSSIP_SETTLE_POLL_INTERVAL_MS = 1000;
        final int GOSSIP_SETTLE_POLL_SUCCESSES_REQUIRED = 3;

        logger.info("Waiting for gossip to settle...");
        Uninterruptibles.sleepUninterruptibly(GOSSIP_SETTLE_MIN_WAIT_MS, TimeUnit.MILLISECONDS);
        int totalPolls = 0;
        int numOkay = 0;
        int epSize = Gossiper.instance.getEndpointCount();
        while (numOkay < GOSSIP_SETTLE_POLL_SUCCESSES_REQUIRED)
        {
            Uninterruptibles.sleepUninterruptibly(GOSSIP_SETTLE_POLL_INTERVAL_MS, TimeUnit.MILLISECONDS);
            int currentSize = Gossiper.instance.getEndpointCount();
            totalPolls++;
            if (currentSize == epSize)
            {
                logger.debug("Gossip looks settled.");
                numOkay++;
            }
            else
            {
                logger.info("Gossip not settled after {} polls.", totalPolls);
                numOkay = 0;
            }
            epSize = currentSize;
            if (forceAfter > 0 && totalPolls > forceAfter)
            {
                logger.warn("Gossip not settled but startup forced by cassandra.skip_wait_for_gossip_to_settle. Gossip total polls: {}",
                            totalPolls);
                break;
            }
        }
        if (totalPolls > GOSSIP_SETTLE_POLL_SUCCESSES_REQUIRED)
            logger.info("Gossip settled after {} extra polls; proceeding", totalPolls - GOSSIP_SETTLE_POLL_SUCCESSES_REQUIRED);
        else
            logger.info("No gossip backlog; proceeding");
    }

}
