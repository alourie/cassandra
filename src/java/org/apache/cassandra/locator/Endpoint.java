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

import java.io.Serializable;
import java.net.UnknownHostException;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.HeartBeatState;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

/**
 * A class to replace the usage of InetAddress to identify hosts in the cluster.
 * Opting for a full replacement class so that in the future if we change the nature
 * of the identifier the refactor will be easier in that we don't have to change the type
 * just the methods.
 *
 * Because an IP might contain multiple C* instances the identification must be done
 * using the IP + port. InetSocketAddress is undesirable for a couple of reasons. It's not comparable,
 * it's toString() method doesn't correctly bracket IPv6, it doesn't handle optional default values,
 * and a couple of other minor behaviors that are slightly less troublesome like handling the
 * need to sometimes return a port and sometimes not.
 *
 */
public final class Endpoint implements Comparable<Endpoint>, Serializable
{
    private static final Logger logger = LoggerFactory.getLogger(Endpoint.class);
    private static final long serialVersionUID = 0;

    public EndpointState state;


    private InetAddressAndPort listenAddress;
    private InetAddressAndPort broadcastAddress;
    // TODO: do we still need it?? Can't say
    private InetAddressAndPort nativeAddress;
    private InetAddressAndPort broadcastNativeAddress;
    private InetAddressAndPort preferredAddress = null;

    public Endpoint(final InetAddressAndPort listenAddress,
                    final InetAddressAndPort broadcastAddress,
                    final InetAddressAndPort nativeAddress,
                    final InetAddressAndPort broadcastNativeAddress)
    {
        if (listenAddress == null)
            throw new IllegalArgumentException("listen_address provided is empty!");

        this.listenAddress = listenAddress;
        if (broadcastAddress == null)
        {
            this.broadcastAddress = listenAddress;
        }
        else
        {
            this.broadcastAddress = broadcastAddress;
        }

        if (nativeAddress == null)
        {
            this.nativeAddress = getDefaultNativeAddress();
        }
        else
        {
            this.nativeAddress = nativeAddress;
        }

        if (broadcastNativeAddress == null)
        {
            if (this.nativeAddress != getDefaultNativeAddress())
                this.broadcastNativeAddress = nativeAddress;
            else
                this.broadcastNativeAddress = this.broadcastAddress;
        }
        else
        {
            this.broadcastNativeAddress = broadcastAddress;
        }

        // hostId can be null in some cases, for example when seeds list is initiated and only IP is known
    }

    public String getGossipStatus()
    {
        return EndpointState.getGossipStatus(state);
    }

    @Override
    public boolean equals(Object o)
    {
        Endpoint that = (Endpoint) o;
        UUID localID = getHostId();
        UUID thatID = that == null ? null : that.getHostId();
        return equalAddresses(o) && ((localID == null && thatID == null) || (localID != null && localID.equals(thatID)));
    }

    public boolean equalAddresses(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Endpoint that = (Endpoint) o;

        return equalAddresses(listenAddress, that.listenAddress) &&
               equalAddresses(broadcastAddress, that.broadcastAddress) &&
               equalAddresses(nativeAddress, that.nativeAddress) &&
               equalAddresses(broadcastNativeAddress, that.broadcastNativeAddress);
    }

    private boolean equalAddresses(InetAddressAndPort me, InetAddressAndPort other)
    {
        // both null
        if (me == null && other == null)
            return true;

        // me not null
        if (me != null)
            return me.equals(other);

        // me null, other not null
        return false;
    }

    @Override
    public int hashCode()
    {
        int result = 0;
        result += listenAddress.hashCode() + broadcastAddress.hashCode() + nativeAddress.hashCode() + broadcastNativeAddress.hashCode();
        return 31 * result;
    }

    @Override
    public int compareTo(Endpoint o)
    {

        int retvalAddresses = listenAddress.compareTo(o.listenAddress) + broadcastAddress.compareTo(o.broadcastAddress) + broadcastNativeAddress.compareTo(o.broadcastNativeAddress);
        if (retvalAddresses != 0)
        {
            return retvalAddresses;
        }

        // TODO: compare hostIDs
        // TODO? : compare states?

        return 0;
    }


    @Override
    public String toString()
    {
        String endpoint = "Endpoint %s (hostId: %s)";
        return String.format(endpoint, getPreferredAddress().toString(), getHostId());
    }

    public String toStringBig()
    {
        String endpoint = "Endpoint:\n";

        if (listenAddress != null)
            endpoint += "LocalAddress: " + listenAddress.toString() + "\n";

        if (broadcastAddress != null)
            endpoint += "Broadcast address: " + broadcastAddress.toString() + "\n";

        if (nativeAddress != null)
            endpoint += "Native address: " + nativeAddress.toString() + "\n";

        if (broadcastNativeAddress != null)
            endpoint += "Broadcast native address: " + broadcastNativeAddress.toString() + "\n";

        endpoint += "HostID: " + getHostId();

        return endpoint;
    }

    public boolean hasAddress(InetAddressAndPort address)
    {
        return listenAddress.equals(address) || broadcastAddress.equals(address) || nativeAddress.equals(address) || broadcastNativeAddress.equals(address);
    }

    public void updateValuesFrom(final Endpoint sourceEndpoint)
    {
        this.listenAddress = sourceEndpoint.listenAddress;
        this.broadcastAddress = sourceEndpoint.broadcastAddress;
        this.broadcastNativeAddress = sourceEndpoint.broadcastNativeAddress;
        this.nativeAddress = sourceEndpoint.nativeAddress;
    }

    public UUID getHostId()
    {
        return state == null ? null : UUID.fromString(state.getApplicationState(ApplicationState.HOST_ID).value);
    }

    public InetAddressAndPort getPreferredAddress()
    {
        if (preferredAddress == null)
        {
            preferredAddress = MessagingService.instance().getPreferredRemoteAddr(broadcastAddress);
        }
        return preferredAddress;
    }

    public void resetPreferredAddress()
    {
        preferredAddress = null;
        getPreferredAddress();
    }

    private InetAddressAndPort getDefaultNativeAddress()
    {
        try
        {
            return InetAddressAndPort.getByName("0.0.0.0");
        }
        catch (UnknownHostException e)
        {
            logger.info("Error creating an InetAddress object for defaultNativeAddress 0.0.0.0");
            return null;
        }
    }

    public static Endpoint getLocalEndpoint()
    {
        Endpoint localEndpoint = StorageService.instance.getTokenMetadata().getEndpointForAddress(FBUtilities.getBroadcastAddressAndPort(), false);

        // not in TokenMetadata yet
        if (localEndpoint == null)
            localEndpoint = new Endpoint(
            FBUtilities.getLocalAddressAndPort(),
            FBUtilities.getBroadcastAddressAndPort(),
            null,
            FBUtilities.getBroadcastNativeAddressAndPort()
            );
        return localEndpoint;
    }

    public boolean isShutdown()
    {
        if (state == null)
        {
            return false;
        }

        VersionedValue versionedValue = state.getApplicationState(ApplicationState.STATUS_WITH_PORT);
        if (versionedValue == null)
        {
            versionedValue = state.getApplicationState(ApplicationState.STATUS);
            if (versionedValue == null)
            {
                return false;
            }
        }

        String value = versionedValue.value;
        String[] pieces = value.split(VersionedValue.DELIMITER_STR, -1);
        assert (pieces.length > 0);
        String appState = pieces[0];
        return appState.equals(VersionedValue.SHUTDOWN);
    }

    public boolean isAlive()
    {
        return state.isAlive();
    }

    public boolean isSilentShutdownState()
    {
        String status = getGossipStatus();
        return !status.isEmpty() && EndpointState.SILENT_SHUTDOWN_STATES.contains(status);
    }


    public EndpointState getStateForVersionBiggerThan(int version)
    {
        return state.getStateForVersionBiggerThan(version, getLocalEndpoint().toString());
    }
}

