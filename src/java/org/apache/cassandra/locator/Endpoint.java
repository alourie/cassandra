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
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Optional;
import java.util.UUID;

import com.google.common.net.HostAndPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.utils.FastByteOperations;

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

    public InetAddressAndPort localAddress;
    public InetAddressAndPort broadcastAddress;
    public InetAddressAndPort broadcastNativeAddress;

    public UUID hostId;

    public Endpoint(final InetAddressAndPort localAddress,
                    final InetAddressAndPort broadcastAddress,
                    final InetAddressAndPort broadcastNativeAddress,
                    final UUID hostId)
    {
        if (localAddress == null && broadcastAddress == null && broadcastNativeAddress == null)
            throw new IllegalArgumentException("All provided addresses are empty. At least one address must be present!");

        if (hostId == null)
            throw new IllegalArgumentException("UUID must be provided when creating an Endpoint");

        this.localAddress = localAddress;
        this.broadcastAddress = broadcastAddress;
        this.broadcastNativeAddress = broadcastNativeAddress;
        this.hostId = hostId;
    }

    @Override
    public boolean equals(Object o)
    {
        Endpoint that = (Endpoint) o;
        return equalAddresses(o) && hostId.equals(that.hostId);
    }

    public boolean equalAddresses(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Endpoint that = (Endpoint) o;

        return equalAddresses(localAddress, that.localAddress) &&
               equalAddresses(broadcastAddress, that.broadcastAddress) &&
               equalAddresses(broadcastNativeAddress, that.broadcastNativeAddress);
    }

    private boolean equalAddresses(InetAddressAndPort me, InetAddressAndPort other)
    {
        // both null
        if ( me == null && other == null)
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
        if (localAddress != null)
            result += localAddress.hashCode();
        if (broadcastAddress != null)
            result += broadcastAddress.hashCode();
        if (broadcastNativeAddress != null)
            result += broadcastNativeAddress.hashCode();

        return 31 * result + hostId.hashCode();
    }

    @Override
    public int compareTo(Endpoint o)
    {
//
//        if (o.hostId == null)
//            return 1;

        int retvalAddresses = localAddress.compareTo(o.localAddress) + broadcastAddress.compareTo(o.broadcastAddress) + broadcastNativeAddress.compareTo(o.broadcastNativeAddress);
        if (retvalAddresses != 0)
        {
            return retvalAddresses;
        }

        int retvalHostId = hostId.compareTo(o.hostId);
        if (retvalHostId != 0)
        {
            return retvalHostId;
        }

        return 0;
    }


    @Override
    public String toString()
    {
        String endpoint = "Endpoint:\n";

        if (localAddress != null)
            endpoint += "LocalAddress: " + localAddress.toString() + "\n";

        if (broadcastAddress != null)
            endpoint += "Broadcast address: " + broadcastAddress.toString() + "\n";

        if (broadcastNativeAddress != null)
            endpoint += "Broadcast native address: " + broadcastNativeAddress.toString()  + "\n";

        endpoint += "HostID: " + hostId;

        return endpoint;

    }

    @Override
    public Endpoint clone() {
        return new Endpoint(localAddress, broadcastAddress, broadcastNativeAddress, hostId);
    }

    public boolean hasAddress(InetAddressAndPort address)
    {
        return address.equals(localAddress) || address.equals(broadcastAddress) || address.equals(broadcastNativeAddress);
    }

    public InetAddressAndPort getPreferredAddress()
    {
        // TODO
        return localAddress;
    }

    //    public static UUID getHostId()
//    {
//
//        UUID hostId = null;
//        if (DatabaseDescriptor.isClientInitialized() || DatabaseDescriptor.isSystemKeyspaceReadable())
//        {
//            // check the peers first
//            Endpoint testEndpoint = getByAddressOverrideDefaults(address, port, null);
//            Optional<Endpoint> ep = SystemKeyspace.loadHostIds().keySet()
//                                                  .stream().filter(e -> e.equalAddresses(testEndpoint)).findFirst();
//            if (ep.isPresent())
//                hostId = ep.get().hostId;
//
//            // if not found in peers, check local
//            if (hostId == null)
//            {
//                hostId = SystemKeyspace.getLocalHostId(false);
//            }
//
//            // If it's not local, and not a peer, it means it's not initialised yet, use null
//
//        }
//
//    }

}
