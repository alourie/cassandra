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
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.net.MessagingService;

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

    private InetAddressAndPort listenAddress;
    private InetAddressAndPort broadcastAddress;
    private InetAddressAndPort nativeAddress;
    private InetAddressAndPort broadcastNativeAddress;
    private InetAddressAndPort preferredAddress = null;

    private UUID hostId;


    private final InetAddressAndPort defaultNativeAddress = getDefaultNativeAddress();

    public Endpoint(final InetAddressAndPort listenAddress,
                    final InetAddressAndPort broadcastAddress,
                    final InetAddressAndPort nativeAddress,
                    final InetAddressAndPort broadcastNativeAddress,
                    final UUID hostId)
    {
        if (listenAddress == null)
            throw new IllegalArgumentException("listen address provided is empty!");

        this.listenAddress = listenAddress;
        if (broadcastAddress == null) {
            this.broadcastAddress = listenAddress;
        }
        else
        {
            this.broadcastAddress = broadcastAddress;
        }

        if (nativeAddress == null) {
            this.nativeAddress = defaultNativeAddress;
        }
        else
        {
            this.nativeAddress = nativeAddress;
        }

        if (broadcastNativeAddress == null) {
            if (this.nativeAddress != defaultNativeAddress)
                this.broadcastNativeAddress = nativeAddress;
            else
                this.broadcastNativeAddress = this.broadcastAddress;
        }
        else
        {
            this.broadcastNativeAddress= broadcastAddress;
        }

        // hostId can be null in some cases, for example when seeds list is initiated and only IP is known
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

        return equalAddresses(listenAddress, that.listenAddress) &&
               equalAddresses(broadcastAddress, that.broadcastAddress) &&
               equalAddresses(nativeAddress, that.nativeAddress) &&
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
        result += listenAddress.hashCode() + broadcastAddress.hashCode() + nativeAddress.hashCode() + broadcastNativeAddress.hashCode();
        return 31 * result + hostId.hashCode();
    }

    @Override
    public int compareTo(Endpoint o)
    {
//
//        if (o.hostId == null)
//            return 1;

        int retvalAddresses = listenAddress.compareTo(o.listenAddress) + broadcastAddress.compareTo(o.broadcastAddress) + broadcastNativeAddress.compareTo(o.broadcastNativeAddress);
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
        String endpoint = "Endpoint %s (hostId: %s)";
        return String.format(endpoint, getPreferredAddress().toString(), hostId);
    }

    public String toStringBig()
    {
        String endpoint = "Endpoint:\n";

        if (listenAddress != null)
            endpoint += "LocalAddress: " + listenAddress.toString() + "\n";

        if (broadcastAddress != null)
            endpoint += "Broadcast address: " + broadcastAddress.toString() + "\n";

        if (nativeAddress != null)
            endpoint += "Native address: " + nativeAddress.toString()  + "\n";

        if (broadcastNativeAddress != null)
            endpoint += "Broadcast native address: " + broadcastNativeAddress.toString()  + "\n";

        endpoint += "HostID: " + hostId;

        return endpoint;

    }

    @Override
    public Endpoint clone() {
        return new Endpoint(listenAddress, broadcastAddress, nativeAddress, broadcastNativeAddress, hostId);
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
        this.hostId = sourceEndpoint.hostId;
    }

    public UUID getHostId() {return hostId;}

    public void setHostId(final UUID uuid) {hostId = uuid;}

    public InetAddressAndPort getPreferredAddress()
    {
        if (preferredAddress == null)
        {
            preferredAddress = MessagingService.instance().getPreferredRemoteAddr(broadcastAddress);
        }
        return preferredAddress;
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
