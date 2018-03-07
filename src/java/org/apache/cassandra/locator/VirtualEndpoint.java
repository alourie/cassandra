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
import javax.xml.crypto.Data;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.net.HostAndPort;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.net.MessagingService;
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
public final class VirtualEndpoint implements Comparable<VirtualEndpoint>, Serializable
{
    private static final Logger logger = LoggerFactory.getLogger(VirtualEndpoint.class);
    private static final long serialVersionUID = 0;

    public static final UUID initialHostId = UUID.fromString("00000000-0000-0000-0000-000000000000");

    //Store these here to avoid requiring DatabaseDescriptor to be loaded. DatabaseDescriptor will set
    //these when it loads the config. A lot of unit tests won't end up loading DatabaseDescriptor.
    //Tools that might use this class also might not load database descriptor. Those tools are expected
    //to always override the defaults.
    static volatile int defaultPort = 7000;

    public final InetAddress address;
    public final byte[] addressBytes;
    public final int port;
    public UUID hostId;

    private VirtualEndpoint(InetAddress address, byte[] addressBytes, int port, UUID hostId)
    {
        Preconditions.checkNotNull(address);
        Preconditions.checkNotNull(addressBytes);
        Preconditions.checkNotNull(hostId);
        validatePortRange(port);
        this.address = address;
        this.port = port;
        this.addressBytes = addressBytes;
        this.hostId = hostId;
    }

    private static void validatePortRange(int port)
    {
        if (port < 0 | port > 65535)
        {
            throw new IllegalArgumentException("Port " + port + " is not a valid port number in the range 0-65535");
        }
    }

    @Override
    public boolean equals(Object o)
    {
        VirtualEndpoint that = (VirtualEndpoint) o;
        return equalAddresses(o) && hostId.equals(that.hostId);
    }

    public boolean equalAddresses(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        VirtualEndpoint that = (VirtualEndpoint) o;

        if (port != that.port) return false;
        return address.equals(that.address);
    }

    @Override
    public int hashCode()
    {
        int result = address.hashCode();
        result = 31 * result + port + hostId.hashCode();
        return result;
    }

    @Override
    public int compareTo(VirtualEndpoint o)
    {

        if (o.hostId == null)
            return 1;

        int retval = FastByteOperations.compareUnsigned(addressBytes, 0, addressBytes.length, o.addressBytes, 0, o.addressBytes.length);
        if (retval != 0)
        {
            return retval;
        }

        int retvalHostId = hostId.compareTo(o.hostId);
        if (retvalHostId != 0)
        {
            return retvalHostId;
        }

        return Integer.compare(port, o.port);
    }

    public String getHostAddress(boolean withPort)
    {
        if (withPort)
        {
            return toString();
        }
        else
        {
            return address.getHostAddress();
        }
    }

    @Override
    public String toString()
    {
        return toString(true);
    }

    public String toString(boolean withPort)
    {
        if (withPort)
        {
            return HostAndPort.fromParts(address.getHostAddress(), port).toString();
        }
        else
        {
            return address.toString();
        }
    }

    public String toStringWithHostId(boolean withPort)
    {
        return toString(withPort) + " (HostId: " + hostId + ")";
    }

    public static VirtualEndpoint getByName(String name) throws UnknownHostException
    {
        return getByNameOverrideDefaults(name, null);
    }

    /**
     *
     * @param name Hostname + optional ports string
     * @param port Port to connect on, overridden by values in hostname string, defaults to DatabaseDescriptor default if not specified anywhere.
     * @return
     * @throws UnknownHostException
     */
    public static VirtualEndpoint getByNameOverrideDefaults(String name, Integer port) throws UnknownHostException
    {
        HostAndPort hap = HostAndPort.fromString(name);
        if (hap.hasPort())
        {
            port = hap.getPort();
        }
        return getByAddressOverrideDefaults(InetAddress.getByName(hap.getHost()), port);
    }

    public static VirtualEndpoint getByAddress(byte[] address) throws UnknownHostException
    {
        return getByAddress(InetAddress.getByAddress(address));
    }

    public static VirtualEndpoint getByAddress(InetAddress address)
    {
        return getByAddressOverrideDefaults(address, null);
    }

    public static VirtualEndpoint getByAddressOverrideDefaults(InetAddress address, Integer port)
    {

        UUID hostId = initialHostId;
//
//        final boolean messagingServiceListening = DatabaseDescriptor.isInTest() && MessagingService.test().isListening();
//        final boolean fullState = DatabaseDescriptor.isClientInitialized();
        if (DatabaseDescriptor.isLocalDataRetreiveable())
            hostId = SystemKeyspace.getLocalHostId();

        return getByAddressOverrideDefaults(address, port, hostId);
    }

    public static VirtualEndpoint getByAddressOverrideDefaults(InetAddress address, Integer port, UUID hostId)
    {
        if (port == null)
            port = defaultPort;

        if (hostId == null)
            hostId = initialHostId;

        return new VirtualEndpoint(address, address.getAddress(), port, hostId);
    }

    public static VirtualEndpoint getLoopbackAddress()
    {
        return VirtualEndpoint.getByAddress(InetAddress.getLoopbackAddress());
    }

    public static VirtualEndpoint getLocalHost() throws UnknownHostException
    {
        return VirtualEndpoint.getByAddress(InetAddress.getLocalHost());
    }

    public static void initializeDefaultPort(int port)
    {
        defaultPort = port;
    }
}
