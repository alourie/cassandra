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

import java.net.UnknownHostException;

import com.google.common.annotations.VisibleForTesting;

import com.sun.jersey.impl.provider.entity.XMLRootObjectProvider;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.gms.*;
import org.apache.cassandra.net.MessagingService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sidekick helper for snitches that want to reconnect from one IP addr for a node to another.
 * Typically, this is for situations like EC2 where a node will have a public address and a private address,
 * where we connect on the public, discover the private, and reconnect on the private.
 */
public class ReconnectableSnitchHelper implements IEndpointStateChangeSubscriber
{
    private static final Logger logger = LoggerFactory.getLogger(ReconnectableSnitchHelper.class);
    private final IEndpointSnitch snitch;
    private final String localDc;
    private final boolean preferLocal;

    public ReconnectableSnitchHelper(IEndpointSnitch snitch, String localDc, boolean preferLocal)
    {
        this.snitch = snitch;
        this.localDc = localDc;
        this.preferLocal = preferLocal;
    }

    private void reconnect(Endpoint publicAddress, VersionedValue localAddressValue)
    {
        try
        {
            reconnect(publicAddress, InetAddressAndPort.getByName(localAddressValue.value), snitch, localDc);
        }
        catch (UnknownHostException e)
        {
            logger.error("Error in getting the IP address resolved: ", e);
        }
    }

    @VisibleForTesting
    static void reconnect(Endpoint endpoint, InetAddressAndPort newLocalAddress, IEndpointSnitch snitch, String localDc)
    {
        final InetAddressAndPort localAddress = endpoint.getPreferredAddress();
        if (!DatabaseDescriptor.getInternodeAuthenticator().authenticate(localAddress.address, MessagingService.instance().portFor(localAddress)))
        {
            logger.debug("InternodeAuthenticator said don't reconnect to {} on {}", localAddress, newLocalAddress);
            return;
        }

        if (snitch.getDatacenter(endpoint).equals(localDc) && !localAddress.equals(newLocalAddress))
        {
            MessagingService.instance().reconnectWithNewIp(localAddress, newLocalAddress);
            logger.debug("Initiated reconnect to an Internal IP {} for the {}", newLocalAddress, localAddress);
            endpoint.resetPreferredAddress();
        }
    }

    public void beforeChange(InetAddressAndPort endpoint, EndpointState currentState, ApplicationState newStateKey, VersionedValue newValue)
    {
        // no-op
    }

    public void onJoin(Endpoint endpoint)
    {
        if (preferLocal && !endpoint.state.inDeadState())
        {
            VersionedValue address = endpoint.state.getApplicationState(ApplicationState.INTERNAL_ADDRESS_AND_PORT);
            if (address == null)
            {
                address = endpoint.state.getApplicationState(ApplicationState.INTERNAL_IP);
            }
            if (address != null)
            {
                reconnect(endpoint, address);
            }
        }
    }

    //Skeptical this will always do the right thing all the time port wise. It will converge on the right thing
    //eventually once INTERNAL_ADDRESS_AND_PORT is populated
    public void onChange(Endpoint endpoint, ApplicationState state, VersionedValue value)
    {
        if (preferLocal && !endpoint.state.inDeadState())
        {
            if ((state == ApplicationState.INTERNAL_ADDRESS_AND_PORT) ||
                (state == ApplicationState.INTERNAL_IP && endpoint.state.getApplicationState(ApplicationState.INTERNAL_ADDRESS_AND_PORT) == null))
            {
                reconnect(endpoint, value);
            }
        }
    }

    public void onAlive(Endpoint endpoint)
    {
        VersionedValue internalIP = endpoint.state.getApplicationState(ApplicationState.INTERNAL_IP);
        VersionedValue internalIPAndPorts = endpoint.state.getApplicationState(ApplicationState.INTERNAL_ADDRESS_AND_PORT);
        if (preferLocal && internalIP != null)
            reconnect(endpoint, internalIPAndPorts != null ? internalIPAndPorts : internalIP);
    }

    public void onDead(InetAddressAndPort endpoint, EndpointState state)
    {
        // do nothing.
    }

    public void onRemove(InetAddressAndPort endpoint)
    {
        // do nothing.
    }

    public void onRestart(InetAddressAndPort endpoint, EndpointState state)
    {
        // do nothing.
    }
}
