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

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class VirtualEndpointTest
{
    private static interface ThrowingRunnable
    {
        public void run() throws Throwable;
    }

    @Test
    public void getByNameIPv4Test() throws Exception
    {
        //Negative port
        shouldThrow(() -> VirtualEndpoint.getByName("127.0.0.1:-1"), IllegalArgumentException.class);
        //Too large port
        shouldThrow(() -> VirtualEndpoint.getByName("127.0.0.1:65536"), IllegalArgumentException.class);

        //bad address, caught by InetAddress
        shouldThrow(() -> VirtualEndpoint.getByName("127.0.0.1.0"), UnknownHostException.class);

        //Test default port
        VirtualEndpoint address = VirtualEndpoint.getByName("127.0.0.1");
        assertEquals(InetAddress.getByName("127.0.0.1"), address.address);
        assertEquals(VirtualEndpoint.defaultPort, address.port);

        //Test overriding default port
        address = VirtualEndpoint.getByName("127.0.0.1:42");
        assertEquals(InetAddress.getByName("127.0.0.1"), address.address);
        assertEquals(42, address.port);
    }

    @Test
    public void getByNameIPv6Test() throws Exception
    {
        //Negative port
        shouldThrow(() -> VirtualEndpoint.getByName("[2001:0db8:0000:0000:0000:ff00:0042:8329]:-1"), IllegalArgumentException.class);
        //Too large port
        shouldThrow(() -> VirtualEndpoint.getByName("[2001:0db8:0000:0000:0000:ff00:0042:8329]:65536"), IllegalArgumentException.class);

        //bad address, caught by InetAddress
        shouldThrow(() -> VirtualEndpoint.getByName("2001:0db8:0000:0000:0000:ff00:0042:8329:8329"), UnknownHostException.class);

        //Test default port
        VirtualEndpoint address = VirtualEndpoint.getByName("2001:0db8:0000:0000:0000:ff00:0042:8329");
        assertEquals(InetAddress.getByName("2001:0db8:0000:0000:0000:ff00:0042:8329"), address.address);
        assertEquals(VirtualEndpoint.defaultPort, address.port);

        //Test overriding default port
        address = VirtualEndpoint.getByName("[2001:0db8:0000:0000:0000:ff00:0042:8329]:42");
        assertEquals(InetAddress.getByName("2001:0db8:0000:0000:0000:ff00:0042:8329"), address.address);
        assertEquals(42, address.port);
    }

    @Test
    public void compareAndEqualsAndHashCodeTest() throws Exception
    {
        VirtualEndpoint address1 = VirtualEndpoint.getByName("127.0.0.1:42");
        VirtualEndpoint address4 = VirtualEndpoint.getByName("127.0.0.1:43");
        VirtualEndpoint address5 = VirtualEndpoint.getByName("127.0.0.1:41");
        VirtualEndpoint address6 = VirtualEndpoint.getByName("127.0.0.2:42");
        VirtualEndpoint address7 = VirtualEndpoint.getByName("127.0.0.0:42");

        assertEquals(0, address1.compareTo(address1));
        assertEquals(-1, address1.compareTo(address4));
        assertEquals(1, address1.compareTo(address5));
        assertEquals(-1, address1.compareTo(address6));
        assertEquals(1, address1.compareTo(address7));

        assertEquals(address1, address1);
        assertEquals(address1.hashCode(), address1.hashCode());
        assertEquals(address1, VirtualEndpoint.getByName("127.0.0.1:42"));
        assertEquals(address1.hashCode(), VirtualEndpoint.getByName("127.0.0.1:42").hashCode());
        assertEquals(address1, VirtualEndpoint.getByNameOverrideDefaults("127.0.0.1", 42));
        assertEquals(address1.hashCode(), VirtualEndpoint.getByNameOverrideDefaults("127.0.0.1", 42).hashCode());
        int originalPort = VirtualEndpoint.defaultPort;
        VirtualEndpoint.initializeDefaultPort(42);
        try
        {
            assertEquals(address1, VirtualEndpoint.getByName("127.0.0.1"));
            assertEquals(address1.hashCode(), VirtualEndpoint.getByName("127.0.0.1").hashCode());
        }
        finally
        {
            VirtualEndpoint.initializeDefaultPort(originalPort);
        }
        assertTrue(!address1.equals(address4));
        assertTrue(!address1.equals(address5));
        assertTrue(!address1.equals(address6));
        assertTrue(!address1.equals(address7));
    }

    @Test
    public void toStringTest() throws Exception
    {
        String ipv4 = "127.0.0.1:42";
        String ipv6 = "[2001:db8:0:0:0:ff00:42:8329]:42";
        assertEquals(ipv4, VirtualEndpoint.getByName(ipv4).toString());
        assertEquals(ipv6, VirtualEndpoint.getByName(ipv6).toString());
    }


    private void shouldThrow(ThrowingRunnable t, Class expectedClass)
    {
        try
        {
            t.run();
        }
        catch (Throwable thrown)
        {
            assertEquals(thrown.getClass(), expectedClass);
            return;
        }
        fail("Runnable didn't throw");
    }

}
