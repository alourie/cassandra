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
package org.apache.cassandra.net;

import java.io.*;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.util.UUID;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.VirtualEndpoint;

/*
 * As of version 4.0 the endpoint description includes a port number as an unsigned short
 */
public class CompactEndpointSerializationHelper implements IVersionedSerializer<VirtualEndpoint>
{
    public static final IVersionedSerializer<VirtualEndpoint> instance = new CompactEndpointSerializationHelper();

    /**
     * Streaming uses its own version numbering so we need to ignore it and always use currrent version.
     * There is no cross version streaming so it will always use the latest address serialization.
     **/
    public static final IVersionedSerializer<VirtualEndpoint> streamingInstance = new IVersionedSerializer<VirtualEndpoint>()
    {
        public void serialize(VirtualEndpoint virtualEndpoint, DataOutputPlus out, int version) throws IOException
        {
            instance.serialize(virtualEndpoint, out, MessagingService.current_version);
        }

        public VirtualEndpoint deserialize(DataInputPlus in, int version) throws IOException
        {
            return instance.deserialize(in, MessagingService.current_version);
        }

        public long serializedSize(VirtualEndpoint virtualEndpoint, int version)
        {
            return instance.serializedSize(virtualEndpoint, MessagingService.current_version);
        }
    };

    private CompactEndpointSerializationHelper() {}

    public void serialize(VirtualEndpoint endpoint, DataOutputPlus out, int version) throws IOException
    {
        if (version >= MessagingService.VERSION_40)
        {
            byte[] buf = endpoint.addressBytes;
            out.writeByte(buf.length + 2 + 16);
            out.write(buf);
            out.writeShort(endpoint.port);
            out.writeLong(endpoint.hostId.getMostSignificantBits());
            out.writeLong(endpoint.hostId.getLeastSignificantBits());
        }
        else
        {
            byte[] buf = endpoint.addressBytes;
            out.writeByte(buf.length);
            out.write(buf);
        }
    }

    // TODO (De)serialize hostId
    public VirtualEndpoint deserialize(DataInputPlus in, int version) throws IOException
    {
        int size = in.readByte() & 0xFF;
        switch(size)
        {
            //The original pre-4.0 serialiation of just an address
            case 4:
            case 16:
            {
                byte[] bytes = new byte[size];
                in.readFully(bytes, 0, bytes.length);
                return VirtualEndpoint.getByAddress(InetAddress.getByAddress(bytes));
            }
            //Address and one port
            case 6:
            case 18:
            {
                byte[] bytes = new byte[size - 2];
                in.readFully(bytes);

                int port = in.readShort() & 0xFFFF;
                return VirtualEndpoint.getByAddressOverrideDefaults(InetAddress.getByAddress(bytes), port, null);
            }
            // Address, port and hostId
            case 22:
            case 34:
            {
                byte[] addressBytes = new byte[size - 18];
                in.readFully(addressBytes);

                int port = in.readShort() & 0xFFFF;

                long mostSig = in.readLong();
                long leastSig = in.readLong();
                UUID hostId = new UUID(mostSig, leastSig);
                return VirtualEndpoint.getByAddressOverrideDefaults(InetAddress.getByAddress(addressBytes), port, hostId);
            }
            default:
                throw new AssertionError("Unexpected size " + size);

        }
    }

    public long serializedSize(VirtualEndpoint from, int version)
    {
        //4.0 includes a port number
        if (version >= MessagingService.VERSION_40)
        {
            if (from.address instanceof Inet4Address)
                return 1 + 4 + 2 + 16;
            assert from.address instanceof Inet6Address;
            return 1 + 16 + 2 + 16;
        }
        else
        {
            if (from.address instanceof Inet4Address)
                return 1 + 4;
            assert from.address instanceof Inet6Address;
            return 1 + 16;
        }
    }
}
