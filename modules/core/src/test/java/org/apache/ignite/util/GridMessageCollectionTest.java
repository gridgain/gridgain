/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.util;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.direct.DirectMessageReader;
import org.apache.ignite.internal.direct.DirectMessageWriter;
import org.apache.ignite.internal.managers.communication.GridIoMessageFactory;
import org.apache.ignite.internal.managers.communication.IgniteMessageFactoryImpl;
import org.apache.ignite.internal.util.UUIDCollectionMessage;
import org.apache.ignite.plugin.extensions.communication.IgniteMessageFactory;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.junit.Test;

import static java.util.UUID.randomUUID;
import static org.apache.ignite.internal.util.GridMessageCollection.of;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 *
 */
public class GridMessageCollectionTest {
    /** */
    private byte proto;

    /**
     * @param proto Protocol version.
     * @return Writer.
     */
    protected MessageWriter writer(byte proto) {
        return new DirectMessageWriter(proto);
    }

    /**
     * @param msgFactory Message factory.
     * @param proto Protocol version.
     * @return Writer.
     */
    protected MessageReader reader(MessageFactory msgFactory, byte proto) {
        return new DirectMessageReader(msgFactory, proto);
    }

    /**
     *
     */
    @Test
    public void testMarshal() {
        proto = 2;
        doTestMarshal();
    }

    /**
     *
     */
    @Test
    public void testMarshalLegacyMode() {
        proto = 1;
        doTestMarshal();
    }

    /** */
    private void doTestMarshal() {
        UUIDCollectionMessage um0 = UUIDCollectionMessage.of();
        UUIDCollectionMessage um1 = UUIDCollectionMessage.of(randomUUID());
        UUIDCollectionMessage um2 = UUIDCollectionMessage.of(randomUUID(), randomUUID());
        UUIDCollectionMessage um3 = UUIDCollectionMessage.of(randomUUID(), randomUUID(), randomUUID());

        assertNull(um0);
        assertEquals(3, um3.uuids().size());

        doTestMarshal(um1);
        doTestMarshal(um2);
        doTestMarshal(um3);

        doTestMarshal(of(um0));
        doTestMarshal(of(um1));
        doTestMarshal(of(um2));
        doTestMarshal(of(um3));

        doTestMarshal(of(um2, um3));
        doTestMarshal(of(um1, um0, um3));

        doTestMarshal(of(of(um3), of(um2)));
        doTestMarshal(of(of(of(of(of(um0))), um1, of(um3))));
    }

    /**
     * @param m Message.
     */
    private void doTestMarshal(Message m) {
        ByteBuffer buf = ByteBuffer.allocate(8 * 1024);

        m.writeTo(buf, writer(proto));

        buf.flip();

        byte b0 = buf.get();
        byte b1 = buf.get();

        short type = (short)((b1 & 0xFF) << 8 | b0 & 0xFF);

        assertEquals(m.directType(), type);

        IgniteMessageFactory msgFactory =
                new IgniteMessageFactoryImpl(new MessageFactory[]{new GridIoMessageFactory()});

        Message mx = msgFactory.create(type);

        mx.readFrom(buf, reader(msgFactory, proto));

        assertEquals(m, mx);
    }
}
