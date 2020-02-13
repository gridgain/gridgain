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

package org.apache.ignite.spi.discovery.tcp.messages;

import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.managers.discovery.CustomMessageWrapper;
import org.apache.ignite.internal.managers.discovery.IncompleteDeserializationException;
import org.apache.ignite.internal.processors.tracing.messages.SpanContainer;
import org.apache.ignite.internal.processors.tracing.messages.TraceableMessage;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Wrapped for custom message.
 */
@TcpDiscoveryRedirectToClient
@TcpDiscoveryEnsureDelivery
public class TcpDiscoveryCustomEventMessage extends TcpDiscoveryAbstractMessage implements TraceableMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private transient volatile DiscoverySpiCustomMessage msg;

    /** */
    private transient volatile Class<?> msgClass;

    /** */
    private byte[] msgBytes;

    /** Span container. */
    private SpanContainer spanContainer = new SpanContainer();

    /**
     * @param creatorNodeId Creator node id.
     * @param msg Message.
     * @param msgBytes Serialized message.
     */
    public TcpDiscoveryCustomEventMessage(UUID creatorNodeId, @Nullable DiscoverySpiCustomMessage msg,
        @NotNull byte[] msgBytes) {
        super(creatorNodeId);

        this.msg = msg;
        this.msgBytes = msgBytes;
    }

    /**
     * Copy constructor.
     * @param msg Message.
     */
    public TcpDiscoveryCustomEventMessage(TcpDiscoveryCustomEventMessage msg) {
        super(msg);

        this.msgBytes = msg.msgBytes;
        this.msg = msg.msg;
        this.msgClass = msg.msgClass;
        this.spanContainer = msg.spanContainer;
    }

    /**
     * Clear deserialized form of wrapped message.
     */
    public void clearMessage() {
        msg = null;
    }

    /**
     * @return Serialized message.
     */
    public byte[] messageBytes() {
        return msgBytes;
    }

    /**
     * @return Class of DiscoveryCustomMessage enclosed in this discovery custom event.
     * @throws IgniteCheckedException If message was not deserialized from byte array.
     */
    public Class<?> messageClass() throws IgniteCheckedException {
        if (msgClass == null)
            throw new IgniteCheckedException("Message has not been deserialized yet: " + this);

        return msgClass;
    }

    /**
     * @param msg Message.
     * @param msgBytes Serialized message.
     */
    public void message(@Nullable DiscoverySpiCustomMessage msg, @NotNull byte[] msgBytes) {
        this.msg = msg;
        this.msgBytes = msgBytes;
    }

    /**
     * @param marsh Marshaller.
     * @param ldr Classloader.
     * @return Deserialized message,
     * @throws java.lang.Throwable if unmarshal failed.
     */
    @Nullable public DiscoverySpiCustomMessage message(@NotNull Marshaller marsh, ClassLoader ldr) throws Throwable {
        if (msg == null) {
            try {
                msg = U.unmarshal(marsh, msgBytes, ldr);
            }
            catch (IgniteCheckedException e) {
                // Try to resurrect a message in a case of deserialization failure
                if (e.getCause() instanceof IncompleteDeserializationException)
                    return new CustomMessageWrapper(((IncompleteDeserializationException)e.getCause()).message());

                throw e;
            }

            assert msg != null;
        }

        if (msg instanceof CustomMessageWrapper)
            msgClass = ((CustomMessageWrapper)msg).delegate().getClass();

        return msg;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        return super.equals(obj) &&
            obj instanceof TcpDiscoveryCustomEventMessage &&
            F.eq(
                ((TcpDiscoveryCustomEventMessage)obj).verifierNodeId(),
                verifierNodeId());
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TcpDiscoveryCustomEventMessage.class, this, "super", super.toString());
    }

    /** {@inheritDoc} */
    @Override public SpanContainer spanContainer() {
        return spanContainer;
    }
}
