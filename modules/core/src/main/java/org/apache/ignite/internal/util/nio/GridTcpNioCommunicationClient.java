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

package org.apache.ignite.internal.util.nio;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.lang.IgniteInClosure2X;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.communication.tcp.messages.ConnectionCheckMessage;
import org.jetbrains.annotations.Nullable;

/**
 * Grid client for NIO server.
 */
public class GridTcpNioCommunicationClient extends GridAbstractCommunicationClient {
    /** Session. */
    private final GridNioSession ses;

    /** Logger. */
    private final IgniteLogger log;

    /** Remote node may use connection check message. */
    private final boolean enableConnectionCheckMessage;

    /**
     * @param connIdx Connection index.
     * @param ses Session.
     * @param log Logger.
     */
    public GridTcpNioCommunicationClient(
        int connIdx,
        GridNioSession ses,
        IgniteLogger log,
        boolean enableConnectionCheckMessage
    ) {
        super(connIdx);

        assert ses != null;
        assert log != null;

        this.ses = ses;
        this.log = log;
        this.enableConnectionCheckMessage = enableConnectionCheckMessage;
    }

    /**
     * @return Gets underlying session.
     */
    public GridNioSession session() {
        return ses;
    }

    /** {@inheritDoc} */
    @Override public void doHandshake(IgniteInClosure2X<InputStream, OutputStream> handshakeC) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean close() {
        boolean res = super.close();

        if (res)
            ses.close();

        return res;
    }

    /** {@inheritDoc} */
    @Override public void forceClose() {
        super.forceClose();

        ses.close();
    }

    /** {@inheritDoc} */
    @Override public void sendMessage(byte[] data, int len) throws IgniteCheckedException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void sendMessage(ByteBuffer data) throws IgniteCheckedException {
        if (closed())
            throw new IgniteCheckedException("Client was closed: " + this);

        GridNioFuture<?> fut = ses.send(data);

        if (fut.isDone())
            fut.get();
    }

    /** {@inheritDoc} */
    @Override public boolean sendMessage(@Nullable UUID nodeId, Message msg, IgniteInClosure<IgniteException> c)
        throws IgniteCheckedException {
        try {
            // Node ID is never provided in asynchronous send mode.
            assert nodeId == null;

            ses.sendNoFuture(msg, c);
        }
        catch (IgniteCheckedException e) {
            if (log.isDebugEnabled())
                log.debug("Failed to send message [client=" + this + ", err=" + e + ']');

            if (e.getCause() instanceof IOException) {
                ses.close();

                return true;
            }
            else
                throw new IgniteCheckedException("Failed to send message [client=" + this + ']', e);
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean async() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public long getIdleTime() {
        long now = U.currentTimeMillis();

        // Session can be used for receiving and sending.
        return Math.min(Math.min(now - ses.lastReceiveTime(), now - ses.lastSendScheduleTime()),
            now - ses.lastSendTime());
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridTcpNioCommunicationClient.class, this, super.toString());
    }

    /**
     * Sends special {@link ConnectionCheckMessage} through the channel to check if connection still alive.
     */
    public void checkConnectionIfEnabled() {
        if (!enableConnectionCheckMessage)
            return;

        ConnectionCheckMessage msg = new ConnectionCheckMessage();

        ses.send(msg);

        if (log.isDebugEnabled())
            log.debug("Connection check message was sent [rmtAddr=" + ses.remoteAddress()
                    + ", locAddr=" + ses.localAddress() + "]");
    }
}
