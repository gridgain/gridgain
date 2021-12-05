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

package org.apache.ignite.internal.processors.odbc;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.nio.GridNioParser;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.internal.util.nio.GridNioSessionMetaKey;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * This class implements stream parser based on {@link ClientListenerNioServerBuffer}.
 * <p>
 * The rule for this parser is that every message sent over the stream is prepended with
 * 4-byte integer header containing message size. So, the stream structure is as follows:
 * <pre>
 *     +--+--+--+--+--+--+...+--+--+--+--+--+--+--+...+--+
 *     | MSG_SIZE  |   MESSAGE  | MSG_SIZE  |   MESSAGE  |
 *     +--+--+--+--+--+--+...+--+--+--+--+--+--+--+...+--+
 * </pre>
 */
public class ClientListenerNioMessageParser implements GridNioParser {
    /** Message metadata key. */
    static final int MSG_META_KEY = GridNioSessionMetaKey.nextUniqueKey();

    /** First message key. */
    static final int FIRST_MESSAGE_RECEIVED_KEY = GridNioSessionMetaKey.nextUniqueKey();

    /** */
    private final IgniteLogger log;

    /** */
    public ClientListenerNioMessageParser(IgniteLogger log) {
        this.log = log;
    }

    /** {@inheritDoc} */
    @Override public Object decode(GridNioSession ses, ByteBuffer buf) throws IOException, IgniteCheckedException {
        ClientMessage msg = ses.removeMeta(MSG_META_KEY);

        try {
            if (msg == null)
                msg = new ClientMessage(ses.meta(FIRST_MESSAGE_RECEIVED_KEY) == null);

            boolean finished = false;

            if (buf.hasRemaining())
                finished = msg.readFrom(buf);

            if (finished) {
                ses.addMeta(FIRST_MESSAGE_RECEIVED_KEY, true);

                return msg;
            }
            else {
                ses.addMeta(MSG_META_KEY, msg);

                return null;
            }
        }
        catch (Throwable e) {
            U.error(log, "Failed to read message [msg=" + msg +
                    ", buf=" + buf + ", ses=" + ses + "]", e);

            throw e;
        }
    }

    /** {@inheritDoc} */
    @Override public ByteBuffer encode(GridNioSession ses, Object msg) throws IOException, IgniteCheckedException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return ClientListenerNioMessageParser.class.getSimpleName();
    }
}
