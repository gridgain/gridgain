/*
 * Copyright 2023 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.cache;

import java.nio.ByteBuffer;

/**
 * Cache object container that only accumulates type of the object without its data.
 */
public class IncompleteCacheObjectShadow extends IncompleteCacheObject {
    /** Value size in bytes. */
    private int valLen;

    /** This variable indicates that the header was red and ready to use. */
    private boolean headerReady;

    /**
     * @param buf Byte buffer.
     */
    public IncompleteCacheObjectShadow(final ByteBuffer buf) {
        if (buf.remaining() >= HEAD_LEN) {
            valLen = buf.getInt();
            type = buf.get();

            headerReady();
        }
        // We cannot fully read head to initialize data buffer.
        // Start partial read of header.
        else
            head = new byte[HEAD_LEN];
    }

    /** {@inheritDoc} */
    @Override public void readData(ByteBuffer buf) {
        if (!headerReady) {
            assert head != null : "Header should be initialized before data reading.";

            int len = Math.min(HEAD_LEN - headOff, buf.remaining());

            buf.get(head, headOff, len);

            headOff += len;

            if (headOff == HEAD_LEN) {
                final ByteBuffer headBuf = ByteBuffer.wrap(head);

                headBuf.order(buf.order());

                valLen = headBuf.getInt();
                type = headBuf.get();

                headerReady();
            }
        }

        if (headerReady) {
            int len = Math.min(valLen - off, buf.remaining());

            buf.position(buf.position() + len);

            off += len;
        }
    }

    /**
     * @return {@code True} if cache object is fully assembled.
     */
    @Override public boolean isReady() {
        return headerReady && off == valLen;
    }

    /**
     * @return Data array.
     */
    @Override public byte[] data() {
        throw new UnsupportedOperationException("Incomplete cache object shadow does not support materialization");
    }

    /** {@inheritDoc} */
    @Override protected void headerReady() {
        super.headerReady();
        headerReady = true;
    }
}
