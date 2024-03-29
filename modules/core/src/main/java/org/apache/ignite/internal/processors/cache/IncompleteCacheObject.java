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

package org.apache.ignite.internal.processors.cache;

import java.nio.ByteBuffer;

/**
 * Cache object container that accumulates partial binary data
 * unless all of them ready.
 */
public class IncompleteCacheObject extends IncompleteObject<CacheObject> {
    /** 4 bytes - cache object length, 1 byte - type. */
    public static final int HEAD_LEN = 5;

    /** */
    protected byte type;

    /** */
    protected int headOff;

    /** */
    protected byte[] head;

    /**
     * Default constructor
     */
    protected IncompleteCacheObject() {
    }

    /**
     * @param buf Byte buffer.
     */
    public IncompleteCacheObject(final ByteBuffer buf) {
        if (buf.remaining() >= HEAD_LEN) {
            data = new byte[buf.getInt()];
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
        if (data == null) {
            assert head != null;

            final int len = Math.min(HEAD_LEN - headOff, buf.remaining());

            buf.get(head, headOff, len);

            headOff += len;

            if (headOff == HEAD_LEN) {
                final ByteBuffer headBuf = ByteBuffer.wrap(head);

                headBuf.order(buf.order());

                data = new byte[headBuf.getInt()];
                type = headBuf.get();

                headerReady();
            }
        }

        if (data != null)
            super.readData(buf);
    }

    /**
     * Invoke when object header is ready.
     */
    protected void headerReady() {
        if (type == CacheObject.TOMBSTONE)
            object(TombstoneCacheObject.INSTANCE);
    }

    /**
     * @return Size of already read data.
     */
    public int dataOffset() {
        return off;
    }

    /**
     * @return Data type.
     */
    public byte type() {
        return type;
    }
}
