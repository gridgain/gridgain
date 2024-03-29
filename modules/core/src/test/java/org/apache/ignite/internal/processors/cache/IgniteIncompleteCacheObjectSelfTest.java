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
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.CacheObjectAdapter.putValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * Simple test for arbitrary CacheObject reading/writing.
 */
public class IgniteIncompleteCacheObjectSelfTest extends GridCommonAbstractTest {
    /**
     * Test case when requested data cut on cache object header.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testIncompleteObject() throws Exception {
        testIncompleteObject(false);
    }

    @Test
    public void testIncompleteObjectShadow() throws Exception {
        testIncompleteObject(true);
    }

    private void testIncompleteObject(boolean objectShadow) throws Exception {
        final byte[] data = new byte[1024];

        ThreadLocalRandom.current().nextBytes(data);

        final ByteBuffer dataBuf = ByteBuffer.allocate(IncompleteCacheObject.HEAD_LEN + data.length);

        int off = 0;
        int len = 3;

        final TestCacheObject obj = new TestCacheObject((byte) 1);

        // Write part of the cache object and cut on header (3 bytes instead of 5)
        assert putValue(obj.cacheObjectType(), dataBuf, off, len, data, 0);

        off += len;
        len = IncompleteCacheObject.HEAD_LEN - len + data.length;

        // Write rest data.
        assertThat(CacheObjectAdapter.putValue(obj.cacheObjectType(), dataBuf, off, len, data, 0), is(true));

        assertThat("Not all data were written.", !dataBuf.hasRemaining(), is(true));

        dataBuf.clear();

        // Cut on header for reading.
        dataBuf.limit(3);

        final IncompleteCacheObject incompleteObj = objectShadow ?
            new IncompleteCacheObjectShadow(dataBuf) : new IncompleteCacheObject(dataBuf);

        incompleteObj.readData(dataBuf);

        assertThat(!incompleteObj.isReady(), is(true));

        assertThat("Data were read incorrectly.", !dataBuf.hasRemaining(), is(true));

        // Make rest data available.
        dataBuf.limit(dataBuf.capacity());

        incompleteObj.readData(dataBuf);

        assert incompleteObj.isReady();

        // Check that cache object data assembled correctly.
        assertEquals(obj.cacheObjectType(), incompleteObj.type());

        if (!objectShadow)
            Assert.assertArrayEquals(data, incompleteObj.data());
    }

    /**
     *
     */
    private static class TestCacheObject implements CacheObject {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final byte type;

        /**
         * @param type Cache object type.
         */
        private TestCacheObject(final byte type) {
            this.type = type;
        }

        /** {@inheritDoc} */
        @Nullable @Override public <T> T value(final CacheObjectValueContext ctx, final boolean cpy) {
            return value(ctx, cpy, null);
        }

        /** {@inheritDoc} */
        @Nullable @Override public <T> T value(final CacheObjectValueContext ctx, final boolean cpy, ClassLoader ldr) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public byte[] valueBytes(final CacheObjectValueContext ctx) throws IgniteCheckedException {
            return new byte[0];
        }

        /** {@inheritDoc} */
        @Override public int valueBytesLength(final CacheObjectContext ctx) throws IgniteCheckedException {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public boolean putValue(final ByteBuffer buf) throws IgniteCheckedException {
            return false;
        }

        /** {@inheritDoc} */
        @Override public int putValue(long addr) throws IgniteCheckedException {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public boolean putValue(final ByteBuffer buf, final int off, final int len)
            throws IgniteCheckedException {
            return false;
        }

        /** {@inheritDoc} */
        @Override public byte cacheObjectType() {
            return type;
        }

        /** {@inheritDoc} */
        @Override public boolean isPlatformType() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public CacheObject prepareForCache(final CacheObjectContext ctx, boolean compress) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void finishUnmarshal(final CacheObjectValueContext ctx, final ClassLoader ldr)
            throws IgniteCheckedException {
            // No-op
        }

        /** {@inheritDoc} */
        @Override public void prepareMarshal(final CacheObjectValueContext ctx) throws IgniteCheckedException {
            // No-op
        }

        /** {@inheritDoc} */
        @Override public boolean writeTo(final ByteBuffer buf, final MessageWriter writer) {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean readFrom(final ByteBuffer buf, final MessageReader reader) {
            return false;
        }

        /** {@inheritDoc} */
        @Override public short directType() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public byte fieldsCount() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public void onAckReceived() {
            // No-op
        }
    }
}
