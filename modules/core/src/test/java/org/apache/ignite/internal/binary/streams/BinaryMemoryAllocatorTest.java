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

package org.apache.ignite.internal.binary.streams;

import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.getFieldValue;

/**
 * BinaryMemoryAllocator self test.
 */
@WithSystemProperty(key = IgniteSystemProperties.IGNITE_MARSHAL_BUFFERS_RECHECK, value = "500")
public class BinaryMemoryAllocatorTest extends GridCommonAbstractTest {
    private static final int SMALL_OBJECT_SIZE = 1024;

    private static final int LARGE_OBJECT_SIZE = 8 * 1024;

    /**
     * Ensure thread-local allocator shrinks cached buffer eventually.
     */
    @Test
    public void threadLocalBufferCompaction() throws Exception {
        resetThreadLocalBuffer();

        BinaryMemoryAllocatorChunk buffer = threadLocalMemoryAllocatorBuffer();

        // Allocate large object.
        allocateThenRelease(buffer, LARGE_OBJECT_SIZE);

        assertTrue(bufferSize(buffer) >= LARGE_OBJECT_SIZE);

        // Wait for IGNITE_MARSHAL_BUFFERS_RECHECK
        Thread.sleep(1_000);

        // Store small object and expect thread-local buffer shrinks.
        allocateThenRelease(buffer, SMALL_OBJECT_SIZE);

        assertTrue(bufferSize(buffer) < LARGE_OBJECT_SIZE);
    }

    /**
     * Ensure thread-local buffer cached.
     */
    @Test
    public void threadLocalBufferCaching() throws Exception {
        resetThreadLocalBuffer();

        BinaryMemoryAllocatorChunk buffer = threadLocalMemoryAllocatorBuffer();

        // Store large object and expect thread-local buffer expands.
        allocateThenRelease(buffer, LARGE_OBJECT_SIZE);

        int largeBufferSize = bufferSize(buffer);

        assertTrue(largeBufferSize >= LARGE_OBJECT_SIZE);

        // Ensure buffer reused for smaller objects. No immediate shrinking effect.
        allocateThenRelease(buffer, SMALL_OBJECT_SIZE);

        assertEquals(largeBufferSize, bufferSize(buffer));

        // Wait for IGNITE_MARSHAL_BUFFERS_RECHECK
        Thread.sleep(500);

        // Store smaller object, but large enough to prevents thread-local buffer from shrinking.
        allocateThenRelease(buffer, 1 + largeBufferSize / 2);

        assertEquals(largeBufferSize, bufferSize(buffer));
    }

    /**
     * Ensure thread-local buffer expanding.
     */
    @Test
    public void threadLocalBufferExpanding() {
        resetThreadLocalBuffer();

        BinaryMemoryAllocatorChunk buffer = threadLocalMemoryAllocatorBuffer();

        allocateThenRelease(buffer, SMALL_OBJECT_SIZE);

        int smallBufferSize = bufferSize(buffer);

        allocateThenRelease(buffer, LARGE_OBJECT_SIZE);

        assertTrue(smallBufferSize < bufferSize(buffer));
    }

    /**
     * Ensure thread-local buffer reusing.
     */
    @Test
    public void binaryMemoryAllocatorChunk() {
        resetThreadLocalBuffer();

        BinaryMemoryAllocatorChunk buffer = threadLocalMemoryAllocatorBuffer();

        // Acquire buffer.
        byte[] data = buffer.allocate(SMALL_OBJECT_SIZE);
        assertTrue(buffer.isAcquired());

        // Ensure unreleased buffer can't be reused.
        byte[] anotherData = buffer.allocate(SMALL_OBJECT_SIZE);
        assertNotSame(data, anotherData);

        // Unmanaged buffer can't be released.
        buffer.release(anotherData, SMALL_OBJECT_SIZE);
        assertTrue(buffer.isAcquired());

        assertSame(data, getFieldValue(buffer, "data"));

        // Reallocated new buffer.
        byte[] reallocated = buffer.reallocate(data, LARGE_OBJECT_SIZE);
        assertNotSame(data, reallocated);

        // Now old buffer is unmanaged and release has no effect.
        buffer.release(data, LARGE_OBJECT_SIZE);

        assertTrue(buffer.isAcquired());
        assertSame(reallocated, getFieldValue(buffer, "data"));

        // Release managed buffer.
        buffer.release(reallocated, LARGE_OBJECT_SIZE);

        assertFalse(buffer.isAcquired());
    }

    private void allocateThenRelease(BinaryMemoryAllocatorChunk buffer, int size) {
        byte[] data = buffer.allocate(size);
        buffer.release(data, size);
    }

    private static void resetThreadLocalBuffer() {
        ThreadLocal<?> holders = getFieldValue(BinaryMemoryAllocator.THREAD_LOCAL, "holders");

        holders.remove();
    }

    private static int bufferSize(BinaryMemoryAllocatorChunk chunk) {
        return ((byte[])getFieldValue(chunk, "data")).length;
    }

    private static BinaryMemoryAllocatorChunk threadLocalMemoryAllocatorBuffer() {
        return BinaryMemoryAllocator.THREAD_LOCAL.chunk();
    }
}
