/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.binary;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.ignite.internal.binary.streams.BinaryInputStream;
import org.jetbrains.annotations.Nullable;

/** */
public class BinaryReaderExPool {
    /** */
    private final BlockingQueue<BinaryReaderExImpl> readers = new LinkedBlockingQueue<>(100);

    {
        for (int i = 0; i < 100; i++)
            readers.add(new BinaryReaderExImpl());
    }

    /** */
    public BinaryReaderExImpl getReader(BinaryContext ctx, BinaryInputStream in, ClassLoader ldr, boolean forUnmarshal) {
        return getReader(ctx,
            in,
            ldr,
            null,
            forUnmarshal);
    }

    /** */
    public BinaryReaderExImpl getReader(
        BinaryContext ctx,
        BinaryInputStream in,
        ClassLoader ldr,
        @Nullable BinaryReaderHandles hnds,
        boolean forUnmarshal
    ) {
        return getReader(ctx,
            in,
            ldr,
            hnds,
            false,
            forUnmarshal);
    }

    /** */
    public BinaryReaderExImpl getReader(
        BinaryContext ctx,
        BinaryInputStream in,
        ClassLoader ldr,
        @Nullable BinaryReaderHandles hnds,
        boolean skipHdrCheck,
        boolean forUnmarshal
    ) {
        BinaryReaderExImpl reader = readers.poll();
        if (reader == null)
            reader = new BinaryReaderExImpl();

        reader.restart(ctx, in, ldr, hnds, skipHdrCheck, forUnmarshal);

        return reader;
    }

    /** */
    public void offer(BinaryReaderExImpl reader) {
        readers.offer(reader);
    }
}
