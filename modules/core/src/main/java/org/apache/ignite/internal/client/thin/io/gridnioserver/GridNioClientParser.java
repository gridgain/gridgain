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

package org.apache.ignite.internal.client.thin.io.gridnioserver;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.ignite.internal.client.thin.io.ClientMessageDecoder;
import org.apache.ignite.internal.util.nio.GridNioParser;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.internal.util.nio.GridNioSessionMetaKey;
import org.jetbrains.annotations.Nullable;

/**
 * Client message parser.
 */
class GridNioClientParser implements GridNioParser {
    /** */
    private static final int SES_META_DECODER = GridNioSessionMetaKey.nextUniqueKey();

    /** {@inheritDoc} */
    @Override public @Nullable Object decode(GridNioSession ses, ByteBuffer buf) {
        ClientMessageDecoder decoder = ses.meta(SES_META_DECODER);

        if (decoder == null) {
            decoder = new ClientMessageDecoder();

            ses.addMeta(SES_META_DECODER, decoder);
        }

        byte[] bytes = decoder.apply(buf);

        if (bytes == null)
            return null; // Message is not yet completely received.

        // Thin client protocol is little-endian. ByteBuffer will handle conversion as necessary on big-endian systems.
        return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
    }

    /** {@inheritDoc} */
    @Override public ByteBuffer encode(GridNioSession ses, Object msg) {
        return (ByteBuffer)msg;
    }
}
