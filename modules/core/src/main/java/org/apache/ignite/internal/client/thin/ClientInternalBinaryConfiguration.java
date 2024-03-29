/*
 * Copyright 2022 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.client.thin;

import org.apache.ignite.internal.binary.streams.BinaryInputStream;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Cluster binary configuration.
 */
class ClientInternalBinaryConfiguration {
    /** */
    private final boolean compactFooter;

    /** */
    private final BinaryNameMapperMode binaryNameMapperMode;

    /**
     * Constructor.
     *
     * @param stream Stream.
     */
    public ClientInternalBinaryConfiguration(BinaryInputStream stream) {
        compactFooter = stream.readBoolean();
        binaryNameMapperMode = BinaryNameMapperMode.fromOrdinal(stream.readByte());
    }

    /**
     * @return Compact footer.
     */
    public boolean compactFooter() {
        return compactFooter;
    }

    /**
     * @return Name mapper mode.
     */
    public BinaryNameMapperMode binaryNameMapperMode() {
        return binaryNameMapperMode;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ClientInternalBinaryConfiguration.class, this);
    }
}
