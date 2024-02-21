/*
 * Copyright 2024 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.platform.client.binary;

import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.internal.binary.BinaryMetadata;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.binary.BinaryTypeImpl;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;
import org.apache.ignite.internal.processors.platform.utils.PlatformUtils;

import java.util.Collection;

public class ClientBinaryTypesGetResponse extends ClientResponse {
    private final Collection<BinaryType> types;

    /**
     * Constructor.
     *
     * @param reqId Request id.
     * @param types Types.
     */
    ClientBinaryTypesGetResponse(long reqId, Collection<BinaryType> types) {
        super(reqId);

        this.types = types;
    }

    @Override
    public void encode(ClientConnectionContext ctx, BinaryRawWriterEx writer) {
        super.encode(ctx, writer);

        writer.writeInt(types.size());

        for (BinaryType type : types) {
            BinaryTypeImpl typeImpl = (BinaryTypeImpl)type;
            BinaryMetadata meta = typeImpl.metadata();
            PlatformUtils.writeBinaryMetadata(writer, meta, false);
        }
    }
}
