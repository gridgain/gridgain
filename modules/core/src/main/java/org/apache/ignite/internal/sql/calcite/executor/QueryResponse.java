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
package org.apache.ignite.internal.sql.calcite.executor;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryMarshallable;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * TODO: Add class description.
 */
public class QueryResponse  implements Message, GridCacheQueryMarshallable {

    private int linkId;

    private UUID qryNode;

    private long qryId;

    @GridDirectTransient
    private List<List<?>> result;

    private byte[] resultBytes;

    public QueryResponse() {
    }

    public QueryResponse(List<List<?>> result, int linkId, UUID qryNode, long qryId) {
        this.result = result;
        this.linkId = linkId;
        this.qryNode = qryNode;
        this.qryId = qryId;
    }

    @Override public void marshall(Marshaller m) {
        try {
            resultBytes = U.marshal(m, result);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    @Override public void unmarshall(Marshaller m, GridKernalContext ctx) {
        try {
            final ClassLoader ldr = U.resolveClassLoader(ctx.config());

            if (m instanceof BinaryMarshaller)
                // To avoid deserializing of enum types.
                result = ((BinaryMarshaller)m).binaryMarshaller().unmarshal(resultBytes, ldr);
            else
                result = U.unmarshal(m, resultBytes, ldr);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeInt("linkId", linkId))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeLong("qryId", qryId))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeUuid("qryNode", qryNode))
                    return false;

                writer.incrementState();

            case 3:
                if (!writer.writeByteArray("resultBytes", resultBytes))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        if (!reader.beforeMessageRead())
            return false;

        switch (reader.state()) {
            case 0:
                linkId = reader.readInt("linkId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                qryId = reader.readLong("qryId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                qryNode = reader.readUuid("qryNode");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 3:
                resultBytes = reader.readByteArray("resultBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(QueryResponse.class);
    }

    @Override public short directType() {
        return 176; // TODO: CODE: implement.
    }

    @Override public byte fieldsCount() {
        return 4;
    }

    @Override public void onAckReceived() {
    }

    public int linkId() {
        return linkId;
    }

    public UUID queryNode() {
        return qryNode;
    }

    public long queryId() {
        return qryId;
    }

    public List<List<?>> result() {
        return result;
    }

    @Override public String toString() {
        return "QueryResponse{" +
            "linkId=" + linkId +
            ", qryNode=" + qryNode +
            ", qryId=" + qryId +
            ", result=" + result +
            '}';
    }
}
