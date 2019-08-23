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
import org.apache.ignite.internal.sql.calcite.plan.PlanStep;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * TODO: Add class description.
 */
public class QueryRequest implements Message, GridCacheQueryMarshallable {

    private UUID qryNode;

    private long qryId;

    @GridDirectTransient
    private List<PlanStep> globalPlan;

    private byte[] planBytes;

    public QueryRequest() {
    }

    public QueryRequest(T2<UUID, Long> id, List<PlanStep> plan) {
        qryNode = id.getKey();
        qryId = id.getValue();
        globalPlan = plan;
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
                if (!writer.writeByteArray("planBytes", planBytes))
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

        }

        return true;
    }

    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        if (!reader.beforeMessageRead())
            return false;

        switch (reader.state()) {
            case 0:
                planBytes = reader.readByteArray("planBytes");

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

        }

        return reader.afterMessageRead(QueryRequest.class);
    }

    @Override public short directType() {
        return 175;
    }

    @Override public byte fieldsCount() {
        return 3;
    }

    @Override public void onAckReceived() {
    }

    @Override public void marshall(Marshaller m) {
        try {
            planBytes = U.marshal(m, globalPlan);
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
                globalPlan = ((BinaryMarshaller)m).binaryMarshaller().unmarshal(planBytes, ldr);
            else
                globalPlan = U.unmarshal(m, planBytes, ldr);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    @Override public String toString() {
        return "QueryRequest{" +
            "qryId=" + qryId +
            ", globalPlan=" + globalPlan +
            '}';
    }
}
