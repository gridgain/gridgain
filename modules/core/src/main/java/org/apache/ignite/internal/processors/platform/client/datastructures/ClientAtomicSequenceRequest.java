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

package org.apache.ignite.internal.processors.platform.client.datastructures;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.configuration.AtomicConfiguration;
import org.apache.ignite.internal.processors.datastructures.GridCacheAtomicSequenceImpl;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientRequest;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;
import org.apache.ignite.internal.processors.platform.client.ClientStatus;

/**
 * Atomic sequence request.
 */
public class ClientAtomicSequenceRequest extends ClientRequest {
    /** Atomic long name. */
    private final String name;

    /** Cache group name. */
    private final String groupName;

    /**
     * Constructor.
     *
     * @param reader Reader.
     */
    public ClientAtomicSequenceRequest(BinaryRawReader reader) {
        super(reader);

        name = reader.readString();
        groupName = reader.readString();
    }

    /**
     * Gets the atomic sequence.
     *
     * @param ctx Context.
     * @return Atomic sequence or null.
     */
    protected GridCacheAtomicSequenceImpl atomicSequence(ClientConnectionContext ctx) {
        AtomicConfiguration cfg = groupName == null ? null : new AtomicConfiguration().setGroupName(groupName);

        try {
            return (GridCacheAtomicSequenceImpl)ctx.kernalContext().dataStructures().sequence(name, cfg, 0, false);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e.getMessage(), e);
        }
    }

    /**
     * Gets a response for non-existent atomic sequence.
     *
     * @return Response for non-existent atomic sequence.
     */
    protected ClientResponse notFoundResponse() {
        return new ClientResponse(
                requestId(),
                ClientStatus.RESOURCE_DOES_NOT_EXIST,
                String.format("AtomicSequence with name '%s' does not exist.", name));
    }
}
