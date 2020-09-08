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

package org.apache.ignite.internal.processors.platform.client.tx;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientRequest;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;
import org.apache.ignite.internal.processors.platform.client.ClientStatus;
import org.apache.ignite.internal.processors.platform.client.IgniteClientException;
import org.apache.ignite.internal.transactions.IgniteTxTimeoutCheckedException;

/**
 * End the transaction request.
 */
public class ClientTxEndRequest extends ClientRequest {
    /** Transaction id. */
    private final int txId;

    /** Transaction committed. */
    private final boolean committed;

    /**
     * Constructor.
     *
     * @param reader Reader.
     */
    public ClientTxEndRequest(BinaryRawReader reader) {
        super(reader);

        txId = reader.readInt();
        committed = reader.readBoolean();
    }

    /** {@inheritDoc} */
    @Override public ClientResponse process(ClientConnectionContext ctx) {
        ClientTxContext txCtx = ctx.txContext(txId);

        if (txCtx == null && !committed)
            return super.process(ctx);

        if (txCtx == null)
            throw new IgniteClientException(ClientStatus.TX_NOT_FOUND, "Transaction with id " + txId + " not found.");

        try {
            txCtx.acquire(committed);

            try (GridNearTxLocal tx = txCtx.tx()) {
                if (committed)
                    tx.commit();
                else
                    tx.rollback();
            }
        }
        catch (IgniteTxTimeoutCheckedException e) {
            throw new IgniteClientException(ClientStatus.TX_TIMED_OUT, e.getMessage(), e);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteClientException(ClientStatus.FAILED, e.getMessage(), e);
        }
        finally {
            ctx.removeTxContext(txId);

            try {
                txCtx.release(false);
            }
            catch (IgniteCheckedException ignore) {
                // No-op.
            }
        }

        return super.process(ctx);
    }
}
