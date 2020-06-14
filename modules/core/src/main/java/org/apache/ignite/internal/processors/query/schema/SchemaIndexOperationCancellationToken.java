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

package org.apache.ignite.internal.processors.query.schema;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteInClosure;

/**
 * Index operation cancellation token.
 * Implements two futures chan operations.
 * In case of {@code startCancelFut} starts completion, appropriate listener completes all current index opearations.
 * While all index operations are canceled {@code finishCancelFut.onDone()} is called and appropriate listener
 * completes.
 */
public class SchemaIndexOperationCancellationToken {
    /** Start future chain. */
    GridFutureAdapter startCancelFut = new GridFutureAdapter();

    /** End future chain. */
    GridFutureAdapter finishCancelFut = new GridFutureAdapter();

    /** Logger. */
    private final IgniteLogger log;

    /** Constructor. */
    public SchemaIndexOperationCancellationToken(GridKernalContext kctx) {
        log = kctx.log(SchemaIndexOperationCancellationToken.class);
    }

    /** Start cancel chain. */
    public void startCancel() {
        startCancelFut.onDone();
    }

    /** Finish cancel chain. */
    public void finishCancel() {
        finishCancelFut.onDone();
    }

    /** Start cancel listener.
     * @param lsnr Listener.
     **/
    public void listenStartCancel(IgniteInClosure lsnr) {
        startCancelFut.listen(lsnr);
    }

    /** Finish cancel listener.
     *  Syncroniously waits the start future and further call to appropriate listener.
     *
     *  @param lsnr Listener.
     */
    public void listenFinishCancel(IgniteInClosure lsnr) {
        try {
            finishCancelFut.get();
        }
        catch (IgniteCheckedException e) {
            log.error("Error occured while waiting the finish future.", e);
        }

        finishCancelFut.listen(lsnr);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SchemaIndexOperationCancellationToken.class, this);
    }
}
