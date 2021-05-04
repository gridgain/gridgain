/*
 * Copyright 2021 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.localtask;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.pendingtask.DurableBackgroundTask;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.pendingtask.DurableBackgroundTaskResult;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Simple {@link DurableBackgroundTask} implementation for tests.
 */
class SimpleTask extends IgniteDataTransferObject implements DurableBackgroundTask {
    /** Serial version UID. */
    private static final long serialVersionUID = 0L;

    /** Task name. */
    private String name;

    /** Future that will be completed at the beginning of the {@link #executeAsync}. */
    final GridFutureAdapter<Void> onExecFut = new GridFutureAdapter<>();

    /** Future that will be returned from the {@link #executeAsync}. */
    final GridFutureAdapter<DurableBackgroundTaskResult> taskFut = new GridFutureAdapter<>();

    /** Future that will be completed at the beginning of the {@link #cancel}. */
    final GridFutureAdapter<Void> onCancelFut = new GridFutureAdapter<>();

    /**
     * Default constructor.
     */
    public SimpleTask() {
    }

    /**
     * Constructor.
     *
     * @param name Task name.
     */
    public SimpleTask(String name) {
        this.name = name;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public void cancel() {
        onCancelFut.onDone();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<DurableBackgroundTaskResult> executeAsync(GridKernalContext ctx) {
        onExecFut.onDone();

        return taskFut;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeLongString(out, name);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(
        byte protoVer,
        ObjectInput in
    ) throws IOException, ClassNotFoundException {
        name = U.readLongString(in);
    }

    /**
     * Resetting internal futures.
     */
    void reset() {
        onExecFut.reset();
        taskFut.reset();
    }
}
