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

package org.apache.ignite.internal.processors.query.aware;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import org.apache.ignite.internal.util.typedef.internal.S;

import static org.apache.ignite.internal.processors.query.aware.IndexRebuildState.State.INIT;

/**
 * State of rebuilding indexes for the cache.
 */
public class IndexRebuildState {
    /**
     * Enumeration of index rebuild states.
     */
    enum State {
        /** Initial state. */
        INIT,

        /** Completed. */
        COMPLETED,

        /** To be deleted. */
        DELETE
    }

    /** Index rebuild state state atomic updater. */
    private static final AtomicReferenceFieldUpdater<IndexRebuildState, State> STATE_UPDATER =
        AtomicReferenceFieldUpdater.newUpdater(IndexRebuildState.class, State.class, "state");

    /** Persistent cache. */
    private final boolean persistent;

    /** Index rebuild state. */
    private volatile State state = INIT;

    /**
     * Constructor.
     *
     * @param persistent Persistent cache.
     */
    public IndexRebuildState(boolean persistent) {
        this.persistent = persistent;
    }

    /**
     * Checking if the cache is persistent.
     *
     * @return {@code True} if persistent.
     */
    public boolean persistent() {
        return persistent;
    }

    /**
     * Getting the state rebuild of indexes.
     *
     * @return Current state.
     */
    public State state() {
        return state;
    }

    /**
     * Setting the state rebuild of the indexes.
     *
     * @param state New state.
     */
    public void state(State state) {
        this.state = state;
    }

    /**
     * Atomically sets of the state rebuild of the indexes.
     *
     * @param exp Expected state.
     * @param newState New state.
     * @return {@code True} if successful.
     */
    public boolean state(State exp, State newState) {
        return STATE_UPDATER.compareAndSet(this, exp, newState);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IndexRebuildState.class, this);
    }
}
