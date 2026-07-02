/*
 * Copyright 2026 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.rest.request;

import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Request for the {@code DRAIN} REST command: the {@code action} sub-command
 * ({@code start}/{@code stop}/{@code status}) and the optional {@code force} flag.
 */
public class GridRestDrainRequest extends GridRestRequest {
    /** Sub-command of {@code cmd=drain}; case-insensitive on the wire. */
    public enum Action {
        /** Set the drain flag. Auth required. */
        START("start"),

        /** Clear the drain flag. Auth required. */
        STOP("stop"),

        /** Read the drain flag. Auth-exempt. */
        STATUS("status");

        /** Wire-level key as it appears in the {@code action} query parameter. */
        private final String key;

        /**
         * @param key Wire-level key.
         */
        Action(String key) {
            this.key = key;
        }

        /**
         * @return Wire-level key.
         */
        public String key() {
            return key;
        }

        /**
         * Case-insensitive lookup by wire-level key.
         *
         * @param key Wire-level key (may be {@code null}).
         * @return Matching action, or {@code null} if {@code key} is absent or unknown.
         */
        @Nullable public static Action of(@Nullable String key) {
            if (key == null)
                return null;

            for (Action a : values())
                if (a.key.equalsIgnoreCase(key))
                    return a;

            return null;
        }
    }

    /** Sub-action. */
    private Action action;

    /** Whether to bypass the unique-data guard on {@code action=start}. */
    private boolean force;

    /**
     * @return Sub-action, or {@code null} if absent.
     */
    @Nullable public Action action() {
        return action;
    }

    /**
     * @param action Sub-action.
     */
    public void action(@Nullable Action action) {
        this.action = action;
    }

    /**
     * @return {@code force} flag.
     */
    public boolean force() {
        return force;
    }

    /**
     * @param force {@code force} flag.
     */
    public void force(boolean force) {
        this.force = force;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridRestDrainRequest.class, this, super.toString());
    }
}
