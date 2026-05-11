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

package org.apache.ignite.internal.processors.rest.request;

import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Request for the {@code DRAIN} REST command. Carries the {@code action}
 * sub-command per HLD v14 §3: {@code "start"}, {@code "stop"}, or
 * {@code "status"}.
 */
public class GridRestDrainRequest extends GridRestRequest {
    /** Sub-action: {@code "start"}, {@code "stop"}, or {@code "status"}. */
    private String action;

    /**
     * @return Sub-action.
     */
    public String action() {
        return action;
    }

    /**
     * @param action Sub-action.
     */
    public void action(String action) {
        this.action = action;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridRestDrainRequest.class, this, super.toString());
    }
}
