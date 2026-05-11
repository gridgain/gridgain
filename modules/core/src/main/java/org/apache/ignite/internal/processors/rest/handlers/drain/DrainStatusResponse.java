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

package org.apache.ignite.internal.processors.rest.handlers.drain;

/**
 * JSON response payload for {@code cmd=drain&action=status}. JavaBean shape so
 * Jackson serializes the public getters into the {@code response} field of
 * the surrounding {@code GridRestResponse}. Recognized by
 * {@code GridJettyRestHandler} to emit the {@code X-Active-Thin-Clients}
 * response header for shell-parseable preStop hooks.
 */
public class DrainStatusResponse {
    /** Whether the drain flag is currently set on this pod. */
    private boolean draining;

    /** Active thin-client connection count at the moment this response was built. */
    private int activeThinClients;

    /**
     * Default constructor for Jackson.
     */
    public DrainStatusResponse() {
        // No-op.
    }

    /**
     * @param draining Drain flag.
     * @param activeThinClients Active thin-client connection count.
     */
    public DrainStatusResponse(boolean draining, int activeThinClients) {
        this.draining = draining;
        this.activeThinClients = activeThinClients;
    }

    /**
     * @return Drain flag.
     */
    public boolean isDraining() {
        return draining;
    }

    /**
     * @param draining Drain flag.
     */
    public void setDraining(boolean draining) {
        this.draining = draining;
    }

    /**
     * @return Active thin-client connection count.
     */
    public int getActiveThinClients() {
        return activeThinClients;
    }

    /**
     * @param activeThinClients Active thin-client connection count.
     */
    public void setActiveThinClients(int activeThinClients) {
        this.activeThinClients = activeThinClients;
    }
}
