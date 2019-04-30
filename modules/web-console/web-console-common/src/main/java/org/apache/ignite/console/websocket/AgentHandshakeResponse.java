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

package org.apache.ignite.console.websocket;

import java.util.Set;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Handshake request for Web Console Agent.
 */
public class AgentHandshakeResponse {
    /** */
    private String err;

    /** */
    @GridToStringInclude
    private Set<String> toks;

    /**
     * Default constructor for serialization.
     */
    public AgentHandshakeResponse() {
        // No-op.
    }

    /**
     * @param err Error message.
     */
    public AgentHandshakeResponse(String err) {
        this.err = err;
    }

    /**
     * @param toks Tokens.
     */
    public AgentHandshakeResponse(Set<String> toks) {
        this.toks = toks;
    }

    /**
     * @return Error message.
     */
    public String getError() {
        return err;
    }

    /**
     * @param err Error message.
     */
    public void setError(String err) {
        this.err = err;
    }

    /**
     * @return Tokens.
     */
    public Set<String> getTokens() {
        return toks;
    }

    /**
     * @param toks Tokens.
     */
    public void setTokens(Set<String> toks) {
        this.toks = toks;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(AgentHandshakeResponse.class, this);
    }
}
