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

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Handshake request from Web Console Agent.
 */
public class AgentHandshakeRequest {
    /** */
    private boolean disableDemo;

    /** */
    private String ver;

    /** */
    private String buildTime;

    /** */
    @GridToStringInclude
    private Set<String> toks;

    /**
     * Default constructor for serialization.
     */
    public AgentHandshakeRequest() {
        // No-op.
    }

    /**
     * Full constructor.
     *
     * @param disableDemo Disable demo flag.
     * @param ver Agent version.
     * @param buildTime Agent build time.
     * @param toks Tokens.
     */
    public AgentHandshakeRequest(
        boolean disableDemo,
        String ver,
        String buildTime,
        Collection<String> toks
    ) {
        this.disableDemo = disableDemo;
        this.ver = ver;
        this.buildTime = buildTime;
        this.toks = new HashSet<>(toks);
    }

    /**
     * @return Disable demo flag.
     */
    public boolean isDisableDemo() {
        return disableDemo;
    }

    /**
     * @param disableDemo Disable demo flag.
     */
    public void setDisableDemo(boolean disableDemo) {
        this.disableDemo = disableDemo;
    }

    /**
     * @return Agent version.
     */
    public String getVersion() {
        return ver;
    }

    /**
     * @param ver Agent version.
     */
    public void setVersion(String ver) {
        this.ver = ver;
    }

    /**
     * @return Agent built time.
     */
    public String getBuildTime() {
        return buildTime;
    }

    /**
     * @param buildTime Agent built time.
     */
    public void setBuildTime(String buildTime) {
        this.buildTime = buildTime;
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
        return S.toString(AgentHandshakeRequest.class, this);
    }
}
