/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 * 
 * Commons Clause Restriction
 * 
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 * 
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 * 
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.commandline.baseline;

import java.util.List;

/**
 * This class contains all possible arguments after parsing baseline command input.
 */
public class BaselineArguments {
    /** Command. */
    private BaselineCommand cmd;
    /**
     * {@code true} if auto-adjust should be enable, {@code false} if it should be disable, {@code null} if no operation
     * needed.
     */
    private Boolean enableAutoAdjust;
    /** New value of soft timeout. */
    private Long softBaselineTimeout;
    /** Requested topology version. */
    private long topVer = -1;
    /** List of consistent ids for operation. */
    List<String> consistentIds;

    /**
     * @param cmd Command.
     * @param enableAutoAdjust Auto-adjust enabled feature.
     * @param softBaselineTimeout New value of soft timeout.
     * @param topVer Requested topology version.
     * @param consistentIds List of consistent ids for operation.
     */
    public BaselineArguments(BaselineCommand cmd, Boolean enableAutoAdjust, Long softBaselineTimeout,
        long topVer, List<String> consistentIds) {
        this.cmd = cmd;
        this.enableAutoAdjust = enableAutoAdjust;
        this.softBaselineTimeout = softBaselineTimeout;
        this.topVer = topVer;
        this.consistentIds = consistentIds;
    }

    /**
     * @return Command.
     */
    public BaselineCommand getCmd() {
        return cmd;
    }

    /**
     * @return {@code true} if auto-adjust should be enable, {@code false} if it should be disable, {@code null} if no
     * operation needed.
     */
    public Boolean getEnableAutoAdjust() {
        return enableAutoAdjust;
    }

    /**
     * @return New value of soft timeout.
     */
    public Long getSoftBaselineTimeout() {
        return softBaselineTimeout;
    }

    /**
     * @return Requested topology version.
     */
    public long getTopVer() {
        return topVer;
    }

    /**
     * @return List of consistent ids for operation.
     */
    public List<String> getConsistentIds() {
        return consistentIds;
    }

    /**
     * Builder of {@link BaselineArguments}.
     */
    public static class Builder {
        /** Command. */
        private BaselineCommand cmd;
        /**
         * {@code true} if auto-adjust should be enable, {@code false} if it should be disable, {@code null} if no
         * operation needed.
         */
        private Boolean enable;
        /** New value of soft timeout. */
        private Long timeout;
        /** Requested topology version. */
        private long ver = -1;
        /** List of consistent ids for operation. */
        private List<String> ids;

        /**
         * @param cmd Command.
         */
        public Builder(BaselineCommand cmd) {
            this.cmd = cmd;
        }

        /**
         * @param enable {@code true} if auto-adjust should be enable, {@code false} if it should be disable, {@code
         * null} if no operation needed.
         * @return This instance for chaining.
         */
        public Builder withEnable(Boolean enable) {
            this.enable = enable;

            return this;
        }

        /**
         * @param timeout New value of soft timeout.
         * @return This instance for chaining.
         */
        public Builder withSoftBaselineTimeout(Long timeout) {
            this.timeout = timeout;

            return this;
        }

        /**
         * @param ver Requested topology version.
         * @return This instance for chaining.
         */
        public Builder withTopVer(long ver) {
            this.ver = ver;

            return this;
        }

        /**
         * @param ids List of consistent ids for operation.
         * @return This instance for chaining.
         */
        public Builder withConsistentIds(List<String> ids) {
            this.ids = ids;

            return this;
        }

        /**
         * @return {@link BaselineArguments}.
         */
        public BaselineArguments build() {
            return new BaselineArguments(cmd, enable, timeout, ver, ids);
        }
    }
}
