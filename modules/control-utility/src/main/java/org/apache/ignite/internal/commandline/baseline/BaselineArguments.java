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

package org.apache.ignite.internal.commandline.baseline;

import java.util.List;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * This class contains all possible arguments after parsing baseline command input.
 */
public class BaselineArguments {
    /** Command. */
    private BaselineSubcommands cmd;

    /**
     * {@code true} if auto-adjust should be enable, {@code false} if it should be disable, {@code null} if no operation
     * needed.
     */
    private Boolean enableAutoAdjust;

    /** New value of soft timeout. */
    private Long softBaselineTimeout;

    /**
     * {@code true} if scale up auto-adjust should be enable, {@code false} if it should be disable, {@code null} if no operation
     * needed.
     */
    private Boolean scaleUpEnableAutoAdjust;

    /** New value of scale up soft timeout. */
    private Long scaleUpSoftBaselineTimeout;

    /**
     * {@code true} if scale down auto-adjust should be enable, {@code false} if it should be disable, {@code null} if no operation
     * needed.
     */
    private Boolean scaleDownEnableAutoAdjust;

    /** New value of scale down soft timeout. */
    private Long scaleDownSoftBaselineTimeout;

    /** Requested topology version. */
    private long topVer = -1;

    /** List of consistent ids for operation. */
    @GridToStringInclude
    List<String> consistentIds;

    /**
     * @param cmd Command.
     * @param enableAutoAdjust Auto-adjust enabled feature.
     * @param softBaselineTimeout New value of soft timeout.
     * @param topVer Requested topology version.
     * @param consistentIds List of consistent ids for operation.
     */
    public BaselineArguments(
        BaselineSubcommands cmd,
        Boolean enableAutoAdjust,
        Long softBaselineTimeout,
        Boolean scaleUpEnableAutoAdjust,
        Long scaleUpSoftBaselineTimeout,
        Boolean scaleDownEnableAutoAdjust,
        Long scaleDownSoftBaselineTimeout,
        long topVer,
        List<String> consistentIds
    ) {
        this.cmd = cmd;
        this.enableAutoAdjust = enableAutoAdjust;
        this.softBaselineTimeout = softBaselineTimeout;
        this.scaleUpEnableAutoAdjust = scaleUpEnableAutoAdjust;
        this.scaleUpSoftBaselineTimeout = scaleUpSoftBaselineTimeout;
        this.scaleDownEnableAutoAdjust = scaleDownEnableAutoAdjust;
        this.scaleDownSoftBaselineTimeout = scaleDownSoftBaselineTimeout;
        this.topVer = topVer;
        this.consistentIds = consistentIds;
    }

    /**
     * @return Command.
     */
    public BaselineSubcommands getCmd() {
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
     * @return {@code true} if scale up auto-adjust should be enable, {@code false} if it should be disable, {@code null} if no
     * operation needed.
     */
    public Boolean getScaleUpEnableAutoAdjust() {
        return scaleUpEnableAutoAdjust;
    }

    /**
     * @return New value of scale up soft timeout.
     */
    public Long getScaleUpSoftBaselineTimeout() {
        return scaleUpSoftBaselineTimeout;
    }

    /**
     * @return {@code true} if scale down auto-adjust should be enable, {@code false} if it should be disable, {@code null} if no
     * operation needed.
     */
    public Boolean getScaleDownEnableAutoAdjust() {
        return scaleDownEnableAutoAdjust;
    }

    /**
     * @return New value of scale down soft timeout.
     */
    public Long getScaleDownSoftBaselineTimeout() {
        return scaleDownSoftBaselineTimeout;
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

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(BaselineArguments.class, this);
    }

    /**
     * Builder of {@link BaselineArguments}.
     */
    public static class Builder {
        /** Command. */
        private BaselineSubcommands cmd;

        /**
         * {@code true} if auto-adjust should be enable, {@code false} if it should be disable, {@code null} if no
         * operation needed.
         */
        private Boolean enable;

        /** New value of soft timeout. */
        private Long timeout;

        /**
         * {@code true} if scale up auto-adjust should be enable, {@code false} if it should be disable, {@code null} if no
         * operation needed.
         */
        private Boolean scaleUpEnable;

        /** New value of scale up soft timeout. */
        private Long scaleUpTimeout;

        /**
         * {@code true} if scale down auto-adjust should be enable, {@code false} if it should be disable, {@code null} if no
         * operation needed.
         */
        private Boolean scaleDownEnable;

        /** New value of scale down soft timeout. */
        private Long scaleDownTimeout;

        /** Requested topology version. */
        private long ver = -1;

        /** List of consistent ids for operation. */
        private List<String> ids;

        /**
         * @param cmd Command.
         */
        public Builder(BaselineSubcommands cmd) {
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
         * @param scaleUpEnable {@code true} if scale up auto-adjust should be enable, {@code false} if it should be disable, {@code
         * null} if no operation needed.
         * @return This instance for chaining.
         */
        public Builder withScaleUpEnable(Boolean scaleUpEnable) {
            this.scaleUpEnable = scaleUpEnable;

            return this;
        }

        /**
         * @param scaleUpTimeout New value of soft scale up timeout.
         * @return This instance for chaining.
         */
        public Builder withSoftBaselineScaleUpTimeout(Long scaleUpTimeout) {
            this.scaleUpTimeout = scaleUpTimeout;

            return this;
        }

        /**
         * @param scaleDownEnable {@code true} if scale down auto-adjust should be enable, {@code false} if it should be disable, {@code
         * null} if no operation needed.
         * @return This instance for chaining.
         */
        public Builder withScaleDownEnable(Boolean scaleDownEnable) {
            this.scaleDownEnable = scaleDownEnable;

            return this;
        }

        /**
         * @param scaleDownTimeout New value of soft scale down timeout.
         * @return This instance for chaining.
         */
        public Builder withSoftBaselineScaleDownTimeout(Long scaleDownTimeout) {
            this.scaleDownTimeout = scaleDownTimeout;

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
            return new BaselineArguments(cmd, enable, timeout, scaleUpEnable, scaleUpTimeout,
                scaleDownEnable, scaleDownTimeout, ver, ids);
        }
    }
}
