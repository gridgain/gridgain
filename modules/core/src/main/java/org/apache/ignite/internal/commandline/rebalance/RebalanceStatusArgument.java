/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.commandline.rebalance;

public class RebalanceStatusArgument {
    /**
     * Flag that show is rebalance will display also by group.
     */
    private boolean rebCacheView;

    /**
     * @param rebCacheView Shoe chaches flag.
     */
    public RebalanceStatusArgument(boolean rebCacheView) {
        this.rebCacheView = rebCacheView;
    }

    /**
     * @return Show caches flag.
     */
    public boolean isRebCacheView() {
        return rebCacheView;
    }

    /**
     * Builder of {@link RebalanceStatusArgument}.
     */
    public static class Builder {
        /**
         * Flag that show is rebalance will display also by group.
         */
        private boolean rebCacheView;

        /**
         * @param cmd Command.
         */
        public Builder() {
        }

        /**
         * @param rebCacheView Show caches flag.
         */
        public void setRebCacheView(boolean rebCacheView) {
            this.rebCacheView = rebCacheView;
        }

        /**
         * @return {@link BaselineArguments}.
         */
        public RebalanceStatusArgument build() {
            return new RebalanceStatusArgument(rebCacheView);
        }
    }
}
