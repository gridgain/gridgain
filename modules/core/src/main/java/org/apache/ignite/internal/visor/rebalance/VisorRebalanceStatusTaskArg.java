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

package org.apache.ignite.internal.visor.rebalance;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;

/**
 * Argument of rebalance status task.
 */
public class VisorRebalanceStatusTaskArg extends IgniteDataTransferObject {
    /**
     * Flag that show is rebalance will display also by group.
     */
    private boolean rebCacheView;

    /**
     * @return Flag of show groups in result.
     */
    public boolean isRebCacheView() {
        return rebCacheView;
    }

    /**
     * Default constructor.
     */
    public VisorRebalanceStatusTaskArg() {
    }

    /**
     * @param rebCacheView Chaces view flag.
     */
    public VisorRebalanceStatusTaskArg(boolean rebCacheView) {
        this.rebCacheView = rebCacheView;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeBoolean(rebCacheView);

    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        rebCacheView = in.readBoolean();
    }
}
