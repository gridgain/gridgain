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

package org.apache.ignite.internal.visor.baseline;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

/**
 * Argument for {@link VisorBaselineTask}.
 */
public class VisorBaselineTaskArg extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private VisorBaselineOperation op;

    /** */
    private long topVer;

    /** */
    private List<String> consistentIds;

    /** Baseline auto adjust enable flag. */
    private Boolean autoAdjustEnabled;

    /** Baseline scale up auto adjust enable flag. */
    private Boolean scaleUpAutoAdjustEnabled;

    /** Baseline scale down auto adjust enable flag. */
    private Boolean scaleDownAutoAdjustEnabled;

    /** Awaiting time of baseline auto adjust after last topology event in ms. */
    private Long autoAdjustAwaitingTime;

    /** Awaiting time of baseline scale up auto adjust after the last topology event in ms. */
    private Long scaleUpAutoAdjustAwaitingTime;

    /** Awaiting time of baseline scale down auto adjust after the last topology event in ms. */
    private Long scaleDownAutoAdjustAwaitingTime;

    /**
     * Default constructor.
     */
    public VisorBaselineTaskArg() {
        // No-op.
    }

    /**
     * This constructor is required by Web Console.
     * Do not remove or change signature.
     *
     * @param topVer Topology version.
     * @param consistentIds Consistent ids.
     */
    public VisorBaselineTaskArg(
        VisorBaselineOperation op,
        long topVer,
        List<String> consistentIds
    ) {
        this(op, topVer, consistentIds, null, null, null, null, null, null);
    }

    /**
     * @param topVer Topology version.
     * @param consistentIds Consistent ids.
     * @param autoAdjustEnabled Baseline auto adjust enable flag.
     * @param autoAdjustAwaitingTime Await time of baseline auto adjust after last topology event in ms.
     * @param scaleUpAutoAdjustEnabled Baseline scale up auto adjust enable flag.
     * @param scaleUpAutoAdjustAwaitingTime Await time of baseline scale up auto adjust after last topology event in ms.
     * @param scaleDownAutoAdjustEnabled Baseline scale down auto adjust enable flag.
     * @param scaleDownAutoAdjustAwaitingTime Await time of baseline scale down auto adjust after last topology event in ms.
     */
    public VisorBaselineTaskArg(
        VisorBaselineOperation op,
        long topVer,
        List<String> consistentIds,
        Boolean autoAdjustEnabled,
        Long autoAdjustAwaitingTime,
        Boolean scaleUpAutoAdjustEnabled,
        Long scaleUpAutoAdjustAwaitingTime,
        Boolean scaleDownAutoAdjustEnabled,
        Long scaleDownAutoAdjustAwaitingTime
    ) {
        this.op = op;
        this.topVer = topVer;
        this.consistentIds = consistentIds;
        this.autoAdjustEnabled = autoAdjustEnabled;
        this.autoAdjustAwaitingTime = autoAdjustAwaitingTime;
        this.scaleUpAutoAdjustEnabled = scaleUpAutoAdjustEnabled;
        this.scaleUpAutoAdjustAwaitingTime = scaleUpAutoAdjustAwaitingTime;
        this.scaleDownAutoAdjustEnabled = scaleDownAutoAdjustEnabled;
        this.scaleDownAutoAdjustAwaitingTime = scaleDownAutoAdjustAwaitingTime;
    }

    /**
     * @return Base line operation.
     */
    public VisorBaselineOperation getOperation() {
        return op;
    }

    /**
     * @return Topology version.
     */
    public long getTopologyVersion() {
        return topVer;
    }

    /**
     * @return Consistent IDs.
     */
    public List<String> getConsistentIds() {
        return consistentIds;
    }

    /** {@inheritDoc} */
    @Override public byte getProtocolVersion() {
        return V3;
    }

    /**
     * @return Baseline auto adjust enable flag.
     */
    public Boolean isAutoAdjustEnabled() {
        return autoAdjustEnabled;
    }

    /**
     * @return Await time of baseline auto adjust after last topology event in ms.
     */
    public Long getAutoAdjustAwaitingTime() {
        return autoAdjustAwaitingTime;
    }

    /**
     * @return Baseline scale up auto adjust enable flag.
     */
    public Boolean isScaleUpAutoAdjustEnabled() {
        return scaleUpAutoAdjustEnabled;
    }

    /**
     * @return Await time of baseline scale up auto adjust after last topology event in ms.
     */
    public Long getScaleUpAutoAdjustAwaitingTime() {
        return scaleUpAutoAdjustAwaitingTime;
    }

    /**
     * @return Baseline scale down auto adjust enable flag.
     */
    public Boolean isScaleDownAutoAdjustEnabled() {
        return scaleDownAutoAdjustEnabled;
    }

    /**
     * @return Await time of baseline scale down auto adjust after last topology event in ms.
     */
    public Long getScaleDownAutoAdjustAwaitingTime() {
        return scaleDownAutoAdjustAwaitingTime;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeEnum(out, op);
        out.writeLong(topVer);
        U.writeCollection(out, consistentIds);
        out.writeObject(autoAdjustEnabled);
        out.writeObject(autoAdjustAwaitingTime);
        out.writeObject(scaleUpAutoAdjustEnabled);
        out.writeObject(scaleUpAutoAdjustAwaitingTime);
        out.writeObject(scaleDownAutoAdjustEnabled);
        out.writeObject(scaleDownAutoAdjustAwaitingTime);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        op = VisorBaselineOperation.fromOrdinal(in.readByte());
        topVer = in.readLong();
        consistentIds = U.readList(in);

        if (protoVer > V1) {
            autoAdjustEnabled = (Boolean)in.readObject();
            autoAdjustAwaitingTime = (Long)in.readObject();
        }

        if (protoVer > V2) {
            scaleUpAutoAdjustEnabled = (Boolean)in.readObject();
            scaleUpAutoAdjustAwaitingTime = (Long)in.readObject();

            scaleDownAutoAdjustEnabled = (Boolean)in.readObject();
            scaleDownAutoAdjustAwaitingTime = (Long)in.readObject();
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorBaselineTaskArg.class, this);
    }
}
