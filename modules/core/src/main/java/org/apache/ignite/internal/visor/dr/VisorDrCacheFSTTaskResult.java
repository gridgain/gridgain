/*
 * Copyright 2022 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.visor.dr;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.S;

/** */
public class VisorDrCacheFSTTaskResult extends IgniteDataTransferObject {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** */
    private String resultMsg;

    /** */
    private byte dataCenterId;

    /** */
    public VisorDrCacheFSTTaskResult() {
    }

    /** */
    public VisorDrCacheFSTTaskResult(byte dcId, String msg) {
        dataCenterId = dcId;
        resultMsg = msg;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeByte(dataCenterId);
        out.writeUTF(resultMsg);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        dataCenterId = in.readByte();
        resultMsg = in.readUTF();
    }

    /**
     * @return Data center id.
     */
    public byte dataCenterId() {
        return dataCenterId;
    }

    /**
     * @return Result message.
     */
    public String resultMessage() {
        return resultMsg;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorDrCacheFSTTaskResult.class, this);
    }
}

