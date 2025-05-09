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
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.util.IgniteUtils.MAX_UTF_BYTES;
import static org.apache.ignite.internal.util.IgniteUtils.UTF_BYTE_LIMIT;

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

    /** {@inheritDoc} */
    @Override public byte getProtocolVersion() {
        return V2;
    }

    /** */
    public VisorDrCacheFSTTaskResult(byte dcId, String msg) {
        dataCenterId = dcId;
        resultMsg = msg;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeByte(dataCenterId);

        if (U.isStringTooLongForWriteHeuristically(resultMsg)) {
            int length = UTF_BYTE_LIMIT / MAX_UTF_BYTES;

            out.writeUTF(resultMsg.substring(0, length));

            U.writeLongString(out, resultMsg.substring(length));
        }
        else {
            out.writeUTF(resultMsg);
            U.writeLongString(out, null);
        }
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        dataCenterId = in.readByte();
        resultMsg = in.readUTF();

        if (protoVer >= V2) {
            resultMsg += U.readLongString(in);
        }
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

