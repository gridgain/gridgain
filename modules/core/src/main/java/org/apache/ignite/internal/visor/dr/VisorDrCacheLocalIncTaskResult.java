package org.apache.ignite.internal.visor.dr;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.S;

/** */
public class VisorDrCacheLocalIncTaskResult extends IgniteDataTransferObject {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** */
    String resultMsg;

    /** */
    byte dataCenterId;

    /** */
    public void resultMessage(String resultMessage) {
        resultMsg = resultMessage;
    }

    /** */
    public void setDataCenterId(byte dataCenterId) {
        this.dataCenterId = dataCenterId;
    }

    /** */
    public VisorDrCacheLocalIncTaskResult() {
    }

    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeByte(dataCenterId);
        out.writeUTF(resultMsg);
    }

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
        return S.toString(VisorDrCacheLocalIncTaskResult.class, this);
    }
}

