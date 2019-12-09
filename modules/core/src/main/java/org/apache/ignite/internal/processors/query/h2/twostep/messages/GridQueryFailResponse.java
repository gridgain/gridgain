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

package org.apache.ignite.internal.processors.query.h2.twostep.messages;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import org.apache.ignite.cache.query.QueryCancelledException;
import org.apache.ignite.cache.query.QueryRetryException;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * Error message.
 */
public class GridQueryFailResponse implements Message {
    /** General error failure type. */
    public static final byte GENERAL_ERROR = 0;

    /** Cancelled by originator failure type. */
    public static final byte CANCELLED_BY_ORIGINATOR = 1;

    /** Execution error. Query should be retried. */
    public static final byte RETRY_QUERY = 2;

    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private long qryReqId;

    /** */
    private String errMsg;

    /** */
    private byte failCode;

    /** */
    private int sqlErrCode;

    /**
     * Default constructor.
     */
    public GridQueryFailResponse() {
        // No-op.
    }

    /**
     * @param qryReqId Query request ID.
     * @param err Error.
     */
    public GridQueryFailResponse(long qryReqId, Throwable err) {
        this.qryReqId = qryReqId;
        this.errMsg = err.getMessage();

        if (err instanceof QueryCancelledException)
            this.failCode = CANCELLED_BY_ORIGINATOR;
        else if (err instanceof QueryRetryException)
            this.failCode = RETRY_QUERY;
        else
            this.failCode = GENERAL_ERROR;

        IgniteSQLException igniteSqlEx = X.cause(err, IgniteSQLException.class);

        SQLException sqlEx = igniteSqlEx != null ? igniteSqlEx.toJdbcException() : X.cause(err, SQLException.class);

        if (sqlEx != null)
            this.sqlErrCode = sqlEx.getErrorCode();
    }

    /**
     * @return Query request ID.
     */
    public long queryRequestId() {
        return qryReqId;
    }

    /**
     * @return Error.
     */
    public String error() {
        return errMsg;
    }

    /**
     * @return Fail code.
     */
    public byte failCode() {
        return failCode;
    }

    /**
     * @return Sql error code.
     */
    public int sqlErrCode() {
        return sqlErrCode;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridQueryFailResponse.class, this);
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeString("errMsg", errMsg))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeByte("failCode", failCode))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeLong("qryReqId", qryReqId))
                    return false;

                writer.incrementState();

            case 3:
                if (!writer.writeInt("sqlErrCode", sqlErrCode))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        if (!reader.beforeMessageRead())
            return false;

        switch (reader.state()) {
            case 0:
                errMsg = reader.readString("errMsg");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                failCode = reader.readByte("failCode");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                qryReqId = reader.readLong("qryReqId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 3:
                sqlErrCode = reader.readInt("sqlErrCode");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridQueryFailResponse.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 107;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 4;
    }
}
