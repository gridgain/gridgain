package org.apache.ignite.plugin.extensions.communication;

/**
 * Common interface for responses.
 */
public interface TimeLoggableResponse extends Message {
    /** */
    long INVALID_TIMESTAMP = -1;

    /**
     * @return Send timestamp of request that triggered this response
     * in request sender node time.
     */
    long getReqSentTimestamp();

    /**
     * Sets request send timestamp in sender node time.
     */
    void setReqSendTimestamp(long reqSentTimestamp);

    /**
     * @return Received timestamp of request that triggered this response
     * in request receiver node time.
     */
    long getReqReceivedTimestamp();

    /**
     * Sets request receive timestamp in receiver time.
     */
    void setReqReceivedTimestamp(long reqReceivedTimestamp);

    /**
     * @return Response send timestamp which is sum of request send
     * timestamp and request processing time.
     */
    long getResponseSendTimestamp();

    /**
     * Sets request send timestamp.
     */
    void setResponseSendTimestamp(long responseSendTimestamp);
}
