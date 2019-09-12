package org.apache.ignite.plugin.extensions.communication;

/**
 * Common interface for time requests.
 */
public interface TimeLoggableRequest extends Message {
    /**
     * @return Message send timestamp in sender node time.
     */
    long getSendTimestamp();

    /**
     * Sets send timestamp.
     */
    void setSendTimestamp(long sendTimestamp);

    /**
     * @return Message receive timestamp in receiver node time.
     */
    long getReceiveTimestamp();

    /**
     * Sets receive timestamp.
     */
    void setReceiveTimestamp(long receiveTimestamp);
}
