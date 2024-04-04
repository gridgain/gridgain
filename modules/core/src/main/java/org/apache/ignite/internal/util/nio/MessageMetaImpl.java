package org.apache.ignite.internal.util.nio;

/**
 * Basic implementation of message meta-information container.
 */
public class MessageMetaImpl implements MessageMeta {
    /** {@code True} if related message is heartbeat. */
    private final boolean isHeartbeat;

    /** Ctor. */
    public MessageMetaImpl(boolean isHeartbeat) {
        this.isHeartbeat = isHeartbeat;
    }

    /** {@inheritDoc} */
    @Override public boolean isHeartbeat() {
        return isHeartbeat;
    }
}
