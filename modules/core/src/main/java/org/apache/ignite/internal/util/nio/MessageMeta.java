package org.apache.ignite.internal.util.nio;

/**
 * Additional information about message sent through GridNioServer.
 * Implementation stores information about message and/or sender context which can be used by GridNioServer
 * and its auxiliary components.
 */
public interface MessageMeta {
    /**
     * @return {@code True} if message
     */
    boolean isHeartbeat();
}
