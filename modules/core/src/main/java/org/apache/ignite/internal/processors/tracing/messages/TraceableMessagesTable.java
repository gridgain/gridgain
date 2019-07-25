package org.apache.ignite.internal.processors.tracing.messages;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.IgniteException;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryCustomEventMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryJoinRequestMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeAddFinishedMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeAddedMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeFailedMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeLeftMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryServerOnlyCustomEventMessage;

/**
 * Mapping from I/O message to trace that represents
 */
public class TraceableMessagesTable {
    /** Message trace lookup table. */
    private static final Map<Class<? extends TraceableMessage>, String> msgTraceLookupTable = new ConcurrentHashMap<>();

    static {
        msgTraceLookupTable.put(TcpDiscoveryJoinRequestMessage.class, "discovery.node.join.request");
        msgTraceLookupTable.put(TcpDiscoveryNodeAddedMessage.class, "discovery.node.join.add");
        msgTraceLookupTable.put(TcpDiscoveryNodeAddFinishedMessage.class, "discovery.node.join.finish");
        msgTraceLookupTable.put(TcpDiscoveryNodeFailedMessage.class, "discovery.node.failed");
        msgTraceLookupTable.put(TcpDiscoveryNodeLeftMessage.class, "discovery.node.left");
        msgTraceLookupTable.put(TcpDiscoveryCustomEventMessage.class, "discovery.custom.event");
        msgTraceLookupTable.put(TcpDiscoveryServerOnlyCustomEventMessage.class, "discovery.custom.event");
    }

    private TraceableMessagesTable() {};

    public static String traceName(Class<? extends TraceableMessage> msgCls) {
        String traceName = msgTraceLookupTable.get(msgCls);

        if (traceName == null)
            throw new IgniteException("Trace name is not defined for " + msgCls);

        return traceName;
    }
}
