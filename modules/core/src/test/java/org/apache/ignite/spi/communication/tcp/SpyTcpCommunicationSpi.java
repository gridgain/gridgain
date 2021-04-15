package org.apache.ignite.spi.communication.tcp;

import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;

public class SpyTcpCommunicationSpi extends TcpCommunicationSpi {

    @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC)
        throws IgniteSpiException {

        log.info("Send message to node [node=" + node + ", " + "msg=" + msg + ']');

        super.sendMessage(node, msg, ackC);
    }

    @Override public void sendMessage(ClusterNode node, Message msg) throws IgniteSpiException {
        sendMessage(node, msg, null);
    }
}
