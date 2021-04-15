package org.apache.ignite.spi.communication.tcp;

import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridJobCancelRequest;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;

public class SpyTcpCommunicationSpi extends TcpCommunicationSpi {

    @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC)
        throws IgniteSpiException {

        if (msg instanceof GridIoMessage) {
//            log.info("Send message to node [node=" + node + ", " + "msg=" + ((GridIoMessage)msg).message() + ']');

            if (((GridIoMessage)msg).message() instanceof GridJobCancelRequest)
                U.dumpStack(log, "GridJobCancelRequest caught [node=" + node + ", " +
                    "msg=" + ((GridIoMessage)msg).message() + ']');
        }

        super.sendMessage(node, msg, ackC);
    }

    @Override public void sendMessage(ClusterNode node, Message msg) throws IgniteSpiException {
        sendMessage(node, msg, null);
    }
}
