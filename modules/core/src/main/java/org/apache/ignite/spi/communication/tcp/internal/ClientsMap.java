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

package org.apache.ignite.spi.communication.tcp.internal;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.nio.GridCommunicationClient;

/** */
public class ClientsMap {
    /** */
    @SuppressWarnings("PublicField")
    private static final class ClientsArray implements Cloneable {
        /** */
        public final GridCommunicationClient[] clients;
        /** */
        public final GridFutureAdapter<?>[] futs;

        /** */
        public ClientsArray(GridCommunicationClient[] clients, GridFutureAdapter<?>[] futs) {
            this.clients = clients;
            this.futs = futs;
        }

        /** {@inheritDoc} */
        @SuppressWarnings("MethodDoesntCallSuperMethod")
        @Override protected ClientsArray clone() {
            return new ClientsArray(clients.clone(), futs.clone());
        }
    }

    /** */
    private final int connectionsPerNode;

    /** */
    private final ConcurrentMap<UUID, ClientsArray> clients = new ConcurrentHashMap<>();

    /** */
    public ClientsMap(int connectionsPerNode) {
        this.connectionsPerNode = connectionsPerNode;
    }

    /** */
    public GridCommunicationClient get(UUID nodeId, int idx) {
        if (idx >= connectionsPerNode)
            return null;

        ClientsArray curClients = clients.get(nodeId);

        if (curClients == null)
            return null;

        return curClients.clients[idx];
    }

    /** */
    public void add(UUID nodeId, GridCommunicationClient client) {
        int connIdx = client.connectionIndex();

        GridFutureAdapter<?> fut = null;

        while (true) {
            ClientsArray curClients = clients.get(nodeId);

            assert curClients == null || curClients.clients[connIdx] == null : "Client already created [node=" + nodeId +
                ", connIdx=" + connIdx +
                ", client=" + client +
                ", oldClient=" + curClients.clients[connIdx] + ']';

            ClientsArray newClients;

            if (curClients == null) {
                newClients = new ClientsArray(
                    new GridCommunicationClient[connectionsPerNode],
                    new GridFutureAdapter[connectionsPerNode]
                );

                newClients.clients[connIdx] = client;

                if (newClients.futs[connIdx] == null)
                    newClients.futs[connIdx] = fut = new GridFutureAdapter<>();

                if (clients.putIfAbsent(nodeId, newClients) == null)
                    break;
            }
            else {
                newClients = curClients.clone();

                newClients.clients[connIdx] = client;

                if (newClients.futs[connIdx] == null)
                    newClients.futs[connIdx] = fut;

                if (clients.replace(nodeId, curClients, newClients))
                    break;
            }
        }
    }

    /** */
    public boolean remove(UUID nodeId, GridCommunicationClient client) {
        while (true) {
            ClientsArray curClients = clients.get(nodeId);

            int connIdx = client.connectionIndex();

            if (curClients == null || connIdx >= connectionsPerNode || curClients.clients[connIdx] != client)
                return false;

            ClientsArray newClients = curClients.clone();

            newClients.clients[connIdx] = null;

            GridFutureAdapter<?> fut = newClients.futs[connIdx];
            if (fut != null && !fut.isDone())
                fut.onDone();

            newClients.futs[connIdx] = null;

            if (clients.replace(nodeId, curClients, newClients))
                return true;
        }
    }

    /** */
    public IgniteInternalFuture<?> connectionFuture(UUID nodeId, int connIdx) {
        GridFutureAdapter<?> fut = null;

        while (true) {
            ClientsArray curClients = clients.get(nodeId);

            if (curClients == null) {
                ClientsArray newClients = new ClientsArray(
                    new GridCommunicationClient[connectionsPerNode],
                    new GridFutureAdapter[connectionsPerNode]
                );

                if (fut == null)
                    fut = new GridFutureAdapter<>();

                newClients.futs[connIdx] = fut;

                if (clients.putIfAbsent(nodeId, newClients) == null)
                    return fut;
            }
            else
                return curClients.futs[connIdx];
        }
    }

    /** */
    public Set<UUID> clientNodeIds() {
        return clients.keySet();
    }

    /** */
    public void forceClose(UUID nodeId) {
        ClientsArray curClients = clients.get(nodeId);

        if (curClients == null)
            return;

        for (GridCommunicationClient client : curClients.clients) {
            if (client != null)
                client.forceClose();
        }
    }
}
