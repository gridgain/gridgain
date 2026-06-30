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

namespace Apache.Ignite.Core.Impl.Client.Cluster
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Threading;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Impl.Binary;
    using System.Linq;
    using Apache.Ignite.Core.Client.Compute;
    using Apache.Ignite.Core.Client.Services;
    using Apache.Ignite.Core.Impl.Client.Compute;
    using Apache.Ignite.Core.Impl.Client.Services;
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// Ignite client projection implementation.
    /// </summary>
    internal class ClientClusterGroup : IClientClusterGroup
    {
        /** Attribute: platform. */
        private const string AttrPlatform = "org.apache.ignite.platform";

        /** Platform: .NET. */
        private const string PlatformDotNet = "dotnet";

        /** Ignite. */
        private readonly IgniteClient _ignite;

        /** Topology version. */
        private long _topVer;

        /** Locker. */
        private readonly object _syncRoot = new object();

        /** Current projection. */
        private readonly ClientClusterGroupProjection _projection;

        /** Predicate. */
        private readonly Func<IClientClusterNode, bool>? _predicate;

        /** Node ids collection. */
        private Guid[]? _nodeIds;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="ignite">Ignite.</param>
        internal ClientClusterGroup(IgniteClient ignite)
            : this(ignite, ClientClusterGroupProjection.Empty)
        {
            // No-op.
        }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="ignite">Ignite.</param>
        /// <param name="projection">Projection.</param>
        /// <param name="predicate">Predicate.</param>
        private ClientClusterGroup(IgniteClient ignite,
            ClientClusterGroupProjection projection, Func<IClientClusterNode, bool>? predicate = null)
        {
            _ignite = ignite;
            _projection = projection;
            _predicate = predicate;
        }

        /** <inheritDoc /> */
        public IClientClusterGroup ForAttribute(string name, string val)
        {
            IgniteArgumentCheck.NotNullOrEmpty(name, "name");

            return new ClientClusterGroup(_ignite, _projection.ForAttribute(name, val));
        }

        /** <inheritDoc /> */
        public IClientClusterGroup ForDotNet()
        {
            return ForAttribute(AttrPlatform, PlatformDotNet);
        }

        /** <inheritDoc /> */
        public IClientClusterGroup ForServers()
        {
            return new ClientClusterGroup(_ignite, _projection.ForServerNodes(true));
        }

        /** <inheritDoc /> */
        public IClientClusterGroup ForPredicate(Func<IClientClusterNode, bool> p)
        {
            IgniteArgumentCheck.NotNull(p, "p");

            var newPredicate = _predicate == null ? p : node => _predicate(node) && p(node);
            return new ClientClusterGroup(_ignite, _projection, newPredicate);
        }

        /** <inheritDoc /> */
        public ICollection<IClientClusterNode> GetNodes()
        {
            return RefreshNodes();
        }

        /** <inheritDoc /> */
        public async Task<ICollection<IClientClusterNode>> GetNodesAsync()
        {
            return await RefreshNodesAsync().ConfigureAwait(false);
        }

        /** <inheritDoc /> */
        public IClientClusterNode? GetNode(Guid id)
        {
            if (id == Guid.Empty)
            {
                throw new ArgumentException("Node id should not be empty.");
            }

            return GetNodes().FirstOrDefault(node => node.Id == id);
        }

        /** <inheritDoc /> */
        public async Task<IClientClusterNode?> GetNodeAsync(Guid id)
        {
            if (id == Guid.Empty)
            {
                throw new ArgumentException("Node id should not be empty.");
            }

            var nodes = await RefreshNodesAsync().ConfigureAwait(false);

            return nodes.FirstOrDefault(node => node.Id == id);
        }

        /** <inheritDoc /> */
        public IClientClusterNode? GetNode()
        {
            return GetNodes().FirstOrDefault();
        }

        /** <inheritDoc /> */
        public async Task<IClientClusterNode?> GetNodeAsync()
        {
            var nodes = await RefreshNodesAsync().ConfigureAwait(false);

            return nodes.FirstOrDefault();
        }

        /** <inheritDoc /> */
        public IComputeClient GetCompute()
        {
            return new ComputeClient(_ignite, ComputeClientFlags.None, TimeSpan.Zero, this);
        }

        /** <inheritDoc /> */
        public IServicesClient GetServices()
        {
            return new ServicesClient(_ignite, this);
        }

        /// <summary>
        /// Refresh projection nodes.
        /// </summary>
        /// <returns>Nodes.</returns>
        private IList<IClientClusterNode> RefreshNodes()
        {
            long oldTopVer = Interlocked.Read(ref _topVer);

            var topology = RequestTopologyInformation(oldTopVer);
            if (topology != null)
            {
                UpdateTopology(topology.Item1, topology.Item2);
                RequestNodesInfo(topology.Item2);
            }

            return BuildNodesList();
        }

        /// <summary>
        /// Refresh projection nodes asynchronously.
        /// </summary>
        /// <returns>Nodes.</returns>
        private async Task<IList<IClientClusterNode>> RefreshNodesAsync()
        {
            long oldTopVer = Interlocked.Read(ref _topVer);

            var topology = await RequestTopologyInformationAsync(oldTopVer).ConfigureAwait(false);
            if (topology != null)
            {
                UpdateTopology(topology.Item1, topology.Item2);
                await RequestNodesInfoAsync(topology.Item2).ConfigureAwait(false);
            }

            return BuildNodesList();
        }

        /// <summary>
        /// Builds the resulting nodes list from the current topology snapshot, applying the predicate.
        /// </summary>
        /// <returns>Nodes.</returns>
        private IList<IClientClusterNode> BuildNodesList()
        {
            // No topology changes.
            Debug.Assert(_nodeIds != null, "At least one topology update should have occurred.");

            // Local lookup with a native predicate is a trade off between complexity and consistency.
            var nodesList = new List<IClientClusterNode>();
            foreach (Guid nodeId in _nodeIds!)
            {
                IClientClusterNode node = _ignite.GetClientNode(nodeId);
                if (_predicate == null || _predicate(node))
                {
                    nodesList.Add(node);
                }
            }

            return nodesList;
        }

        /// <summary>
        /// Request topology information.
        /// </summary>
        /// <returns>Topology version with nodes identifiers.</returns>
        private Tuple<long, Guid[]>? RequestTopologyInformation(long oldTopVer)
        {
            return DoOutInOp(
                ClientOp.ClusterGroupGetNodeIds,
                ctx => WriteTopologyRequest(ctx, oldTopVer),
                ReadTopologyInformation);
        }

        /// <summary>
        /// Request topology information asynchronously.
        /// </summary>
        /// <returns>Topology version with nodes identifiers.</returns>
        private Task<Tuple<long, Guid[]>?> RequestTopologyInformationAsync(long oldTopVer)
        {
            return DoOutInOpAsync(
                ClientOp.ClusterGroupGetNodeIds,
                ctx => WriteTopologyRequest(ctx, oldTopVer),
                ReadTopologyInformation);
        }

        /// <summary>
        /// Writes the topology information request.
        /// </summary>
        private void WriteTopologyRequest(ClientRequestContext ctx, long oldTopVer)
        {
            ctx.Stream.WriteLong(oldTopVer);
            _projection.Write(ctx.Writer);
        }

        /// <summary>
        /// Reads the topology information response.
        /// </summary>
        private static Tuple<long, Guid[]>? ReadTopologyInformation(ClientResponseContext ctx)
        {
            if (!ctx.Stream.ReadBool())
            {
                // No topology changes.
                return null;
            }

            long remoteTopVer = ctx.Stream.ReadLong();
            return Tuple.Create(remoteTopVer, ReadNodeIds(ctx.Reader));
        }

        /// <summary>
        /// Reads node ids.
        /// </summary>
        /// <param name="reader">Reader.</param>
        /// <returns>Node ids array.</returns>
        private static Guid[] ReadNodeIds(IBinaryRawReader reader)
        {
            int nodesCount = reader.ReadInt();

            var nodeIds = new Guid[nodesCount];
            var stream = ((BinaryReader) reader).Stream;

            for (int i = 0; i < nodesCount; i++)
            {
                nodeIds[i] = BinaryUtils.ReadGuid(stream);
            }

            return nodeIds;
        }

        /// <summary>
        /// Update topology.
        /// </summary>
        /// <param name="remoteTopVer">Remote topology version.</param>
        /// <param name="nodeIds">Node ids.</param>
        internal void UpdateTopology(long remoteTopVer, Guid[] nodeIds)
        {
            lock (_syncRoot)
            {
                // If another thread already advanced topology version further, we still
                // can safely return currently received nodes, but we will not assign them.
                if (_topVer < remoteTopVer)
                {
                    Interlocked.Exchange(ref _topVer, remoteTopVer);
                    _nodeIds = nodeIds;
                }
            }
        }

        /// <summary>
        /// Gets nodes information.
        /// This method will filter only unknown node ids that
        /// have not been serialized inside IgniteClient before.
        /// </summary>
        /// <param name="nodeIds">Node ids array.</param>
        /// <returns>Collection of <see cref="IClusterNode"/> instances.</returns>
        private void RequestNodesInfo(Guid[] nodeIds)
        {
            var unknownNodes = GetUnknownNodes(nodeIds);

            if (unknownNodes.Count > 0)
            {
                RequestRemoteNodesDetails(unknownNodes);
            }
        }

        /// <summary>
        /// Gets nodes information asynchronously.
        /// This method will filter only unknown node ids that
        /// have not been serialized inside IgniteClient before.
        /// </summary>
        /// <param name="nodeIds">Node ids array.</param>
        private async Task RequestNodesInfoAsync(Guid[] nodeIds)
        {
            var unknownNodes = GetUnknownNodes(nodeIds);

            if (unknownNodes.Count > 0)
            {
                await RequestRemoteNodesDetailsAsync(unknownNodes).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Filters out node ids that are already known to the client.
        /// </summary>
        /// <param name="nodeIds">Node ids array.</param>
        /// <returns>Unknown node ids.</returns>
        private List<Guid> GetUnknownNodes(Guid[] nodeIds)
        {
            var unknownNodes = new List<Guid>(nodeIds.Length);
            foreach (var nodeId in nodeIds)
            {
                if (!_ignite.ContainsNode(nodeId))
                {
                    unknownNodes.Add(nodeId);
                }
            }

            return unknownNodes;
        }

        /// <summary>
        /// Make remote API call to fetch node information.
        /// </summary>
        /// <param name="ids">Node identifiers.</param>
        private void RequestRemoteNodesDetails(List<Guid> ids)
        {
            DoOutInOp(ClientOp.ClusterGroupGetNodesInfo,
                ctx => WriteRemoteNodesDetailsRequest(ctx, ids),
                ReadRemoteNodesDetails);
        }

        /// <summary>
        /// Make remote API call to fetch node information asynchronously.
        /// </summary>
        /// <param name="ids">Node identifiers.</param>
        private Task<bool> RequestRemoteNodesDetailsAsync(List<Guid> ids)
        {
            return DoOutInOpAsync(ClientOp.ClusterGroupGetNodesInfo,
                ctx => WriteRemoteNodesDetailsRequest(ctx, ids),
                ReadRemoteNodesDetails);
        }

        /// <summary>
        /// Writes the remote nodes details request.
        /// </summary>
        private static void WriteRemoteNodesDetailsRequest(ClientRequestContext ctx, List<Guid> ids)
        {
            ctx.Stream.WriteInt(ids.Count);
            foreach (var id in ids)
            {
                BinaryUtils.WriteGuid(id, ctx.Stream);
            }
        }

        /// <summary>
        /// Reads the remote nodes details response.
        /// </summary>
        private bool ReadRemoteNodesDetails(ClientResponseContext ctx)
        {
            var cnt = ctx.Stream.ReadInt();
            for (var i = 0; i < cnt; i++)
            {
                _ignite.SaveClientClusterNode(ctx.Reader);
            }

            return true;
        }


        /// <summary>
        /// Does the out in op.
        /// </summary>
        protected T DoOutInOp<T>(ClientOp opId, Action<ClientRequestContext> writeAction,
            Func<ClientResponseContext, T> readFunc)
        {
            return _ignite.Socket.DoOutInOp(opId, writeAction, readFunc, HandleError<T>);
        }

        /// <summary>
        /// Does the out in op asynchronously.
        /// </summary>
        protected Task<T> DoOutInOpAsync<T>(ClientOp opId, Action<ClientRequestContext> writeAction,
            Func<ClientResponseContext, T> readFunc)
        {
            return _ignite.Socket.DoOutInOpAsync(opId, writeAction, readFunc, HandleError<T>);
        }

        /// <summary>
        /// Handles the error.
        /// </summary>
        private static T HandleError<T>(ClientStatusCode status, string msg)
        {
            throw new IgniteClientException(msg, null, status);
        }
    }
}
