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
    using System.Diagnostics.CodeAnalysis;
    using System.Threading;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;
    using System.Linq;
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// Ignite client projection implementation.
    /// </summary>
    internal class ClientClusterGroup : IClientClusterGroup
    {
        /** Attribute: platform. */
        private const string AttrPlatform = "org.apache.ignite.platform";

        /** Platform. */
        private const string Platform = "dotnet";

        /** Ignite. */
        private readonly IgniteClient _ignite;

        /** Marshaller. */
        private readonly Marshaller _marsh;

        /** Topology version. */
        private long _topVer;

        /** Locker. */
        private readonly object _syncRoot = new object();

        /** Current projection. */
        private readonly ClientClusterGroupProjection _projection;

        /** Predicate. */
        private readonly Func<IClientClusterNode, bool> _predicate;

        /** Node ids collection. */
        private IList<Guid> _nodeIds;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="ignite">Ignite.</param>
        /// <param name="marsh">Marshaller.</param>
        internal ClientClusterGroup(IgniteClient ignite, Marshaller marsh)
            : this(ignite, marsh, ClientClusterGroupProjection.Empty)
        {
        }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="ignite">Ignite.</param>
        /// <param name="marsh">Marshaller.</param>
        /// <param name="projection">Projection.</param>
        /// <param name="predicate">Predicate.</param>
        private ClientClusterGroup(IgniteClient ignite, Marshaller marsh,
            ClientClusterGroupProjection projection, Func<IClientClusterNode, bool> predicate = null)
        {
            Debug.Assert(ignite != null);
            Debug.Assert(marsh != null);

            _ignite = ignite;
            _marsh = marsh;
            _projection = projection;
            _predicate = predicate;
        }

        /** <inheritDoc /> */
        public IClientClusterGroup ForAttribute(string name, string val)
        {
            IgniteArgumentCheck.NotNullOrEmpty(name, "name");

            return new ClientClusterGroup(_ignite, _marsh, _projection.ForAttribute(name, val));
        }

        /** <inheritDoc /> */
        public IClientClusterGroup ForDotNet()
        {
            return ForAttribute(AttrPlatform, Platform);
        }

        /** <inheritDoc /> */
        public IClientClusterGroup ForServers()
        {
            return new ClientClusterGroup(_ignite, _marsh, _projection.ForServerNodes(true));
        }

        /** <inheritDoc /> */
        public IClientClusterGroup ForPredicate(Func<IClientClusterNode, bool> p)
        {
            IgniteArgumentCheck.NotNull(p, "p");

            var newPredicate = _predicate == null ? p : node => _predicate(node) && p(node);
            return new ClientClusterGroup(_ignite, _marsh, _projection, newPredicate);
        }

        /** <inheritDoc /> */
        public ICollection<IClientClusterNode> GetNodes()
        {
            return RefreshNodes();
        }

        /** <inheritDoc /> */
        public IClientClusterNode GetNode(Guid id)
        {
            if (id == Guid.Empty)
            {
                throw new ArgumentException("Node id should not be empty.");
            }

            return GetNodes().FirstOrDefault(node => node.Id == id);
        }

        /** <inheritDoc /> */
        public IClientClusterNode GetNode()
        {
            return GetNodes().FirstOrDefault();
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

            // No topology changes.
            Debug.Assert(_nodeIds != null, "At least one topology update should have occurred.");

            // Local lookup with a native predicate is a trade off between complexity and consistency.
            var nodesList = new List<IClientClusterNode>(_nodeIds.Count);
            foreach (Guid nodeId in _nodeIds)
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
        /// <returns>Topology version with nodes identifiers.</returns>rns>
        private Tuple<long, List<Guid>> RequestTopologyInformation(long oldTopVer)
        {
            Action<IBinaryRawWriter> writeAction = writer =>
            {
                writer.WriteLong(oldTopVer);
                _projection.Write(writer);
            };

            Func<IBinaryRawReader, Tuple<long, List<Guid>>> readFunc = reader =>
            {
                if (!reader.ReadBoolean())
                {
                    // No topology changes.
                    return null;
                }

                long remoteTopVer = reader.ReadLong();

                List<Guid> nodeIds = reader.ReadGuidArray().Cast<Guid>().ToList();
                return Tuple.Create(remoteTopVer, nodeIds);
            };

            return DoOutInOp(ClientOp.ClusterGroupGetNodeIds, writeAction, readFunc);
        }

        /// <summary>
        /// Update topology.
        /// </summary>
        /// <param name="remoteTopVer">Remote topology version.</param>
        /// <param name="nodeIds">Node ids.</param>
        internal void UpdateTopology(long remoteTopVer, List<Guid> nodeIds)
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
        /// <param name="nodeIds">Node ids collection.</param>
        /// <returns>Collection of <see cref="IClusterNode"/> instances.</returns>
        private void RequestNodesInfo(IEnumerable<Guid> nodeIds)
        {
            var unknownNodes = nodeIds.Where(nodeId => !_ignite.ContainsNode(nodeId)).ToList();
            if (unknownNodes.Count > 0)
            {
                RequestRemoteNodesDetails(unknownNodes);
            }
        }

        /// <summary>
        /// Make remote API call to fetch node information.
        /// </summary>
        /// <param name="ids">Node identifiers.</param>
        private void RequestRemoteNodesDetails(List<Guid> ids)
        {
            Action<IBinaryRawWriter> writeAction = writer =>
            {
                writer.WriteGuidArray(ids.Select(id => new Guid?(id)).ToArray());
            };

            Func<IBinaryRawReader, bool> readFunc = reader =>
            {
                var cnt = reader.ReadInt();
                for (var i = 0; i < cnt; i++)
                {
                    _ignite.SaveClientClusterNode(reader);
                }

                return true;
            };

            DoOutInOp(ClientOp.ClusterGroupGetNodesInfo, writeAction, readFunc);
        }

        /// <summary>
        /// Does the out in op.
        /// </summary>
        protected T DoOutInOp<T>(ClientOp opId, Action<IBinaryRawWriter> writeAction,
            Func<IBinaryRawReader, T> readFunc)
        {
            return _ignite.Socket.DoOutInOp(opId, stream => WriteRequest(writeAction, stream),
                stream => ReadRequest(readFunc, stream), HandleError<T>);
        }

        /// <summary>
        /// Writes the request.
        /// </summary>
        private void WriteRequest(Action<IBinaryRawWriter> writeAction, IBinaryStream stream)
        {
            if (writeAction != null)
            {
                var writer = _marsh.StartMarshal(stream);

                writeAction(writer.GetRawWriter());

                _marsh.FinishMarshal(writer);
            }
        }

        /// <summary>
        /// Reads the request.
        /// </summary>
        [ExcludeFromCodeCoverage]
        private TRes ReadRequest<TRes>(Func<IBinaryRawReader, TRes> readFunc, IBinaryStream stream)
        {
            if (readFunc != null)
            {
                var reader = _marsh.StartUnmarshal(stream);
                return readFunc(reader.GetRawReader());
            }

            return default(TRes);
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
