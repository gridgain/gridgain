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
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;
    using System.Linq;

    /// <summary>
    /// Ignite client projection implementation.
    /// </summary>
    public class ClientClusterGroup : IClientClusterGroup
    {
        /** Attribute: platform. */
        private const string AttrPlatform = "org.apache.ignite.platform";

        /** Platform. */
        private const string Platform = "dotnet";

        /**
         * Initial topology version; invalid from Java perspective,
         * so update will be triggered when this value is met.
         */
        private const int TopVerInit = 0;

        /** Ignite. */
        private IgniteClient _ignite;

        /** Marshaller. */
        private Marshaller _marsh;

        /** Topology version. */
        private long _topVer = TopVerInit;

        /** Current projection. */
        private ClientClusterGroupProjection _projection;

        /** Predicate. */
        private readonly Func<IClusterNode, bool> _predicate;

        /** Nodes collection. */
        private IList<IClusterNode> _nodes;

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
            ClientClusterGroupProjection projection, Func<IClusterNode, bool> predicate = null)
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
            return new ClientClusterGroup(_ignite, _marsh, _projection.ForServerNodes());
        }

        /** <inheritDoc /> */
        public ICollection<IClusterNode> GetNodes()
        {
            return RefreshNodes();
        }      

        /** <inheritDoc /> */
        public IClusterNode GetNode(Guid id)
        {

            if (id == Guid.Empty)
            {
                throw new ArgumentException("id should not be empty");
            }

            return GetNodes().FirstOrDefault(node => node.Id == id);
        }

        /** <inheritDoc /> */
        public IClusterNode GetNode()
        {
            return GetNodes().FirstOrDefault();
        }

        /// <summary>
        /// Refresh projection nodes.
        /// </summary>
        /// <returns>Nodes.</returns>
        private IList<IClusterNode> RefreshNodes()
        {
            long oldTopVer = Interlocked.Read(ref _topVer);

            Action<BinaryWriter> action = w =>
            {
                w.WriteLong(oldTopVer);
                _projection.Marshall(w);
            };
            var res = DoOutInOp(ClientOp.ClusterGroupGetNodes, w => action(w),
                r => ReadNodes(r.GetRawReader()));

            if (res != null)
            {
                UpdateTopology(res.Item1, res.Item2);

                return res.Item2;
            }

            // No topology changes.
            Debug.Assert(_nodes != null, "At least one topology update should have occurred.");

            return _nodes;
        }

        private Tuple<long, List<IClusterNode>> ReadNodes(IBinaryRawReader reader)
        {
            if (reader.ReadBoolean())
            {
                // Topology has been updated.
                long newTopVer = reader.ReadLong();
                var newNodes = IgniteUtils.ReadNodes((BinaryReader) reader, _predicate);

                return Tuple.Create(newTopVer, newNodes);
            }

            return null;
        }

        /// <summary>
        /// Update topology.
        /// </summary>
        /// <param name="newTopVer">New topology version.</param>
        /// <param name="newNodes">New nodes.</param>
        internal void UpdateTopology(long newTopVer, List<IClusterNode> newNodes)
        {
            lock (this)
            {
                // If another thread already advanced topology version further, we still
                // can safely return currently received nodes, but we will not assign them.
                if (_topVer < newTopVer)
                {
                    Interlocked.Exchange(ref _topVer, newTopVer);

                    _nodes = newNodes.AsReadOnly();
                }
            }
        }

        /// <summary>
        /// Does the out in op.
        /// </summary>
        private T DoOutInOp<T>(ClientOp opId, Action<BinaryWriter> writeAction, Func<IBinaryReader, T> readFunc)
        {
            return _ignite.Socket.DoOutInOp(opId, stream => WriteRequest(writeAction, stream),
                stream => ReadRequest(readFunc, stream), HandleError<T>);
        }

        /// <summary>
        /// Writes the request.
        /// </summary>
        private void WriteRequest(Action<BinaryWriter> writeAction, IBinaryStream stream)
        {
            if (writeAction != null)
            {
                var writer = _marsh.StartMarshal(stream);

                writeAction(writer);

                _marsh.FinishMarshal(writer);
            }
        }

        /// <summary>
        /// Reads the request.
        /// </summary>
        private TRes ReadRequest<TRes>(Func<BinaryReader, TRes> readFunc, IBinaryStream stream)
        {
            if (readFunc != null)
            {
                var reader = _marsh.StartUnmarshal(stream);

                return readFunc(reader);
            }

            return default(TRes);
        }

        /// <summary>
        /// Handles the error.
        /// </summary>
        private T HandleError<T>(ClientStatusCode status, string msg)
        {
            throw new IgniteClientException(msg, null, status);
        }
    }
}
