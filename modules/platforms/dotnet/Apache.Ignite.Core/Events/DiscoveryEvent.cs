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

namespace Apache.Ignite.Core.Events
{
    using System.Collections.Generic;
    using System.Collections.ObjectModel;
    using System.Globalization;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Impl;
    using Apache.Ignite.Core.Impl.Binary;

    /// <summary>
    /// Grid discovery event.
    /// </summary>
    public sealed class DiscoveryEvent : EventBase
	{
        /** */
        private readonly IClusterNode _eventNode;

        /** */
        private readonly long _topologyVersion;

        /** */
        private readonly ReadOnlyCollection<IClusterNode> _topologyNodes;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="r">The reader to read data from.</param>
        internal DiscoveryEvent(BinaryReader r) : base(r)
        {
            _eventNode = ReadNode(r);
            _topologyVersion = r.ReadLong();

            var nodes = IgniteUtils.ReadNodes(r);

            _topologyNodes = nodes == null ? null : new ReadOnlyCollection<IClusterNode>(nodes);
        }

        /// <summary>
        /// Gets node that caused this event to be generated. It is potentially different from the node on which this 
        /// event was recorded. For example, node A locally recorded the event that a remote node B joined the topology. 
        /// In this case this property will return node B. 
        /// </summary>
        public IClusterNode EventNode { get { return _eventNode; } }

        /// <summary>
        /// Gets topology version if this event is raised on topology change and configured discovery
        /// SPI implementation supports topology versioning.
        /// </summary>
        public long TopologyVersion { get { return _topologyVersion; } }

        /// <summary>
        /// Gets topology nodes from topology snapshot. If SPI implementation does not support versioning, the best 
        /// effort snapshot will be captured. 
        /// </summary>
        public ICollection<IClusterNode> TopologyNodes { get { return _topologyNodes; } }

        /// <summary>
        /// Gets shortened version of ToString result.
        /// </summary>
	    public override string ToShortString()
	    {
            return string.Format(CultureInfo.InvariantCulture, 
                "{0}: EventNode={1}, TopologyVersion={2}, TopologyNodes={3}", Name, EventNode, 
                TopologyVersion, TopologyNodes.Count);
	    }
    }
}
