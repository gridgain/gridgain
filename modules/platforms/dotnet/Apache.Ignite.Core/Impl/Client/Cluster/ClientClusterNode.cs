using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Apache.Ignite.Core.Impl.Client.Cluster
{
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Impl.Cluster;

    internal class ClientClusterNode : ClusterNodeImpl
    {
        public ClientClusterNode(IBinaryRawReader reader) : base(reader)
        {
        }

        public bool IsEmpty
        {
            get { return GetAttributes().Count == 0; }
        }
    }
}
