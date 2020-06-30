using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Apache.Ignite.Core.Impl.Cache
{
    using Apache.Ignite.Core.Cache.Store;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Cache.Store;
    using Apache.Ignite.Core.Impl.Handle;

    public class NodeFilter
    {
        private readonly IClusterNodeFilter _clusterNodeFilter;
        private readonly long _handle;

        public NodeFilter(IClusterNodeFilter nodeFilter, HandleRegistry registry)
        {
            _clusterNodeFilter = nodeFilter;
            _handle = registry.AllocateCritical(this);
        }

        /// <summary>
        /// Gets the handle.
        /// </summary>
        public long Handle
        {
            get { return _handle; }
        }

        public bool Apply(IClusterNode clusterNode)
        {
            return _clusterNodeFilter.Invoke(clusterNode);
        }

        internal static NodeFilter CreateFilter(long memPtr, HandleRegistry registry)
        {
            using (var stream = IgniteManager.Memory.Get(memPtr).GetStream())
            {
                var reader = BinaryUtils.Marshaller.StartUnmarshal(stream);

                //var convertBinary = reader.ReadBoolean();
                var cacheFilter = reader.ReadObject<IClusterNodeFilter>();

                if (cacheFilter == null)
                {
                    var className = reader.ReadString();
                    var propertyMap = reader.ReadDictionaryAsGeneric<string, object>();

                    cacheFilter = IgniteUtils.CreateInstance<IClusterNodeFilter>(className, propertyMap);
                }

                return new NodeFilter(cacheFilter, registry);
            }
        }

        internal static bool ApplyFilter(IIgniteInternal ignite, long memPtr)
        {
            using (var stream = IgniteManager.Memory.Get(memPtr).GetStream())
            {
                var reader = BinaryUtils.Marshaller.StartUnmarshal(stream);

                //var convertBinary = reader.ReadBoolean();
                var cacheFilter = reader.ReadObject<IClusterNodeFilter>();

                if (cacheFilter == null)
                {
                    var className = reader.ReadString();
                    var propertyMap = reader.ReadDictionaryAsGeneric<string, object>();

                    cacheFilter = IgniteUtils.CreateInstance<IClusterNodeFilter>(className, propertyMap);
                }

                var nodeId = reader.ReadGuid();


                return cacheFilter.Invoke(ignite.GetNode(nodeId));
            }
        }
    }
}
