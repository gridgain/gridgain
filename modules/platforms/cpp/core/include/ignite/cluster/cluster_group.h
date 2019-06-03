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

 /**
  * @file
  * Declares ignite::cluster::ClusterGroup class.
  */

#ifndef _IGNITE_CLUSTER_CLUSTER_GROUP
#define _IGNITE_CLUSTER_CLUSTER_GROUP

#include <ignite/cluster/cluster_node.h>

#include <ignite/impl/cluster/cluster_group_impl.h>

namespace ignite
{
    namespace cluster
    {
        /**
         * Defines a cluster group which contains all or a subset of cluster nodes.
         * Cluster group allows to group cluster nodes into various subgroups to perform distributed operations on them.
         * The IgniteCluster interface itself also contains the ClusterGroup which makes an instance of IgniteCluster
         * into a cluster group containing all cluster nodes. Use IgniteCluster::ForAll() to get the cluster group in this case.
         */
        class IGNITE_IMPORT_EXPORT ClusterGroup
        {
            friend class impl::cluster::ClusterGroupImpl;
        public:
            /**
             * Constructor.
             *
             * @param pointer to cluster group implementation.
             */
            ClusterGroup(impl::cluster::SP_ClusterGroupImpl impl);

            /**
             * Get cluster group for nodes containing given name and value specified in user attributes.
             *
             * @return Specified nodes cluster group.
             */
            ClusterGroup ForAttribute(std::string name, std::string val);

            /**
             * Get ClusterGroup for all data nodes that have the cache with the specified name running.
             *
             * @return All data nodes ClusterGroup.
             */
            ClusterGroup ForDataNodes(std::string cacheName);

            /**
             * Get server nodes cluster group.
             *
             * @return Server nodes cluster group.
             */
            ClusterGroup ForServers();

            /**
             * Get cpp nodes cluster group.
             *
             * @return Cpp nodes cluster group.
             */
            ClusterGroup ForCpp();

            /**
             * Get container of cluster nodes over this cluster group.
             *
             * @return Vector of cluster nodes.
             */
            std::vector<ClusterNode> GetNodes();

        private:
            impl::cluster::SP_ClusterGroupImpl impl;
            impl::cluster::SP_ClusterGroupImpl GetImpl();
        };
    }
}

#endif //_IGNITE_CLUSTER_CLUSTER_GROUP