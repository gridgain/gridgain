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
  * Declares ignite::cluster::IgniteCluster class.
  */

#ifndef _IGNITE_CLUSTER_IGNITE_CLUSTER
#define _IGNITE_CLUSTER_IGNITE_CLUSTER

#include <ignite/cluster/cluster_group.h>

#include <ignite/impl/cluster/ignite_cluster_impl.h>

namespace ignite
{
    namespace cluster
    {
        class IGNITE_IMPORT_EXPORT IgniteCluster
        {
        public:
            /**
             * Constructor.
             */
            IgniteCluster(common::concurrent::SharedPointer<ignite::impl::cluster::IgniteClusterImpl> impl);

            /**
             * Check if the Ignite grid is active.
             *
             * @return True if grid is active and false otherwise.
             */
            bool IsActive();

            /**
             * Change Ignite grid state to active or inactive.
             *
             * @param active If true start activation process. If false start
             *    deactivation process.
             */
            void SetActive(bool active);

            /**
             * Gets a cluster group impl consisting from the local node.
             *
             * @return Cluster group impl consisting from local node.
             */
            cluster::ClusterGroup ForLocal();

        private:
            common::concurrent::SharedPointer<ignite::impl::cluster::IgniteClusterImpl> impl;
        };
    }
}

#endif //_IGNITE_CLUSTER_IGNITE_CLUSTER