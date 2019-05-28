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
  * Declares ignite::cluster::ClusterNode class.
  */

#ifndef _IGNITE_CLUSTER_CLUSTER_NODE
#define _IGNITE_CLUSTER_CLUSTER_NODE

#include <ignite/impl/cluster/cluster_node_impl.h>

namespace ignite
{
    namespace cluster
    {
        class IGNITE_IMPORT_EXPORT ClusterNode
        {
        public:
            /**
             * Constructor.
             */
            ClusterNode(common::concurrent::SharedPointer<ignite::impl::cluster::ClusterNodeImpl> impl);

            /**
             * Gets globally unique ID.
             *
             * @return Node Guid.
             */
            Guid Id();

        private:
            common::concurrent::SharedPointer<ignite::impl::cluster::ClusterNodeImpl> impl;
        };
    }
}

#endif //_IGNITE_CLUSTER_CLUSTER_NODE