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

#include <ignite/impl/cluster/cluster_group_impl.h>

namespace ignite
{
    namespace cluster
    {
        class IGNITE_IMPORT_EXPORT ClusterGroup
        {
            friend impl::cluster::SP_ClusterGroupImpl GetClusterGroupImpl(ClusterGroup* grp);
        public:
            /**
             * Constructor.
             */
            ClusterGroup(impl::cluster::SP_ClusterGroupImpl impl);

            /**
             * Get cluster group for nodes containing given name and value specified in user attributes.
             *
             * @return Specified nodes cluster group.
             */
            ClusterGroup ForAttribute(std::string name, std::string val);

            /**
             * Get server nodes cluster group.
             *
             * @return Server nodes cluster group.
             */
            ClusterGroup ForServers();

        private:
            impl::cluster::SP_ClusterGroupImpl impl;
        };
    }
}

#endif //_IGNITE_CLUSTER_CLUSTER_GROUP