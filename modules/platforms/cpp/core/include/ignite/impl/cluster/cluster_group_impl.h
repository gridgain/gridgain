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

#ifndef _IGNITE_IMPL_CLUSTER_CLUSTER_GROUP_IMPL
#define _IGNITE_IMPL_CLUSTER_CLUSTER_GROUP_IMPL

#include <ignite/common/concurrent.h>
#include <ignite/common/dynamic_size_array.h>
#include <ignite/jni/java.h>

#include <ignite/impl/interop/interop_target.h>
#include <ignite/impl/compute/compute_impl.h>
#include <ignite/impl/cluster/cluster_node_impl.h>

namespace ignite
{
    namespace impl
    {
        namespace cluster
        {
            /* Forward declaration. */
            class ClusterGroupImpl;

            /* Shared pointer. */
            typedef common::concurrent::SharedPointer<ClusterGroupImpl> SP_ClusterGroupImpl;

            /**
             * Cluster group implementation.
             */
            class IGNITE_FRIEND_EXPORT ClusterGroupImpl : private interop::InteropTarget
            {
                typedef common::concurrent::SharedPointer<IgniteEnvironment> SP_IgniteEnvironment;
                typedef common::concurrent::SharedPointer<compute::ComputeImpl> SP_ComputeImpl;
                typedef ignite::common::DynamicSizeArray<SP_ClusterNodeImpl> ClusterNodes;
            public:
                /**
                 * Constructor used to create new instance.
                 *
                 * @param env Environment.
                 * @param javaRef Reference to java object.
                 */
                ClusterGroupImpl(SP_IgniteEnvironment env, jobject javaRef);

                /**
                 * Destructor.
                 */
                ~ClusterGroupImpl();

                /**
                 * Get cluster group implementation for nodes containing given name and value specified in user attributes.
                 *
                 * @return Specified nodes cluster group implementation.
                 */
                SP_ClusterGroupImpl ForAttribute(std::string name, std::string val);

                /**
                 * Get cluster group implementation for all data nodes that have the cache with the specified name running.
                 *
                 * @return All data nodes cluster group implementation.
                 */
                SP_ClusterGroupImpl ForDataNodes(std::string cacheName);

                /**
                 * Get server nodes cluster group implementation.
                 *
                 * @return Server nodes cluster group implementation.
                 */
                SP_ClusterGroupImpl ForServers();

                /**
                 * Get server nodes cluster group implementation.
                 *
                 * @return Cpp nodes cluster group implementation.
                 */
                SP_ClusterGroupImpl ForCpp();

                /**
                 * Get compute instance over this cluster group.
                 *
                 * @return Compute instance.
                 */
                SP_ComputeImpl GetCompute();

                /**
                 * Get container of cluster nodes over this cluster group.
                 *
                 * @return Vector of cluster nodes.
                 */
                ClusterNodes GetNodes();

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

            private:
                IGNITE_NO_COPY_ASSIGNMENT(ClusterGroupImpl);

                /**
                 * Cluster group over nodes that have specified cache running.
                 *
                 * @param cache name to include into cluster group.
                 * @param operation id.
                 * @return New cluster group implementation.
                 */
                SP_ClusterGroupImpl ForCacheNodes(std::string name, int32_t op);

                /**
                 * Make cluster group implementation using java reference and
                 * internal state of this cluster group.
                 *
                 * @param javaRef Java reference to cluster group to be created.
                 * @return New cluster group implementation.
                 */
                SP_ClusterGroupImpl FromTarget(jobject javaRef);

                /**
                 * Gets instance of compute internally.
                 *
                 * @return Instance of compute.
                 */
                SP_ComputeImpl InternalGetCompute();

                /**
                 * Get container of refreshed cluster nodes over this cluster group..
                 *
                 * @return Instance of compute.
                 */
                ClusterNodes ClusterGroupImpl::RefreshNodes();

                /** Compute for the cluster group. */
                SP_ComputeImpl computeImpl;

                /** Cluster nodes. */
                ClusterNodes nodes;

                /** Cluster nodes top version. */
                int64_t topVer;
            };
        }
    }
}

#endif //_IGNITE_IMPL_CLUSTER_CLUSTER_GROUP_IMPL