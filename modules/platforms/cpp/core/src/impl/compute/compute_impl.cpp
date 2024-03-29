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

#include <ignite/impl/compute/compute_impl.h>

using namespace ignite::common::concurrent;

namespace ignite
{
    namespace impl
    {
        namespace compute
        {
            ComputeImpl::ComputeImpl(SharedPointer<IgniteEnvironment> env, cluster::SP_ClusterGroupImpl clusterGroup) :
                InteropTarget(env, clusterGroup.Get()->GetComputeProcessor()),
                clusterGroup(clusterGroup)
            {
                // No-op.
            }

            bool ComputeImpl::ProjectionContainsPredicate() const
            {
                return clusterGroup.IsValid() && clusterGroup.Get()->GetPredicate() != 0;
            }

            std::vector<ignite::cluster::ClusterNode> ComputeImpl::GetNodes()
            {
                return clusterGroup.Get()->GetNodes();
            }
        }
    }
}