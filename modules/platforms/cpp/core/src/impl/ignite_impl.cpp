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

#include "ignite/impl/ignite_impl.h"

using namespace ignite::common::concurrent;
using namespace ignite::cluster;
using namespace ignite::jni::java;
using namespace ignite::impl::interop;
using namespace ignite::impl::binary;

using namespace ignite::binary;

namespace ignite
{
    namespace impl
    {
        IgniteImpl::IgniteImpl(SharedPointer<IgniteEnvironment> env) :
            InteropTarget(env, static_cast<jobject>(env.Get()->GetProcessor()), true),
            env(env),
            txImpl(),
            prjImpl()
        {
            txImpl.Init(common::Bind(this, &IgniteImpl::InternalGetTransactions));
            prjImpl.Init(common::Bind(this, &IgniteImpl::InternalGetProjection));
        }

        const char* IgniteImpl::GetName() const
        {
            return env.Get()->InstanceName();
        }

        const IgniteConfiguration& IgniteImpl::GetConfiguration() const
        {
            return env.Get()->GetConfiguration();
        }

        JniContext* IgniteImpl::GetContext()
        {
            return env.Get()->Context();
        }

        IgniteImpl::SP_IgniteBindingImpl IgniteImpl::GetBinding()
        {
            return env.Get()->GetBinding();
        }

        IgniteImpl::SP_IgniteClusterImpl IgniteImpl::GetCluster()
        {
            return IgniteImpl::SP_IgniteClusterImpl(new cluster::IgniteClusterImpl(this->GetProjection()));
        }

        IgniteImpl::SP_ComputeImpl IgniteImpl::GetCompute()
        {
            cluster::SP_ClusterGroupImpl serversCluster = prjImpl.Get().Get()->ForServers();

            return serversCluster.Get()->GetCompute();
        }

        IgniteImpl::SP_ComputeImpl IgniteImpl::GetCompute(ClusterGroup grp)
        {
            return this->GetProjection().Get()->GetCompute(grp);
        }

        transactions::TransactionsImpl* IgniteImpl::InternalGetTransactions()
        {
            IgniteError err;

            jobject txJavaRef = InOpObject(ProcessorOp::GET_TRANSACTIONS, err);

            IgniteError::ThrowIfNeeded(err);

            if (!txJavaRef)
                throw IgniteError(IgniteError::IGNITE_ERR_GENERIC, "Can not get Transactions instance.");

            return new transactions::TransactionsImpl(env, txJavaRef);
        }

        cluster::ClusterGroupImpl* IgniteImpl::InternalGetProjection()
        {
            IgniteError err;

            jobject clusterGroupJavaRef = InOpObject(ProcessorOp::GET_CLUSTER_GROUP, err);

            IgniteError::ThrowIfNeeded(err);

            if (!clusterGroupJavaRef)
                throw IgniteError(IgniteError::IGNITE_ERR_GENERIC, "Can not get ClusterGroup instance.");

            return new cluster::ClusterGroupImpl(env, clusterGroupJavaRef);
        }

        cache::CacheImpl* IgniteImpl::GetOrCreateCache(const char* name, IgniteError& err, int32_t op)
        {
            SharedPointer<InteropMemory> mem = env.Get()->AllocateMemory();
            InteropMemory* mem0 = mem.Get();
            InteropOutputStream out(mem0);
            BinaryWriterImpl writer(&out, env.Get()->GetTypeManager());
            BinaryRawWriter rawWriter(&writer);

            rawWriter.WriteString(name);

            out.Synchronize();

            jobject cacheJavaRef = InStreamOutObject(op, *mem0, err);

            if (!cacheJavaRef)
            {
                return NULL;
            }

            char* name0 = common::CopyChars(name);

            return new cache::CacheImpl(name0, env, cacheJavaRef);
        }
    }
}
