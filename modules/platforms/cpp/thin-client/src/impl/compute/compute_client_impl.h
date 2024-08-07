/*
 * Copyright 2021 GridGain Systems, Inc. and Contributors.
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

#ifndef _IGNITE_IMPL_THIN_COMPUTE_COMPUTE_CLIENT
#define _IGNITE_IMPL_THIN_COMPUTE_COMPUTE_CLIENT

#include <ignite/impl/thin/writable.h>
#include <ignite/impl/thin/readable.h>
#include "impl/data_router.h"

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            namespace compute
            {
                /**
                 * Thin client Compute implementation.
                 */
                class IGNITE_IMPORT_EXPORT ComputeClientImpl
                {
                public:
                    /**
                     * Constructor.
                     *
                     * @param router Data router instance.
                     */
                    ComputeClientImpl(const SP_DataRouter& router) :
                        router(router)
                    {
                        // No-op.
                    }

                    /**
                     * Destructor.
                     */
                    ~ComputeClientImpl()
                    {
                        // No-op.
                    }

                    /**
                     * Execute java task internally.
                     *
                     * @param flags Flags.
                     * @param timeout Timeout.
                     * @param taskName Task name.
                     * @param wrArg Argument.
                     * @param res Result.
                     */
                    void ExecuteJavaTask(int8_t flags, int64_t timeout, const std::string& taskName, Writable& wrArg,
                        Readable& res);

                private:
                    /** Data router. */
                    SP_DataRouter router;

                    IGNITE_NO_COPY_ASSIGNMENT(ComputeClientImpl);
                };

                typedef ignite::common::concurrent::SharedPointer<ComputeClientImpl> SP_ComputeClientImpl;
            }
        }
    }
}

#endif // _IGNITE_IMPL_THIN_COMPUTE_COMPUTE_CLIENT
