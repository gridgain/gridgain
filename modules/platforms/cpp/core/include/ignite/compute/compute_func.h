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
 * Declares ignite::compute::ComputeFunc class template.
 */

#ifndef _IGNITE_COMPUTE_COMPUTE_FUNC
#define _IGNITE_COMPUTE_COMPUTE_FUNC

namespace ignite
{
    class Ignite;
    class IgniteBinding;

    namespace compute
    {
        /**
         * Interface for a simple compute function that can be serialized and
         * called on the remote nodes. ignite::binary::BinaryType class template
         * should be specialized for any class, inheriting from this class.
         *
         * @tparam R Call return type. BinaryType should be specialized for the
         *  type if it is not primitive.
         */
        template<typename R>
        class ComputeFunc
        {
            template<typename TF, typename TR>
            friend class ignite::impl::compute::ComputeJobHolderImpl;
            friend class ignite::IgniteBinding;

            typedef R ReturnType;
        public:
            /**
             * Constructor.
             */
            ComputeFunc() :
                ignite(0)
            {
                // No-op.
            }

            /**
             * Destructor.
             */
            virtual ~ComputeFunc()
            {
                // No-op.
            }

            /**
             * Called upon execution by compute.
             *
             * @return Computation result.
             */
            virtual R Call() = 0;

        protected:
            /*
             * Get ignite node pointer.
             * Return pointer to the node on which this function was called.
             *
             * @return Ignite node pointer.
             */
            Ignite& GetIgnite()
            {
                assert(ignite != 0);

                return *ignite;
            }

        private:
            /*
             * Set ignite node pointer.
             *
             * @param ignite Ignite node pointer.
             */
            void SetIgnite(Ignite* ignite)
            {
                this->ignite = ignite;
            }

            /** Ignite node pointer. */
            Ignite* ignite;
        };
    }
}

#endif //_IGNITE_COMPUTE_COMPUTE_FUNC
