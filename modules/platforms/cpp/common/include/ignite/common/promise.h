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
 * Declares ignite::commom::Promise class template.
 */


#ifndef _IGNITE_PROMISE
#define _IGNITE_PROMISE

#include <ignite/common/common.h>
#include <ignite/common/shared_state.h>

#include <ignite/ignite_error.h>
#include <ignite/future.h>

namespace ignite
{
    namespace common
    {
        /**
         * Promise class template. Used to set result of the asynchroniously
         * started computation.
         *
         * @tparam T Promised value type.
         */
        template<typename T>
        class Promise
        {
        public:
            /** Template value type */
            typedef T ValueType;

            /**
             * Constructor.
             */
            Promise() :
                state(new SharedState<ValueType>())
            {
                // No-op.
            }

            /**
             * Destructor.
             */
            ~Promise()
            {
                SharedState<ValueType>* state0 = state.Get();

                assert(state0 != 0);

                if (!state0->IsSet())
                    state0->SetError(IgniteError(IgniteError::IGNITE_ERR_FUTURE_STATE,
                        "Broken promise. Value will never be set due to internal error."));
            }


            /**
             * Get future for this promise.
             *
             * @return New future instance.
             */
            Future<ValueType> GetFuture() const
            {
                return Future<ValueType>(state);
            }

            /**
             * Set value.
             *
             * @throw IgniteError with IgniteError::IGNITE_ERR_FUTURE_STATE if error or value has been set already.
             * @param val Value to set.
             */
            void SetValue(std::auto_ptr<ValueType> val)
            {
                SharedState<ValueType>* state0 = state.Get();

                assert(state0 != 0);

                return state0->SetValue(val);
            }

            /**
             * Set error.
             *
             * @throw IgniteError with IgniteError::IGNITE_ERR_FUTURE_STATE if error or value has been set already.
             * @param err Error to set.
             */
            void SetError(const IgniteError& err)
            {
                SharedState<ValueType>* state0 = state.Get();

                assert(state0 != 0);

                state0->SetError(err);
            }

            /**
             * Set cancel target.
             */
            void SetCancelTarget(std::auto_ptr<Cancelable>& target)
            {
                state.Get()->SetCancelTarget(target);
            }

        private:
            IGNITE_NO_COPY_ASSIGNMENT(Promise);

            /** Shared state. */
            concurrent::SharedPointer< SharedState<ValueType> > state;
        };

        /**
         * Specialization for void.
         */
        template<>
        class Promise<void>
        {
        public:
            /** Template value type */
            typedef void ValueType;

            /**
             * Constructor.
             */
            Promise() :
                state(new SharedState<ValueType>())
            {
                // No-op.
            }

            /**
             * Destructor.
             */
            ~Promise()
            {
                SharedState<ValueType>* state0 = state.Get();

                assert(state0 != 0);

                if (!state0->IsSet())
                    state0->SetError(IgniteError(IgniteError::IGNITE_ERR_FUTURE_STATE,
                        "Broken promise. Value will never be set due to internal error."));
            }


            /**
             * Get future for this promise.
             *
             * @return New future instance.
             */
            Future<ValueType> GetFuture() const
            {
                return Future<ValueType>(state);
            }

            /**
             * Mark as complete.
             *
             * @throw IgniteError with IgniteError::IGNITE_ERR_FUTURE_STATE if error or value has been set already.
             */
            void SetValue()
            {
                SharedState<ValueType>* state0 = state.Get();

                assert(state0 != 0);

                return state0->SetValue();
            }

            /**
             * Set error.
             *
             * @throw IgniteError with IgniteError::IGNITE_ERR_FUTURE_STATE if error or value has been set already.
             * @param err Error to set.
             */
            void SetError(const IgniteError& err)
            {
                SharedState<ValueType>* state0 = state.Get();

                assert(state0 != 0);

                state0->SetError(err);
            }

            /**
             * Set cancel target.
             */
            void SetCancelTarget(std::auto_ptr<Cancelable>& target)
            {
                state.Get()->SetCancelTarget(target);
            }

        private:
            IGNITE_NO_COPY_ASSIGNMENT(Promise);

            /** Shared state. */
            concurrent::SharedPointer< SharedState<ValueType> > state;
        };

        /**
         * Specialization for SharePointer type.
         */
        template<typename T>
        class Promise< concurrent::SharedPointer<T> >
        {
        public:
            /** Template value type */
            typedef T ValueType;

            /** Template value type wrapped in shared pointer */
            typedef concurrent::SharedPointer<ValueType> SP_ValueType;

            /**
             * Constructor.
             */
            Promise() :
                state(new SharedState<SP_ValueType>())
            {
                // No-op.
            }

            /**
             * Destructor.
             */
            ~Promise()
            {
                SharedState<SP_ValueType>* state0 = state.Get();

                assert(state0 != 0);

                if (!state0->IsSet())
                    state0->SetError(IgniteError(IgniteError::IGNITE_ERR_FUTURE_STATE,
                        "Broken promise. Value will never be set due to internal error."));
            }


            /**
             * Get future for this promise.
             *
             * @return New future instance.
             */
            Future<SP_ValueType> GetFuture() const
            {
                return Future<SP_ValueType>(state);
            }

            /**
             * Set value.
             *
             * @throw IgniteError with IgniteError::IGNITE_ERR_FUTURE_STATE if error or value has been set already.
             * @param val Value to set.
             */
            void SetValue(SP_ValueType val)
            {
                SharedState<SP_ValueType>* state0 = state.Get();

                assert(state0 != 0);

                return state0->SetValue(val);
            }

            /**
             * Set error.
             *
             * @throw IgniteError with IgniteError::IGNITE_ERR_FUTURE_STATE if error or value has been set already.
             * @param err Error to set.
             */
            void SetError(const IgniteError& err)
            {
                SharedState<SP_ValueType>* state0 = state.Get();

                assert(state0 != 0);

                state0->SetError(err);
            }

            /**
             * Set cancel target.
             */
            void SetCancelTarget(std::auto_ptr<Cancelable>& target)
            {
                state.Get()->SetCancelTarget(target);
            }

        private:
            IGNITE_NO_COPY_ASSIGNMENT(Promise);

            /** Shared state. */
            concurrent::SharedPointer< SharedState<SP_ValueType> > state;
        };
    }
}

#endif //_IGNITE_PROMISE
