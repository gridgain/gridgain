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

#ifndef _IGNITE_THIN_CLIENT_TEST_VECTOR_LOGGER
#define _IGNITE_THIN_CLIENT_TEST_VECTOR_LOGGER

#include <ignite/common/concurrent.h>

#include <ignite/impl/logger.h>

namespace ignite_test
{
    /**
     * Logger that stores all messages in vector.
     */
    class VectorLogger : public ignite::impl::Logger
    {
    public:
        /**
         * Logger message.
         */
        struct Event
        {
            ignite::impl::LogLevel::Type level;
            std::string message;
            std::string category;
            std::string errorInfo;
        };

        /**
         * Constructor.
         *
         * @param categoryFilter If is not empty, used as a category filter substring.
         * @param messageFilter If is not empty, used as a message filter substring.
         */
        VectorLogger(const std::string& categoryFilter, const std::string& messageFilter) :
            lock(),
            logEvents(),
            messageFilter(messageFilter),
            categoryFilter(categoryFilter)
        {
            // No-op.
        }

        /**
         * Logs the specified message.
         *
         * @param level The level.
         * @param message The message.
         * @param category The logging category name.
         * @param nativeErrorInfo The native error information.
         */
        virtual void Log(ignite::impl::LogLevel::Type level, const std::string& message, const std::string& category,
            const std::string& nativeErrorInfo)
        {
            if ((categoryFilter.empty() || category.find(categoryFilter) != std::string::npos) &&
                (messageFilter.empty() || message.find(messageFilter) != std::string::npos))
            {
                ignite::common::concurrent::CsLockGuard guard(lock);

                Event event;
                event.level = level;
                event.message = message;
                event.category = category;
                event.errorInfo = nativeErrorInfo;

                logEvents.push_back(event);
            }
        }

        /**
         * Determines whether the specified log level is enabled.
         *
         * @param level The level.
         * @return Value indicating whether the specified log level is enabled.
         */
        virtual bool IsEnabled(ignite::impl::LogLevel::Type level)
        {
            (void) level;

            return true;
        }

        /**
         * Get log events.
         * @return Log events.
         */
        std::vector<Event> GetEvents() const
        {
            ignite::common::concurrent::CsLockGuard guard(lock);

            std::vector<Event> res(logEvents);

            return res;
        }

    private:
        /** Lock. */
        mutable ignite::common::concurrent::CriticalSection lock;

        /** Events. */
        std::vector<Event> logEvents;

        /** Message filter. */
        std::string messageFilter;

        /** Category filter. */
        std::string categoryFilter;
    };
} //namespace ignite_test

#endif //_IGNITE_THIN_CLIENT_TEST_VECTOR_LOGGER
