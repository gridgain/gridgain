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

#ifndef _IGNITE_IMPL_LOGGER
#define _IGNITE_IMPL_LOGGER

#include <string>

#include <ignite/common/common.h>

namespace ignite
{
    namespace impl
    {
        struct LogLevel
        {
            /**
             * Defines log levels.
             */
            enum Type
            {
                /** Trace log level. */
                LEVEL_TRACE = 0,

                /** Debug log level. */
                LEVEL_DEBUG = 1,

                /** Info log level. */
                LEVEL_INFO = 2,

                /** Warning log level. */
                LEVEL_WARN = 3,

                /** Error log level. */
                LEVEL_ERROR = 4
            };
        };

        /**
         * Logger abstract interface.
         *
         * Can be used to implement native logger to override java logging.
         */
        class IGNITE_IMPORT_EXPORT Logger
        {
        public:
            /**
             * Destructor.
             */
            virtual ~Logger()
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
            virtual void Log(LogLevel::Type level, const std::string& message, const std::string& category,
                const std::string& nativeErrorInfo) = 0;

            /**
             * Determines whether the specified log level is enabled.
             *
             * @param level The level.
             * @return Value indicating whether the specified log level is enabled.
             */
            virtual bool IsEnabled(LogLevel::Type level) = 0;
        }
    }
}

#endif //_IGNITE_IMPL_LOGGER
