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

#ifndef _IGNITE_ODBC_SYSTEM_SYSTEM_DSN
#define _IGNITE_ODBC_SYSTEM_SYSTEM_DSN

#include <ignite/odbc/system/odbc_constants.h>

namespace ignite
{
    namespace odbc
    {
        namespace config
        {
            class Configuration;
        }
    }
}

/**
 * Display connection window for user to configure connection parameters.
 *
 * @param hwndParent Parent window handle.
 * @param config Output configuration.
 * @return True on success and false on fail.
 */
bool DisplayConnectionWindow(HWND hwndParent, ignite::odbc::config::Configuration& config);

#endif //_IGNITE_ODBC_SYSTEM_SYSTEM_DSN
