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

#ifndef _IGNITE_ODBC_TEST_TEST_UTILS
#define _IGNITE_ODBC_TEST_TEST_UTILS

#ifdef _WIN32
#   include <windows.h>
#endif

#include <sql.h>
#include <sqlext.h>

#include <string>

#include "ignite/ignition.h"

#define ODBC_THROW_ON_ERROR(ret, type, handle)          \
    if (!SQL_SUCCEEDED(ret))                            \
    {                                                   \
        throw ignite_test::GetOdbcError(type, handle);  \
    }

#define ODBC_FAIL_ON_ERROR(ret, type, handle)                       \
    if (!SQL_SUCCEEDED(ret))                                        \
    {                                                               \
        Ignition::StopAll(true);                                    \
        BOOST_FAIL(ignite_test::GetOdbcErrorMessage(type, handle)); \
    }

#define ODBC_FAIL_ON_ERROR1(ret, type, handle, msg)                                    \
    if (!SQL_SUCCEEDED(ret))                                                           \
    {                                                                                  \
        Ignition::StopAll(true);                                                       \
        BOOST_FAIL(ignite_test::GetOdbcErrorMessage(type, handle) + ", msg = " + msg); \
    }

#define MUTE_TEST_FOR_TEAMCITY \
    if (jetbrains::teamcity::underTeamcity()) \
    { \
        BOOST_TEST_MESSAGE("Muted on TeamCity because of periodical non-critical failures"); \
        BOOST_CHECK(jetbrains::teamcity::underTeamcity()); \
        return; \
    }

/**
 * Client ODBC erorr.
 */
class OdbcClientError : public std::exception
{
public:
    /**
     * Constructor
     *
     * @param sqlstate SQL state.
     * @param message Error message.
     */
    OdbcClientError(const std::string& sqlstate, const std::string& message) :
        sqlstate(sqlstate),
        message(message)
    {
        // No-op.
    }

    /**
     * Destructor.
     */
    virtual ~OdbcClientError() IGNITE_NO_THROW
    {
         // No-op.
    }

    /**
     * Implementation of the standard std::exception::what() method.
     * Synonym for GetText() method.
     *
     * @return Error message string.
     */
    virtual const char* what() const IGNITE_NO_THROW
    {
        return message.c_str();
    }

    /** SQL state. */
    std::string sqlstate;

    /** Error message. */
    std::string message;
};

namespace ignite_test
{
    /** Read buffer size. */
    enum { ODBC_BUFFER_SIZE = 1024 };

    /**
     * Extract error.
     *
     * @param handleType Type of the handle.
     * @param handle Handle.
     * @return Error.
     */
    OdbcClientError GetOdbcError(SQLSMALLINT handleType, SQLHANDLE handle);

    /**
     * Extract error state.
     *
     * @param handleType Type of the handle.
     * @param handle Handle.
     * @param idx Error record index.
     * @return Error state.
     */
    std::string GetOdbcErrorState(SQLSMALLINT handleType, SQLHANDLE handle, int idx = 1);

    /**
     * Extract error message.
     *
     * @param handleType Type of the handle.
     * @param handle Handle.
     * @param idx Error record index.
     * @return Error message.
     */
    std::string GetOdbcErrorMessage(SQLSMALLINT handleType, SQLHANDLE handle, int idx = 1);

    /**
     * @return Test config directory path.
     */
    std::string GetTestConfigDir();

    /**
     * Initialize configuration for a node.
     *
     * Inits Ignite node configuration from specified config file.
     * Config file is searched in path specified by IGNITE_NATIVE_TEST_CPP_CONFIG_PATH
     * environmental variable.
     *
     * @param cfg Ignite config.
     * @param cfgFile Ignite node config file name without path.
     */
    void InitConfig(ignite::IgniteConfiguration& cfg, const char* cfgFile);

    /**
     * Start node with the config for the current platform.
     *
     * @param cfg Basic config path. Changed to platform config if needed.
     * @param name Instance name.
     */
    ignite::Ignite StartPlatformNode(const char* cfg, const char* name);

    /**
     * Remove all the LFS artifacts.
     */
    void ClearLfs();
}

#endif // _IGNITE_ODBC_TEST_TEST_UTILS
