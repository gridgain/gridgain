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

#include <boost/test/unit_test.hpp>

#include <cassert>

#include <ignite/common/platform_utils.h>

#include "test_utils.h"

namespace ignite_test
{
    OdbcClientError GetOdbcError(SQLSMALLINT handleType, SQLHANDLE handle)
    {
        SQLCHAR sqlstate[7] = {};
        SQLINTEGER nativeCode;

        SQLCHAR message[ODBC_BUFFER_SIZE];
        SQLSMALLINT reallen = 0;

        SQLGetDiagRec(handleType, handle, 1, sqlstate, &nativeCode, message, ODBC_BUFFER_SIZE, &reallen);

        return OdbcClientError(
            std::string(reinterpret_cast<char*>(sqlstate)),
            std::string(reinterpret_cast<char*>(message), reallen));
    }

    std::string GetOdbcErrorState(SQLSMALLINT handleType, SQLHANDLE handle, int idx)
    {
        SQLCHAR sqlstate[7] = {};
        SQLINTEGER nativeCode;

        SQLCHAR message[ODBC_BUFFER_SIZE];
        SQLSMALLINT reallen = 0;

        SQLGetDiagRec(handleType, handle, idx, sqlstate, &nativeCode, message, ODBC_BUFFER_SIZE, &reallen);

        return std::string(reinterpret_cast<char*>(sqlstate));
    }

    std::string GetOdbcErrorMessage(SQLSMALLINT handleType, SQLHANDLE handle, int idx)
    {
        SQLCHAR sqlstate[7] = {};
        SQLINTEGER nativeCode;

        SQLCHAR message[ODBC_BUFFER_SIZE];
        SQLSMALLINT reallen = 0;

        SQLGetDiagRec(handleType, handle, idx, sqlstate, &nativeCode, message, ODBC_BUFFER_SIZE, &reallen);

        std::string res(reinterpret_cast<char*>(sqlstate));

        if (!res.empty())
            res.append(": ").append(reinterpret_cast<char*>(message), reallen);
        else
            res = "No results";

        return res;
    }

    std::string GetTestConfigDir()
    {
        using namespace ignite;

        std::string cfgPath = common::GetEnv("IGNITE_NATIVE_TEST_ODBC_CONFIG_PATH");

        if (!cfgPath.empty())
            return cfgPath;

        std::string home = jni::ResolveIgniteHome();

        if (home.empty())
            return home;

        std::stringstream path;

        path << home << common::Fs
             << "modules" << common::Fs
             << "platforms" << common::Fs
             << "cpp" << common::Fs
             << "odbc-test" << common::Fs
             << "config";

        return path.str();
    }

    void InitConfig(ignite::IgniteConfiguration& cfg, const char* cfgFile)
    {
        using namespace ignite;

        assert(cfgFile != 0);
        std::string cfgPath(cfgFile);

        cfg.jvmOpts.push_back("-Xdebug");
        cfg.jvmOpts.push_back("-Xnoagent");
        cfg.jvmOpts.push_back("-Djava.compiler=NONE");
        cfg.jvmOpts.push_back("-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005");
        cfg.jvmOpts.push_back("-XX:+HeapDumpOnOutOfMemoryError");
        cfg.jvmOpts.push_back("-Duser.timezone=GMT");
        cfg.jvmOpts.push_back("-DIGNITE_QUIET=false");
        cfg.jvmOpts.push_back("-DIGNITE_UPDATE_NOTIFIER=false");
        cfg.jvmOpts.push_back("-DIGNITE_LOG_CLASSPATH_CONTENT_ON_STARTUP=false");
        cfg.jvmOpts.push_back("-Duser.language=en");
        // Un-comment to debug SSL
        //cfg.jvmOpts.push_back("-Djavax.net.debug=ssl");

        cfg.igniteHome = jni::ResolveIgniteHome();
        cfg.jvmClassPath = jni::CreateIgniteHomeClasspath(cfg.igniteHome, true);

#ifdef IGNITE_TESTS_32
        // Cutting off the ".xml" part.
        cfgPath.resize(cfgPath.size() - 4);
        cfgPath += "-32.xml";

        cfg.jvmInitMem = 256;
        cfg.jvmMaxMem = 768;
#else
        cfg.jvmInitMem = 1024;
        cfg.jvmMaxMem = 4096;
#endif

        std::string cfgDir = GetTestConfigDir();

        if (cfgDir.empty())
            throw IgniteError(IgniteError::IGNITE_ERR_GENERIC, "Failed to resolve test config directory");

        std::stringstream path;

        path << cfgDir << common::Fs << cfgPath;

        cfg.springCfgPath = path.str();
    }

    ignite::Ignite StartPlatformNode(const char* cfg, const char* name)
    {
        using namespace ignite;

        assert(name != 0);

        IgniteConfiguration config;

        InitConfig(config, cfg);

        return Ignition::Start(config, name);
    }

    std::string AppendPath(const std::string& base, const std::string& toAdd)
    {
        std::stringstream stream;

        stream << base << ignite::common::Fs << toAdd;

        return stream.str();
    }

    void ClearLfs()
    {
        std::string home = ignite::jni::ResolveIgniteHome();
        std::string workDir = AppendPath(home, "work");

        ignite::common::DeletePath(workDir);
    }
}
