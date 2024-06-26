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

#include <fstream>

#include <ignite/common/platform_utils.h>

#include "test_utils.h"

namespace ignite
{
    Ignite IGNITE_IMPORT_EXPORT StartIgnite(const IgniteConfiguration& cfg, const char* name, IgniteError& err,
        impl::Logger* logger);
}

namespace ignite_test
{
    std::string GetTestConfigDir()
    {
        using namespace ignite;

        std::string cfgPath = common::GetEnv("IGNITE_NATIVE_TEST_CPP_THIN_CONFIG_PATH");

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
             << "thin-client-test" << common::Fs
             << "config";

        return path.str();
    }

    void InitConfig(ignite::IgniteConfiguration& cfg, const std::string& cfgFile)
    {
        using namespace ignite;

        assert(!cfgFile.empty());

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
        // cfg.jvmOpts.push_back("-Djavax.net.debug=ssl");

        cfg.igniteHome = jni::ResolveIgniteHome();
        cfg.jvmClassPath = jni::CreateIgniteHomeClasspath(cfg.igniteHome, true);

#ifdef IGNITE_TESTS_32
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

        path << cfgDir << common::Fs << cfgFile;

        cfg.springCfgPath = path.str();
    }

    ignite::Ignite StartServerNode(const std::string& cfgFile, const std::string& name, ignite::impl::Logger* logger)
    {
        using namespace ignite;

        assert(!name.empty());

        IgniteConfiguration cfg;

        InitConfig(cfg, cfgFile);

        IgniteError err;

        Ignite node = ignite::StartIgnite(cfg, name.c_str(), err, logger);

        IgniteError::ThrowIfNeeded(err);

        return node;
    }

    ignite::Ignite StartCrossPlatformServerNode(const std::string& cfgFile, const std::string& name, ignite::impl::Logger* logger)
    {
        std::string config(cfgFile);

#ifdef IGNITE_TESTS_32
        // Cutting off the ".xml" part.
        config.resize(config.size() - 4);
        config += "-32.xml";
#endif //IGNITE_TESTS_32

        return StartServerNode(config.c_str(), name, logger);
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
