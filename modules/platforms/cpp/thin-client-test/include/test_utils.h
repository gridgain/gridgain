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

#ifndef _IGNITE_THIN_CLIENT_TEST_TEST_UTILS
#define _IGNITE_THIN_CLIENT_TEST_TEST_UTILS

#include <string>

#include <ignite/ignition.h>

#define MUTE_TEST_FOR_TEAMCITY \
    if (jetbrains::teamcity::underTeamcity()) \
    { \
        BOOST_TEST_MESSAGE("Muted on TeamCity because of periodical non-critical failures"); \
        BOOST_CHECK(jetbrains::teamcity::underTeamcity()); \
        return; \
    }

namespace ignite_test
{
    /**
     * @return Test config directory path.
     */
    std::string GetTestConfigDir();

    /**
     * Initialize configuration for a node.
     *
     * Inits Ignite node configuration from specified config file.
     * Config file is searched in path specified by IGNITE_NATIVE_TEST_CPP_THIN_CONFIG_PATH
     * environmental variable.
     *
     * @param cfg Ignite config.
     * @param cfgFile Ignite node config file name without path.
     */
    void InitConfig(ignite::IgniteConfiguration& cfg, const char* cfgFile);

    /**
     * Start Ignite node.
     *
     * Starts new Ignite node with the specified name and from specified config file.
     * Config file is searched in path specified by IGNITE_NATIVE_TEST_CPP_THIN_CONFIG_PATH
     * environmental variable.
     *
     * @param cfgFile Ignite node config file name without path.
     * @param name Node name.
     * @param logger Logger to use.
     * @return New node.
     */
    ignite::Ignite StartServerNode(const char* cfgFile, const char* name, ignite::impl::Logger* logger = 0);

    /**
     * Start Ignite node with config path corrected for specific platform.
     *
     * @param cfgFile Ignite node config file name without path.
     * @param name Node name.
     * @param logger Logger to use.
     * @return New node.
     */
    ignite::Ignite StartCrossPlatformServerNode(const char* cfgFile, const char* name, ignite::impl::Logger* logger = 0);

    /**
     * Remove all the LFS artifacts.
     */
    void ClearLfs();
}

#endif // _IGNITE_THIN_CLIENT_TEST_TEST_UTILS