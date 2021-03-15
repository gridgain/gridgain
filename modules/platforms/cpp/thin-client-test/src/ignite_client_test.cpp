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

#include <ignite/ignition.h>

#include <ignite/thin/ignite_client_configuration.h>
#include <ignite/thin/ignite_client.h>

#include <test_utils.h>
#include <vector_logger.h>

using namespace ignite::thin;
using namespace boost::unit_test;
using namespace ignite_test;

class IgniteClientTestSuiteFixture
{
public:
    IgniteClientTestSuiteFixture()
    {
        // No-op.
    }

    ~IgniteClientTestSuiteFixture()
    {
        ignite::Ignition::StopAll(false);
    }

    /**
     * Check that if client started with given configuration and connection limit then the actual number of active
     * connections is equal to the expected value.
     *
     * @param cfg Client configuration.
     * @param logger Logger.
     * @param limit Limit to set
     * @param expect Expected connections number.
     */
    void CheckConnectionsNum(IgniteClientConfiguration &cfg, VectorLogger* logger, uint32_t limit, size_t expect)
    {
        cfg.SetConnectionsLimit(limit);
        IgniteClient client = IgniteClient::Start(cfg);

        BOOST_CHECK_EQUAL(GetActiveConnections(logger), expect);
    }

    /**
     * Get Number of active connections.
     *
     * @param logger Logger.
     * @return Number of active connections.
     */
    static size_t GetActiveConnections(VectorLogger* logger)
    {
        typedef std::vector<VectorLogger::Event> Events;

        size_t connected = 0;
        size_t disconnected = 0;

        std::vector<VectorLogger::Event> logs = logger->GetEvents();
        for (Events::iterator it = logs.begin(); it != logs.end(); ++it)
        {
            if (it->message.find("Client connected") != std::string::npos)
                ++connected;

            if (it->message.find("Client disconnected") != std::string::npos)
                ++connected;
        }

        return connected - disconnected;
    }

    /**
     * Start node with logging.
     *
     * @param name Node name.
     * @param logger Logger.
     */
    static ignite::Ignite StartNodeWithLog(const std::string& name, VectorLogger* logger)
    {
        return ignite_test::StartCrossPlatformServerNode("cache.xml", name.c_str(), logger);
    }
};

BOOST_FIXTURE_TEST_SUITE(IgniteClientTestSuite, IgniteClientTestSuiteFixture)

BOOST_AUTO_TEST_CASE(IgniteClientConnection)
{
    ignite::Ignite serverNode = ignite_test::StartCrossPlatformServerNode("cache.xml", "ServerNode");

    IgniteClientConfiguration cfg;

    cfg.SetEndPoints("127.0.0.1:11110");

    IgniteClient::Start(cfg);
}

BOOST_AUTO_TEST_CASE(IgniteClientConnectionFailover)
{
    ignite::Ignite serverNode = ignite_test::StartCrossPlatformServerNode("cache.xml", "ServerNode");

    IgniteClientConfiguration cfg;

    cfg.SetEndPoints("127.0.0.1:11109..11111");

    IgniteClient::Start(cfg);
}

BOOST_AUTO_TEST_CASE(IgniteClientConnectionLimit)
{
    VectorLogger logger("connected");

    ignite::Ignite serverNode0 = StartNodeWithLog("Node0", &logger);
    ignite::Ignite serverNode1 = StartNodeWithLog("Node1", &logger);
    ignite::Ignite serverNode2 = StartNodeWithLog("Node2", &logger);

    IgniteClientConfiguration cfg;

    cfg.SetEndPoints("127.0.0.1:11110,127.0.0.1:11111,127.0.0.1:11112");

    CheckConnectionsNum(cfg, &logger, 0, 3);
    CheckConnectionsNum(cfg, &logger, 1, 1);
    CheckConnectionsNum(cfg, &logger, 2, 2);
    CheckConnectionsNum(cfg, &logger, 3, 3);
    CheckConnectionsNum(cfg, &logger, 4, 3);
    CheckConnectionsNum(cfg, &logger, 100500, 3);
}

BOOST_AUTO_TEST_SUITE_END()
