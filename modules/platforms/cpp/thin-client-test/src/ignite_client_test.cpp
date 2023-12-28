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
#include <test_server.h>

#include <boost/test/unit_test.hpp>
#include <boost/bind.hpp>

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
    IgniteClientTestSuiteFixture() :
        logger("org.apache.ignite.internal.processors.odbc.ClientListenerNioListener", "connected")
    {
        // No-op.
    }

    ~IgniteClientTestSuiteFixture()
    {
        ignite::Ignition::StopAll(false);
    }

    /**
     * Wait for connections.
     * @param logger Logger.
     * @param expect Expected connections number.
     * @param timeout Timeout.
     * @return True if condition was met, false if timeout has been reached.
     */
    static bool WaitForConnections(VectorLogger* logger, size_t expected, int32_t timeout = 5000)
    {
        return ignite_test::WaitForCondition(
                boost::bind(&IgniteClientTestSuiteFixture::CheckActiveConnections, logger, expected),
                timeout);
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

        BOOST_CHECK(WaitForConnections(logger, expect));
        BOOST_CHECK_EQUAL(GetActiveConnections(logger), expect);
    }
    /**
     * Check that client started with specified size of user thread pool started exactly the specified number of threads
     * in thread pool.
     *
     * @param cfg Client configuration.
     * @param num Expected thread number
     */
    static void CheckThreadsNum(IgniteClientConfiguration &cfg, uint32_t num)
    {
        ignite::TestServer server;
        server.PushHandshakeResponse(true);
        server.Start();

        int32_t threadsBefore = ignite::common::concurrent::GetThreadsCount();
        int32_t netThreads = 1;

#ifdef _WIN32
        // In Windows there is one additional thread for connecting.
        netThreads += 1;
#endif
        int32_t threadsExpected = static_cast<int32_t>(num) + netThreads;

        cfg.SetUserThreadPoolSize(num);
        {
            IgniteClient client = IgniteClient::Start(cfg);

            int32_t threadsActual = ignite::common::concurrent::GetThreadsCount() - threadsBefore;

            BOOST_CHECK_EQUAL(threadsExpected, threadsActual);
        }

        int32_t threadsAfter = ignite::common::concurrent::GetThreadsCount();

        BOOST_CHECK_EQUAL(threadsBefore, threadsAfter);
        BOOST_CHECK_EQUAL(num, cfg.GetUserThreadPoolSize());

        server.Stop();
    }

    /**
     * Check number of active connections.
     *
     * @param logger Logger.
     * @param expect connections to expect.
     * @return @c true on success.
     */
    static bool CheckActiveConnections(VectorLogger* logger, size_t expect)
    {
        return GetActiveConnections(logger) == expect;
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
            if (it->message.find("Client connected: ") != std::string::npos)
                ++connected;

            if (it->message.find("Client disconnected: ") != std::string::npos)
                ++disconnected;
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
        return ignite_test::StartCrossPlatformServerNode("cache.xml", "ServerNode" + name, logger);
    }

protected:
    /** Logger. */
    VectorLogger logger;
};

BOOST_FIXTURE_TEST_SUITE(IgniteClientTestSuite, IgniteClientTestSuiteFixture)

BOOST_AUTO_TEST_CASE(IgniteClientConnection)
{
    ignite::Ignite serverNode = StartNodeWithLog("0", &logger);

    IgniteClientConfiguration cfg;

    cfg.SetEndPoints("127.0.0.1:11110");

    IgniteClient client = IgniteClient::Start(cfg);

    BOOST_CHECK(WaitForConnections(&logger, 1));
    BOOST_CHECK_EQUAL(GetActiveConnections(&logger), 1);
}

BOOST_AUTO_TEST_CASE(IgniteClientConnectionFailover)
{
    ignite::Ignite serverNode = StartNodeWithLog("0", &logger);

    IgniteClientConfiguration cfg;

    cfg.SetEndPoints("127.0.0.1:11109..11111");

    IgniteClient client = IgniteClient::Start(cfg);

    BOOST_CHECK(WaitForConnections(&logger, 1));
    BOOST_CHECK_EQUAL(GetActiveConnections(&logger), 1);
}

BOOST_AUTO_TEST_CASE(IgniteClientConnectionLimit)
{
    ignite::Ignite serverNode0 = StartNodeWithLog("0", &logger);
    ignite::Ignite serverNode1 = StartNodeWithLog("1", &logger);
    ignite::Ignite serverNode2 = StartNodeWithLog("2", &logger);

    IgniteClientConfiguration cfg;

    cfg.SetEndPoints("127.0.0.1:11110,127.0.0.1:11111,127.0.0.1:11112");

    CheckConnectionsNum(cfg, &logger, 0, 3);
    CheckConnectionsNum(cfg, &logger, 1, 1);
    CheckConnectionsNum(cfg, &logger, 2, 2);
    CheckConnectionsNum(cfg, &logger, 3, 3);
    CheckConnectionsNum(cfg, &logger, 4, 3);
    CheckConnectionsNum(cfg, &logger, 100500, 3);

    ignite::Ignition::StopAll(true);
}

BOOST_AUTO_TEST_CASE(IgniteClientReconnect)
{
    MUTE_TEST_FOR_TEAMCITY;

    ignite::Ignite serverNode0 = StartNodeWithLog("0", &logger);
    ignite::Ignite serverNode1 = StartNodeWithLog("1", &logger);

    IgniteClientConfiguration cfg;

    cfg.SetEndPoints("127.0.0.1:11110,127.0.0.1:11111,127.0.0.1:11112");

    IgniteClient client = IgniteClient::Start(cfg);

    BOOST_CHECK(WaitForConnections(&logger, 2));
    BOOST_CHECK_EQUAL(GetActiveConnections(&logger), 2);

    ignite::Ignite serverNode2 = StartNodeWithLog("2", &logger);

    BOOST_CHECK(WaitForConnections(&logger, 3));
    BOOST_CHECK_EQUAL(GetActiveConnections(&logger), 3);

    ignite::Ignition::Stop(serverNode1.GetName(), true);

    BOOST_CHECK(WaitForConnections(&logger, 2));
    BOOST_CHECK_EQUAL(GetActiveConnections(&logger), 2);

    serverNode1 = StartNodeWithLog("1", &logger);

    BOOST_CHECK(WaitForConnections(&logger, 3, 20000));
    BOOST_CHECK_EQUAL(GetActiveConnections(&logger), 3);

    ignite::Ignition::StopAll(true);

    BOOST_CHECK(WaitForConnections(&logger, 0));
    BOOST_CHECK_EQUAL(GetActiveConnections(&logger), 0);

    BOOST_REQUIRE_THROW((client.GetOrCreateCache<int, int>("test")), ignite::IgniteError);
}

BOOST_AUTO_TEST_CASE(IgniteClientUserThreadPoolSize)
{
    IgniteClientConfiguration cfg;

    BOOST_CHECK_EQUAL(1, cfg.GetUserThreadPoolSize());

    cfg.SetEndPoints("127.0.0.1:11110");

    CheckThreadsNum(cfg, 1);
    CheckThreadsNum(cfg, 2);
    CheckThreadsNum(cfg, 3);
    CheckThreadsNum(cfg, 4);
    CheckThreadsNum(cfg, 8);
    CheckThreadsNum(cfg, 16);
    CheckThreadsNum(cfg, 128);
}

BOOST_AUTO_TEST_SUITE_END()
