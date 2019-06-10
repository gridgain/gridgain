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
#include <ignite/test_utils.h>

using namespace ignite;
using namespace ignite::common;
using namespace ignite::common::concurrent;
using namespace ignite::cluster;

using namespace boost::unit_test;

/*
 * Test setup fixture.
 */
struct ClusterTestSuiteFixture
{
    Ignite node;

    /*
     * Constructor.
     */
    ClusterTestSuiteFixture() :
#ifdef IGNITE_TESTS_32
        node(ignite_test::StartNode("cache-test-32.xml", "ClusterTest"))
#else
        node(ignite_test::StartNode("cache-test.xml", "ClusterTest"))
#endif
    {
        // No-op.
    }

    /*
     * Destructor.
     */
    ~ClusterTestSuiteFixture()
    {
        Ignition::StopAll(true);
    }
};

/*
 * Test setup fixture.
 */
struct ClusterTestSuiteFixtureIsolated
{
    Ignite node;

    /*
     * Constructor.
     */
    ClusterTestSuiteFixtureIsolated() :
#ifdef IGNITE_TESTS_32
        node(ignite_test::StartNode("isolated-32.xml", "ClusterTestIsolated"))
#else
        node(ignite_test::StartNode("isolated.xml", "ClusterTestIsolated"))
#endif
    {
        // No-op.
    }

    /*
     * Destructor.
     */
    ~ClusterTestSuiteFixtureIsolated()
    {
        Ignition::StopAll(true);
    }
};

BOOST_FIXTURE_TEST_SUITE(ClusterTestSuite, ClusterTestSuiteFixture)

BOOST_AUTO_TEST_CASE(IgniteGetCluster)
{
    IgniteCluster cluster = node.GetCluster();

    BOOST_REQUIRE(cluster.IsActive());
}

BOOST_AUTO_TEST_CASE(IgniteGetNodes)
{
    IgniteCluster cluster = node.GetCluster();

    BOOST_REQUIRE(cluster.IsActive());

    ClusterGroup group = cluster.AsClusterGroup();

    std::vector<ClusterNode> nodes = group.GetNodes();

    BOOST_REQUIRE(nodes.size() == 1);
}

BOOST_AUTO_TEST_CASE(IgniteForAttribute)
{
    IgniteCluster cluster = node.GetCluster();

    BOOST_REQUIRE(cluster.IsActive());

    ClusterGroup group1 = cluster.AsClusterGroup().ForAttribute("TestAttribute", "Value");
    ClusterGroup group2 = cluster.AsClusterGroup().ForAttribute("NotExistAttribute", "Value");

    BOOST_REQUIRE(group1.GetNodes().size() == 1);
    BOOST_REQUIRE(group2.GetNodes().size() == 0);
}

BOOST_AUTO_TEST_CASE(IgniteForDataNodes)
{
    IgniteCluster cluster = node.GetCluster();

    BOOST_REQUIRE(cluster.IsActive());

    ClusterGroup group1 = cluster.AsClusterGroup().ForDataNodes("cache1");
    ClusterGroup group2 = cluster.AsClusterGroup().ForDataNodes("notExist");

    BOOST_REQUIRE(group1.GetNodes().size() == 1);
    BOOST_REQUIRE(group2.GetNodes().size() == 0);
}

BOOST_AUTO_TEST_CASE(IgniteForServers)
{
    IgniteCluster cluster = node.GetCluster();

    BOOST_REQUIRE(cluster.IsActive());

    ClusterGroup group = cluster.AsClusterGroup().ForServers();

    BOOST_REQUIRE(group.GetNodes().size() == 1);
}

BOOST_AUTO_TEST_CASE(IgniteForCpp)
{
    IgniteCluster cluster = node.GetCluster();

    BOOST_REQUIRE(cluster.IsActive());

    ClusterGroup group = cluster.AsClusterGroup().ForCpp();

    BOOST_REQUIRE(group.GetNodes().size() == 1);
}

BOOST_AUTO_TEST_SUITE_END()

BOOST_FIXTURE_TEST_SUITE(ClusterTestSuiteIsolated, ClusterTestSuiteFixtureIsolated)

BOOST_AUTO_TEST_CASE(IgniteSetActive)
{
    BOOST_REQUIRE(node.IsActive());

    node.SetActive(false);

    BOOST_REQUIRE(!node.IsActive());

    node.SetActive(true);

    BOOST_REQUIRE(node.IsActive());
}

BOOST_AUTO_TEST_SUITE_END()
