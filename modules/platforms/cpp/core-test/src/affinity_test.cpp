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
#include <boost/chrono.hpp>
#include <boost/thread.hpp>

#include <ignite/ignition.h>
#include <ignite/test_utils.h>

using namespace ignite;
using namespace ignite::cache;
using namespace ignite::cluster;
using namespace ignite::common::concurrent;
using namespace ignite_test;

using namespace boost::unit_test;

/*
 * Test setup fixture.
 */
struct AffinityTestSuiteFixture
{
    Ignite node;

    Cache<int, int> cache;
    CacheAffinity<int> affinity;

    Ignite MakeNode(const char* name)
    {
#ifdef IGNITE_TESTS_32
        const char* config = "cache-test-32.xml";
#else
        const char* config = "cache-test.xml";
#endif
        return StartNode(config, name);
    }

    /*
     * Constructor.
     */
    AffinityTestSuiteFixture() :
        node(MakeNode("AffinityNode1")),
        cache(node.GetCache<int, int>("cache1")),
        affinity(node.GetAffinity<int>(cache.GetName()))
    {
        // No-op.
    }

    /*
     * Destructor.
     */
    ~AffinityTestSuiteFixture()
    {
        Ignition::StopAll(true);
    }
};

BOOST_FIXTURE_TEST_SUITE(AffinityTestSuite, AffinityTestSuiteFixture)

BOOST_AUTO_TEST_CASE(IgniteAffinityGetPartition)
{
    BOOST_CHECK_EQUAL(1024, affinity.GetPartitions());

    BOOST_CHECK_EQUAL(0, affinity.GetPartition(0));
    BOOST_CHECK_EQUAL(1, affinity.GetPartition(1));
}

BOOST_AUTO_TEST_CASE(IgniteAffinityIsPrimaryOrBackup)
{
    std::vector<ClusterNode> nodes = node.GetCluster().AsClusterGroup().GetNodes();

    BOOST_REQUIRE(true == affinity.IsPrimary(nodes.front(), 0));
    BOOST_REQUIRE(false == affinity.IsBackup(nodes.front(), 0));
    BOOST_REQUIRE(true == affinity.IsPrimaryOrBackup(nodes.front(), 0));
}

BOOST_AUTO_TEST_CASE(IgniteAffinityGetDifferentPartitions)
{
    std::vector<ClusterNode> nodes = node.GetCluster().AsClusterGroup().GetNodes();

    BOOST_CHECK_EQUAL(affinity.GetPrimaryPartitions(nodes.front()).size(), 1024);
    BOOST_CHECK_EQUAL(affinity.GetBackupPartitions(nodes.front()).size(), 0);
    BOOST_CHECK_EQUAL(affinity.GetAllPartitions(nodes.front()).size(), 1024);
}

BOOST_AUTO_TEST_CASE(IgniteAffinityGetAffinityKey)
{
    BOOST_CHECK_EQUAL((affinity.GetAffinityKey<int>(10)), 10);
    BOOST_CHECK_EQUAL((affinity.GetAffinityKey<int>(20)), 20);
}

BOOST_AUTO_TEST_CASE(IgniteAffinityMapKeysToNodes)
{
    int32_t vals[] = { 1, 2, 3 };
    std::list<int32_t> keys(vals, vals + sizeof(vals)/sizeof(int));
    std::map<ClusterNode, std::list<int32_t> > map = affinity.MapKeysToNodes(keys);

    BOOST_REQUIRE(map.size() == 1);

    for (std::list<int>::iterator it = keys.begin(); it != keys.end(); ++it)
    {
        ClusterNode clusterNode = affinity.MapKeyToNode(*it);
        BOOST_REQUIRE(map.find(clusterNode) != map.end());

        std::list<int32_t> nodeKeys = map[clusterNode];
        BOOST_REQUIRE(nodeKeys.size() > 0);
        BOOST_REQUIRE(std::find(nodeKeys.begin(), nodeKeys.end(), *it) != nodeKeys.end());
    }
}

BOOST_AUTO_TEST_CASE(IgniteAffinityMapKeyToPrimaryAndBackups)
{
    const int32_t key = 1;

    std::vector<ClusterNode> nodes = affinity.MapKeyToPrimaryAndBackups(key);

    BOOST_REQUIRE(nodes.size() == 1);
    BOOST_REQUIRE(true == affinity.IsPrimary(nodes.front(), key));

    int part = affinity.GetPartition(key);
    std::vector<ClusterNode> partNodes = affinity.MapPartitionToPrimaryAndBackups(part);

    BOOST_REQUIRE(nodes.front().GetId() == partNodes.front().GetId());
}

BOOST_AUTO_TEST_CASE(IgniteAffinityMapPartitionsToNodes)
{
    std::vector<int> parts(1, 0);
    std::map<int, ClusterNode> map = affinity.MapPartitionsToNodes(parts);

    BOOST_CHECK_EQUAL(parts.size(), map.size());
}

BOOST_AUTO_TEST_CASE(IgniteAffinityMapPartitionToNode)
{
    const int32_t key = 1;

    ClusterNode clusterNode = affinity.MapKeyToNode(key);

    BOOST_REQUIRE(true == affinity.IsPrimary(clusterNode, key));
    BOOST_REQUIRE(true == affinity.IsPrimaryOrBackup(clusterNode, key));
    BOOST_REQUIRE(false == affinity.IsBackup(clusterNode, key));

    int part = affinity.GetPartition(key);
    ClusterNode partNode = affinity.MapPartitionToNode(part);

    BOOST_REQUIRE(clusterNode.GetId() == partNode.GetId());
}

BOOST_AUTO_TEST_SUITE_END()