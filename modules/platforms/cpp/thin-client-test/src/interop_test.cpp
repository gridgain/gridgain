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

#include <boost/test/unit_test.hpp>

#include <ignite/ignition.h>

#include <ignite/thin/ignite_client_configuration.h>
#include <ignite/thin/ignite_client.h>

#include <ignite/complex_type.h>
#include <test_utils.h>

using namespace ignite::thin;
using namespace boost::unit_test;

namespace
{
    /** Test put affinity key Java task. */
    const std::string TEST_PUT_AFFINITY_KEY_TASK("org.apache.ignite.platform.PlatformComputePutAffinityKeyTask");
}

class InteropTestSuiteFixture
{
public:
    static ignite::Ignite StartNode(const char* name)
    {
        return ignite_test::StartCrossPlatformServerNode("interop.xml", name);
    }

    InteropTestSuiteFixture()
    {
        serverNode = StartNode("ServerNode");

        IgniteClientConfiguration cfg;
        cfg.SetEndPoints("127.0.0.1:11110");

        client = IgniteClient::Start(cfg);
    }

    ~InteropTestSuiteFixture()
    {
        ignite::Ignition::StopAll(false);
    }

protected:
    /** Server node. */
    ignite::Ignite serverNode;

    /** Client. */
    IgniteClient client;
};

/**
 * Affinity key class.
 */
struct AffinityKey
{
    /** Key */
    int32_t key;

    /** Affinity key */
    int32_t aff;

    /**
     * Default constructor.
     */
    AffinityKey() :
            key(0),
            aff(0)
    {
        // No-op.
    }

    /**
     * Constructor.
     * @param key Key.
     * @param aff Affinity key.
     */
    AffinityKey(int32_t key, int32_t aff) :
            key(key),
            aff(aff)
    {
        // No-op.
    }
};

namespace ignite
{
    namespace binary
    {
        template<>
        struct BinaryType<AffinityKey> : BinaryTypeDefaultAll<AffinityKey>
        {
            static void GetTypeName(std::string& dst)
            {
                dst = "AffinityKey";
            }

            static void Write(BinaryWriter& writer, const AffinityKey& obj)
            {
                writer.WriteInt32("key", obj.key);
                writer.WriteInt32("affKey", obj.aff);
            }

            static void Read(BinaryReader& reader, AffinityKey& dst)
            {
                dst.key = reader.ReadInt32("key");
                dst.aff = reader.ReadInt32("affKey");
            }

            static void GetAffinityFieldName(std::string& dst)
            {
                dst = "affKey";
            }
        };
    }
}

BOOST_FIXTURE_TEST_SUITE(InteropTestSuite, InteropTestSuiteFixture)

BOOST_AUTO_TEST_CASE(PutObjectByCppThenByJava)
{
    cache::CacheClient<AffinityKey, AffinityKey> cache = client.GetOrCreateCache<AffinityKey, AffinityKey>("default");

    AffinityKey key1(2, 3);
    cache.Put(key1, key1);

    compute::ComputeClient compute = client.GetCompute();

    compute.ExecuteJavaTask<int*>(TEST_PUT_AFFINITY_KEY_TASK);

    AffinityKey key2(1, 2);
    AffinityKey val = cache.Get(key2);

    BOOST_CHECK_EQUAL(val.key, 1);
    BOOST_CHECK_EQUAL(val.aff, 2);
}

BOOST_AUTO_TEST_CASE(PutObjectPointerByCppThenByJava)
{
    cache::CacheClient<AffinityKey*, AffinityKey*> cache =
        client.GetOrCreateCache<AffinityKey*, AffinityKey*>("default");

    AffinityKey* key1 = new AffinityKey(2, 3);
    cache.Put(key1, key1);

    delete key1;

    compute::ComputeClient compute = client.GetCompute();

    compute.ExecuteJavaTask<int*>(TEST_PUT_AFFINITY_KEY_TASK);

    AffinityKey* key2 = new AffinityKey(1, 2);
    AffinityKey* val = cache.Get(key2);

    BOOST_CHECK_EQUAL(val->key, 1);
    BOOST_CHECK_EQUAL(val->aff, 2);

    delete key2;
    delete val;
}

BOOST_AUTO_TEST_SUITE_END()
