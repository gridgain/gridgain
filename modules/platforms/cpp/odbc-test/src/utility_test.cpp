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

#include <vector>

#include <boost/test/unit_test.hpp>

#include <ignite/impl/binary/binary_writer_impl.h>

#include <ignite/odbc/utility.h>
#include <ignite/common/utils.h>

#include "network/internal_utils.h"

using namespace ignite::utility;

BOOST_AUTO_TEST_SUITE(UtilityTestSuite)

BOOST_AUTO_TEST_CASE(TestUtilityRemoveSurroundingSpaces)
{
    std::string inStr("   \r \n    \t  some meaningfull data   \n\n   \t  \r  ");
    std::string expectedOutStr("some meaningfull data");

    std::string realOutStr(ignite::common::StripSurroundingWhitespaces(inStr.begin(), inStr.end()));

    BOOST_REQUIRE(expectedOutStr == realOutStr);
}

BOOST_AUTO_TEST_CASE(TestUtilityCopyStringToBuffer)
{
    char buffer[1024];

    std::string str("Some data. And some more data here.");

    CopyStringToBuffer(str, buffer, sizeof(buffer));

    BOOST_REQUIRE(!strcmp(buffer, str.c_str()));

    CopyStringToBuffer(str, buffer, 11);

    BOOST_REQUIRE(!strcmp(buffer, str.substr(0, 10).c_str()));
}

BOOST_AUTO_TEST_CASE(TestUtilityWriteReadString)
{
    using namespace ignite::impl::binary;
    using namespace ignite::impl::interop;

    std::string inStr1("Hello World!");
    std::string inStr2;
    std::string inStr3("Lorem ipsum");

    std::string outStr1;
    std::string outStr2;
    std::string outStr3;
    std::string outStr4;

    ignite::impl::interop::InteropUnpooledMemory mem(1024);
    InteropOutputStream outStream(&mem);
    BinaryWriterImpl writer(&outStream, 0);

    WriteString(writer, inStr1);
    WriteString(writer, inStr2);
    WriteString(writer, inStr3);
    writer.WriteNull();

    outStream.Synchronize();

    InteropInputStream inStream(&mem);
    BinaryReaderImpl reader(&inStream);

    ReadString(reader, outStr1);
    ReadString(reader, outStr2);
    ReadString(reader, outStr3);
    ReadString(reader, outStr4);

    BOOST_REQUIRE(inStr1 == outStr1);
    BOOST_REQUIRE(inStr2 == outStr2);
    BOOST_REQUIRE(inStr3 == outStr3);
    BOOST_REQUIRE(outStr4.empty());
}

void CheckDecimalWriteRead(const std::string& val)
{
    using namespace ignite::impl::binary;
    using namespace ignite::impl::interop;
    using namespace ignite::common;
    using namespace ignite::utility;

    InteropUnpooledMemory mem(1024);
    InteropOutputStream outStream(&mem);
    BinaryWriterImpl writer(&outStream, 0);

    Decimal decimal(val);

    WriteDecimal(writer, decimal);

    outStream.Synchronize();

    InteropInputStream inStream(&mem);
    BinaryReaderImpl reader(&inStream);

    Decimal out;
    ReadDecimal(reader, out);

    std::stringstream converter;
    converter << out;

    std::string res = converter.str();

    BOOST_CHECK_EQUAL(res, val);
}

/**
 * Check that Decimal writing and reading works as expected.
 *
 * 1. Create Decimal value.
 * 2. Write using standard serialization algorithm.
 * 3. Read using standard de-serialization algorithm.
 * 4. Check that initial and read value are equal.
 *
 * Repeat with the following values: 0, 1, -1, 0.1, -0.1, 42, -42, 160, -160, 34729864879625196, -34729864879625196,
 * 3472986487.9625196, -3472986487.9625196, 3472.9864879625196, -3472.9864879625196, 0.34729864879625196,
 * -0.34729864879625196
 */
BOOST_AUTO_TEST_CASE(TestUtilityWriteReadDecimal)
{
    CheckDecimalWriteRead("0");
    CheckDecimalWriteRead("1");
    CheckDecimalWriteRead("-1");
    CheckDecimalWriteRead("0.1");
    CheckDecimalWriteRead("-0.1");
    CheckDecimalWriteRead("42");
    CheckDecimalWriteRead("-42");
    CheckDecimalWriteRead("160");
    CheckDecimalWriteRead("-160");
    CheckDecimalWriteRead("34729864879625196");
    CheckDecimalWriteRead("-34729864879625196");
    CheckDecimalWriteRead("3472986487.9625196");
    CheckDecimalWriteRead("-3472986487.9625196");
    CheckDecimalWriteRead("3472.9864879625196");
    CheckDecimalWriteRead("-3472.9864879625196");
    CheckDecimalWriteRead("0.34729864879625196");
    CheckDecimalWriteRead("-0.34729864879625196");
}

struct TestAddrinfo;

/**
 * The TestAddrinfo struct for testing purposes.
 */
struct TestAddrinfo
{
    int id;
    TestAddrinfo* ai_next;
};

/**
 * Check that Addresses are shuffled randomly.
 *
 * 1. Make collection of 5 addresses.
 * 2. Shuffle it 2 times and get 2 outputs.
 * 3. Make sure that first collection does not equal out1 and out2.
 */
BOOST_AUTO_TEST_CASE(TestUtilityShuffleAddresses)
{
    std::vector<TestAddrinfo> addrs(5);

    addrs[0].id = 0;
    addrs[0].ai_next = &addrs[1];

    addrs[1].id = 1;
    addrs[1].ai_next = &addrs[2];

    addrs[2].id = 2;
    addrs[2].ai_next = &addrs[3];

    addrs[3].id = 3;
    addrs[3].ai_next = &addrs[4];

    addrs[4].id = 4;
    addrs[4].ai_next = NULL;

    std::vector<TestAddrinfo*> out1 = ignite::network::internal_utils::ShuffleAddresses(&addrs[0]);
    std::vector<TestAddrinfo*> out2 = ignite::network::internal_utils::ShuffleAddresses(&addrs[0]);

    bool allEq = true;
    for (size_t i = 0; i < addrs.size(); ++i)
    {
        allEq = allEq && (addrs[i].id == out1[i]->id);
        allEq = allEq && (addrs[i].id == out2[i]->id);
    }

    BOOST_REQUIRE(!allEq);
}
BOOST_AUTO_TEST_SUITE_END()
