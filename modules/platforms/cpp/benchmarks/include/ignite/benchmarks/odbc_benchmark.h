/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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

#ifndef IGNITE_BENCHMARKS_ODBC_BENCHMARK_H
#define IGNITE_BENCHMARKS_ODBC_BENCHMARK_H

#include <boost/program_options.hpp>

#include <ignite/benchmarks/odbc_utils.h>
#include <ignite/benchmarks/basic_benchmark.h>

namespace benchmark
{

/**
 * Configuration for the ODBC Benchmark.
 */
struct OdbcBenchmarkConfig
{
    /** Connection string. */
    std::string connectionString;

    /** Name of the cache to use. */
    std::string cacheName;

    /** Begin of the cache key range to use. */
    int32_t cacheRangeBegin;

    /** End of the cache key range to use. */
    int32_t cacheRangeEnd;

    /**
     * Initialize a benchmark config using environment variables.
     *
     * @return Instance of config.
     */
    static OdbcBenchmarkConfig GetFromEnv()
    {
        OdbcBenchmarkConfig self;

        self.connectionString = utils::GetEnvVar("CONNECTION_STRING");

        self.cacheName = utils::GetEnvVar("CACHE_NAME", std::string("PUBLIC"));

        std::string range = utils::GetEnvVar("CACHE_RANGE");

        if (std::count(range.begin(), range.end(), '-') != 1)
            throw std::runtime_error("Invalid CACHE_RANGE: expected format is <number>-<number>");

        std::string::iterator dlIt = std::find(range.begin(), range.end(), '-');
        size_t dlPos = dlIt - range.begin();

        std::string range_begin = range.substr(0, dlPos);
        std::string range_end = range.substr(dlPos + 1);

        self.cacheRangeBegin = utils::LexicalCast<int32_t>(range_begin);
        self.cacheRangeEnd = utils::LexicalCast<int32_t>(range_end);

        return self;
    }

    /**
     * Initialize a benchmark config using environment variables.
     *
     * @param vm Variable map.
     * @return Instance of config.
     */
    static OdbcBenchmarkConfig GetFromVm(boost::program_options::variables_map& vm)
    {
        OdbcBenchmarkConfig self;

        self.connectionString = vm["connection_string"].as<std::string>();

        self.cacheName = vm["cache"].as<std::string>();

        std::string range = vm["range"].as<std::string>();

        if (std::count(range.begin(), range.end(), '-') != 1)
            throw std::runtime_error("Invalid cache range: expected format is <number>-<number>");

        std::string::iterator dlIt = std::find(range.begin(), range.end(), '-');
        size_t dlPos = dlIt - range.begin();

        std::string range_begin = range.substr(0, dlPos);
        std::string range_end = range.substr(dlPos + 1);

        self.cacheRangeBegin = utils::LexicalCast<int32_t>(range_begin);
        self.cacheRangeEnd = utils::LexicalCast<int32_t>(range_end);

        return self;
    }

private:
    OdbcBenchmarkConfig()
    {
        // No-op.
    }
};

/**
 * Basic ODBC benchmark.
 */
class OdbcBenchmark : public BasicBenchmark
{
public:
    /** Config type for the benchmark. Should be set - used by runner. */
    typedef OdbcBenchmarkConfig ConfigType;

    /**
     * Default constructor.
     */
    OdbcBenchmark(boost::shared_ptr<const ConfigType> cfg) :
        BasicBenchmark(),
        config(cfg)
    {
        // No-op.
    }

    /**
     * Destructor.
     */
    virtual ~OdbcBenchmark()
    {
        // No-op.
    }

    virtual void SetUp()
    {
        BasicBenchmark::SetUp();

        // Allocate an environment handle
        SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);

        // We want ODBC 3 support
        SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, reinterpret_cast<void*>(SQL_OV_ODBC3), 0);

        // Allocate a connection handle
        SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);

        // Combining connect string
        std::vector<SQLCHAR> connectStr(config->connectionString.begin(), config->connectionString.end());

        SQLCHAR outStr[odbc_utils::ODBC_BUFFER_SIZE];
        SQLSMALLINT outStrLen;

        // Connecting to ODBC server.
        SQLRETURN ret = SQLDriverConnect(dbc, NULL, &connectStr[0], static_cast<SQLSMALLINT>(connectStr.size()),
                outStr, sizeof(outStr), &outStrLen, SQL_DRIVER_COMPLETE);

        if (!SQL_SUCCEEDED(ret))
            odbc_utils::ThrowOdbcError(SQL_HANDLE_DBC, dbc, "Failed to connect");

        // Allocate a statement handle
        SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
    }

    virtual void CleanUp()
    {
        // Releasing statement handle.
        SQLFreeHandle(SQL_HANDLE_STMT, stmt);

        // Disconnecting from the server.
        SQLDisconnect(dbc);

        // Releasing allocated handles.
        SQLFreeHandle(SQL_HANDLE_DBC, dbc);
        SQLFreeHandle(SQL_HANDLE_ENV, env);

        BasicBenchmark::CleanUp();
    }

protected:
    /** Environment handle. */
    SQLHENV env;

    /** Connection handle. */
    SQLHDBC dbc;

    /** Statement handle. */
    SQLHSTMT stmt;

    /** Benchmark config. */
    boost::shared_ptr<const ConfigType> config;
};

} // namespace benchmark

#endif // IGNITE_BENCHMARKS_ODBC_BENCHMARK_H
