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

#include <ignite/benchmarks/odbc_utils.h>
#include <ignite/benchmarks/basic_benchmark.h>

struct odbc_benchmark_config
{
    std::string connection_string;
    int warmup_secs;
    int duration_secs;
    std::string cache_name;
    int32_t cache_range_begin;
    int32_t cache_range_end;

    static odbc_benchmark_config get_from_env()
    {
        odbc_benchmark_config self;

        self.connection_string = utils::get_env_var("CONNECTION_STRING");

        self.warmup_secs = utils::get_env_var<int>("WARMUP", 0);
        self.duration_secs = utils::get_env_var<int>("DURATION");
        self.cache_name = utils::get_env_var("CACHE_NAME", std::string("PUBLIC"));

        std::string range = utils::get_env_var("CACHE_RANGE");

        if (std::count(range.begin(), range.end(), '-') != 1)
            throw std::runtime_error("Invalid CACHE_RANGE: expected format is <number>-<number>");

        auto dl_it = std::find(range.begin(), range.end(), '-');
        auto dl_pos = dl_it - range.begin();

        std::string range_begin = range.substr(0, dl_pos);
        std::string range_end = range.substr(dl_pos + 1);

        self.cache_range_begin = utils::lexical_cast<int32_t>(range_begin);
        self.cache_range_end = utils::lexical_cast<int32_t>(range_end);

        return self;
    }

private:
    odbc_benchmark_config() = default;
};

class odbc_benchmark : public basic_benchmark
{
public:
    odbc_benchmark() :
        basic_benchmark(),
        config(odbc_benchmark_config::get_from_env())
    {
        // No-op.
    }

    virtual ~odbc_benchmark()
    {
        // No-op.
    }

    virtual void set_up()
    {
        basic_benchmark::set_up();

        // Allocate an environment handle
        SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);

        // We want ODBC 3 support
        SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, reinterpret_cast<void*>(SQL_OV_ODBC3), 0);

        // Allocate a connection handle
        SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);

        // Combining connect string
        std::vector<SQLCHAR> connectStr(config.connection_string.begin(), config.connection_string.end());

        SQLCHAR outstr[odbc_utils::ODBC_BUFFER_SIZE];
        SQLSMALLINT outstrlen;

        // Connecting to ODBC server.
        SQLRETURN ret = SQLDriverConnect(dbc, NULL, &connectStr[0], static_cast<SQLSMALLINT>(connectStr.size()),
                outstr, sizeof(outstr), &outstrlen, SQL_DRIVER_COMPLETE);

        if (!SQL_SUCCEEDED(ret))
            odbc_utils::ThrowOdbcError(SQL_HANDLE_DBC, dbc, "Failed to connect");

        // Allocate a statement handle
        SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
    }

    virtual void tear_down()
    {
        // Releasing statement handle.
        SQLFreeHandle(SQL_HANDLE_STMT, stmt);

        // Disconnecting from the server.
        SQLDisconnect(dbc);

        // Releasing allocated handles.
        SQLFreeHandle(SQL_HANDLE_DBC, dbc);
        SQLFreeHandle(SQL_HANDLE_ENV, env);

        basic_benchmark::tear_down();
    }

    void run()
    {
        basic_benchmark::run(config.warmup_secs, config.duration_secs);
    }

protected:
    odbc_benchmark_config config;

    SQLHENV env;

    SQLHDBC dbc;

    SQLHSTMT stmt;
};


#endif // IGNITE_BENCHMARKS_ODBC_BENCHMARK_H
