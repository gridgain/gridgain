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

#ifndef IGNITE_BENCHMARKS_REPLICATE_THREAD_RUNNER_H
#define IGNITE_BENCHMARKS_REPLICATE_THREAD_RUNNER_H

#include <iostream>
#include <vector>
#include <sstream>
#include <numeric>
#include <algorithm>
#include <stdexcept>

#include <boost/chrono.hpp>
#include <boost/atomic.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/make_shared.hpp>

#include <ignite/benchmarks/measure_thread.h>

/**
 * Benchmark runner that copies a benchmark instance for every thread.
 */
template<typename Benchmark>
class ReplicateThreadRunner
{
public:
    /**
     * Default constructor.
     */
    ReplicateThreadRunner()
    {
        // No-op.
    }

    virtual ~ReplicateThreadRunner()
    {
        // No-op.
    }

    /**
     * Run benchmarks.
     *
     * @return Exit code.
     */
    virtual int Run()
    {
        using namespace boost::chrono;

        typedef typename Benchmark::ConfigType BenchmarkConfig;

        BenchmarkConfig config = Benchmark::GetConfig();

        int retcode = 0;

        boost::atomic_uint_fast64_t accumulator;

        std::vector<MeasureThread> threads;
        threads.resize(config.threadCnt);

        typedef std::vector<MeasureThread>::iterator ThreadsIter;

        std::vector< boost::shared_ptr<Benchmark> > benchmarks;

        try
        {
            {
                std::cout << "Loading data to the server" << std::endl;

                Benchmark loadBench;

                loadBench.SetUp();
                loadBench.LoadData();
                loadBench.CleanUp();
            }

            steady_clock::time_point begin = steady_clock::now();

            for (size_t i = 0; i < threads.size(); ++i)
            {
                benchmarks.push_back(boost::make_shared<Benchmark>());

                threads.at(i).Start(*benchmarks.back(), accumulator);
            }

            if (config.warmupSecs > 0)
            {
                std::cout << "Warming up" << std::endl;

                boost::this_thread::sleep_for(seconds(config.warmupSecs));
            }

            uint_fast64_t lastSeenCounter = accumulator.load();
            steady_clock::time_point lastSeenTime = steady_clock::now();

            std::cout << "Time, sec | Operations/sec | Latency, nsec" << std::endl;

            while (steady_clock::now() - begin < seconds(config.durationSecs))
            {
                boost::this_thread::sleep_for(seconds(1));

                int64_t latency = 0;
                for (ThreadsIter it = threads.begin(); it != threads.end(); ++it)
                    latency += it->GetLatency();

                uint_fast64_t counterNow = accumulator.load();
                uint_fast64_t operations = counterNow - lastSeenCounter;
                lastSeenCounter = counterNow;

                steady_clock::time_point timeNow = steady_clock::now();
                double secsPassed = duration_cast<nanoseconds>(timeNow - lastSeenTime).count() / 1000000000.0;
                lastSeenTime = timeNow;

                uint_fast64_t throughput = static_cast<uint_fast64_t>(operations / secsPassed);

                std::cout << time(NULL) << " | " << throughput << " | " << latency << std::endl;
            }

            std::cout << "END" << std::endl;
        }
        catch (const std::exception& err)
        {
            std::cout << "Runtime error: " << err.what() << std::endl;

            retcode = -1;
        }

        for (ThreadsIter it = threads.begin(); it != threads.end(); ++it)
            it->Stop();

        std::cout << std::endl;
        std::cout << std::endl;

        return retcode;
    }

private:
    /// Random device.
    std::random_device randomDevice;
};

#endif // IGNITE_BENCHMARKS_REPLICATE_THREAD_RUNNER_H

