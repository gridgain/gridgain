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
#include <boost/program_options.hpp>

#include <ignite/benchmarks/measure_thread.h>

namespace benchmark
{

/**
 * Configuration for the Runner.
 */
struct RunnerConfig
{
    /** How long should take a warmup phase. */
    int32_t warmupSecs;

    /** How long should the whole measurement take. */
    int32_t durationSecs;

    /** Number of threads to use in benchmark. */
    int32_t threadCnt;

    /**
     * Initialize a runner config using environment variables.
     *
     * @return Instance of config.
     */
    static RunnerConfig GetFromEnv()
    {
        RunnerConfig self;

        self.warmupSecs = utils::GetEnvVar<int32_t>("WARMUP", 0);
        self.durationSecs = utils::GetEnvVar<int32_t>("DURATION");

        self.threadCnt = utils::GetEnvVar<int32_t>("THREAD_CNT");

        return self;
    }

    /**
     * Initialize a runner config using environment variables.
     *
     * @param vm Variable map.
     * @return Instance of config.
     */
    static RunnerConfig GetFromVm(boost::program_options::variables_map& vm)
    {
        RunnerConfig self;

        self.warmupSecs = vm["warmup"].as<int32_t>();
        self.durationSecs = vm["duration"].as<int32_t>();
        self.threadCnt = vm["threads"].as<int32_t>();

        return self;
    }
};

/**
 * Benchmark factory.
 */
template<typename Benchmark>
class BenchmarkFactory
{
public:
    /** Benchmark type. */
    typedef Benchmark BenchmarkType;

    /**
     * Destructor.
     */
    virtual ~BenchmarkFactory()
    {
        // No-op.
    }

    /**
     * Construct a new instance of the benchmark.
     *
     * @return New instance of the benchmark.
     */
    virtual boost::shared_ptr<BenchmarkType> Construct() = 0;
};

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
    ReplicateThreadRunner(const RunnerConfig &cfg, BenchmarkFactory<Benchmark>& factory) :
        config(cfg),
        factory(factory)
    {
        // No-op.
    }

    /**
     * Destructor.
     */
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

        int retcode = 0;

        boost::atomic_uint_fast64_t accumulator;

        std::vector<MeasureThread> threads;
        threads.resize(config.threadCnt);

        typedef std::vector<MeasureThread>::iterator ThreadsIter;

        std::vector< boost::shared_ptr<Benchmark> > benchmarks;

        try
        {
            {
                boost::shared_ptr<Benchmark> loadBench = factory.Construct();

                loadBench->SetUp();
                loadBench->LoadData();
                loadBench->CleanUp();
            }

            steady_clock::time_point begin = steady_clock::now();

            for (size_t i = 0; i < threads.size(); ++i)
            {
                benchmarks.push_back(factory.Construct());

                threads.at(i).Start(*benchmarks.back(), accumulator);
            }

            if (config.warmupSecs > 0)
            {
                std::cout << "Warming up" << std::endl;

                boost::this_thread::sleep_for(seconds(config.warmupSecs));
            }

            uint_fast64_t lastSeenCounter = accumulator.load();
            steady_clock::time_point lastSeenTime = steady_clock::now();

            std::cout << "Time (sec),Operations/sec,Latency (nsec)" << std::endl;

            int32_t totalDuration = config.durationSecs + config.warmupSecs;

            while (steady_clock::now() - begin < seconds(totalDuration))
            {
                boost::this_thread::sleep_for(seconds(1));

                int64_t latency = 0;
                for (ThreadsIter it = threads.begin(); it != threads.end(); ++it)
                    latency += it->GetLatency();

                latency /= threads.size();

                uint_fast64_t counterNow = accumulator.load();
                uint_fast64_t operations = counterNow - lastSeenCounter;
                lastSeenCounter = counterNow;

                steady_clock::time_point timeNow = steady_clock::now();
                double secsPassed = duration_cast<nanoseconds>(timeNow - lastSeenTime).count() / 1000000000.0;
                lastSeenTime = timeNow;

                uint_fast64_t throughput = static_cast<uint_fast64_t>(operations / secsPassed);

                std::cout << time(NULL) << "," << throughput << "," << latency << std::endl;
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
    /** Config */
    RunnerConfig config;

    /** Benchmark factory */
    BenchmarkFactory<Benchmark>& factory;

    /** Random device. */
    std::random_device randomDevice;
};

} // namespace benchmark

#endif // IGNITE_BENCHMARKS_REPLICATE_THREAD_RUNNER_H

