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

#ifndef IGNITE_BENCHMARKS_MEASURE_THREAD_H
#define IGNITE_BENCHMARKS_MEASURE_THREAD_H

#include <vector>
#include <algorithm>
#include <iostream>

#include <boost/thread.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/chrono.hpp>
#include <boost/atomic.hpp>

#include <ignite/benchmarks/odbc_utils.h>
#include <ignite/benchmarks/basic_benchmark.h>

namespace benchmark
{

/**
 * The MeasureThread class.
 */
class MeasureThread
{
    enum { CALCULATIONS_TO_ACCUMULATE = 20 };
public:
    /**
     * Constructor.
     */
    MeasureThread() :
        thread(),
        stop(false),
        lastLatency(0)
    {
        // No-op.
    }

    /**
     * Destructor.
     */
    ~MeasureThread()
    {
        Stop();
    }

    /**
     * Start measuring.
     *
     * @param benchmark Benchmark.
     * @param accumulator Runs count accumulator.
     */
    void Start(BasicBenchmark& benchmark, boost::atomic_uint_fast64_t& accumulator)
    {
        thread.reset(
            new boost::thread(
                boost::bind(
                    &MeasureThread::Run,
                    this,
                    boost::ref(benchmark),
                    boost::ref(accumulator)
                )
            )
        );
    }

    /**
     * Stop measuring.
     */
    void Stop()
    {
        stop = true;

        if (thread->joinable())
            thread->join();
    }

    /**
     * Get Latency.
     * @return Last latency.
     */
    int64_t GetLatency() const
    {
        return lastLatency;
    }

private:
    /**
     * Main routine.
     *
     * @param benchmark Benchmark.
     * @param accumulator Runs count accumulator.
     */
    void Run(BasicBenchmark& benchmark, boost::atomic_uint_fast64_t& accumulator)
    {
        try
        {
            benchmark.SetUp();

            std::vector<int64_t> arr;
            arr.reserve(CALCULATIONS_TO_ACCUMULATE + 1);

            while (!stop)
            {
                int64_t action_ns_pass = MeasureAction(benchmark);

                arr.push_back(action_ns_pass);
                accumulator.fetch_add(1);

                if (arr.size() >= CALCULATIONS_TO_ACCUMULATE)
                {
                    lastLatency = std::accumulate(arr.begin(), arr.end(), 0) / arr.size();
                    arr.clear();
                }
            }

            benchmark.CleanUp();
        }
        catch (const std::exception& err)
        {
            std::cout << "Runtime error: " << err.what() << std::endl;
        }
    }

    /**
     * Measure action.
     *
     * @param benchmark Benchmark.
     * @return Action duration in nanoseconds.
     */
    int64_t MeasureAction(BasicBenchmark& benchmark)
    {
        using namespace boost::chrono;

        steady_clock::time_point before = steady_clock::now();

        benchmark.DoAction();

        steady_clock::time_point after = steady_clock::now();

        return duration_cast<nanoseconds>(after - before).count();
    }

    /** Thread. */
    boost::shared_ptr<boost::thread> thread;

    /** Stop flag. */
    volatile bool stop;

    /** Last measured latency */
    int64_t lastLatency;
};

} // namespace benchmark

#endif // IGNITE_BENCHMARKS_MEASURE_THREAD_H
