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

#ifndef IGNITE_BENCHMARKS_BASIC_BENCHMARK_H
#define IGNITE_BENCHMARKS_BASIC_BENCHMARK_H

#include <iostream>
#include <chrono>
#include <vector>
#include <sstream>
#include <random>
#include <numeric>
#include <algorithm>
#include <stdexcept>

namespace utils
{

template<typename TO, typename TI>
TO lexical_cast(const TI& in)
{
    TO out;

    std::stringstream converter;
    converter << in;
    converter >> out;

    return out;
}

template<>
std::string lexical_cast(const std::string& in)
{
    return in;
}

template<typename OutType = std::string>
OutType get_env_var(const std::string& name)
{
    char* var = std::getenv(name.c_str());

    if (!var)
        throw std::runtime_error("Environment variable '" + name + "' is not set");

    std::string varStr(var);
    std::cout << name << "=" << varStr << std::endl;

    return lexical_cast<OutType>(varStr);
}

template<typename OutType = std::string>
OutType get_env_var(const std::string& name, OutType dflt)
{
    char* var = std::getenv(name.c_str());

    if (!var)
    {
        std::cout << name << "=" << dflt << std::endl;

        return dflt;
    }

    std::string varStr(var);
    std::cout << name << "=" << varStr << std::endl;

    return lexical_cast<OutType>(varStr);
}

} // namespace utils

/**
 * @brief The basic_benchmark class
 * Benchmark abstract class.
 */
class basic_benchmark
{
public:
    enum { CALCULATIONS_TO_ACCUMULATE = 50 };

    /**
     * @brief basic_benchmark
     * Default constructor.
     */
    basic_benchmark()
    {
        // No-op.
    }

    virtual ~basic_benchmark() = default;

    /**
     * @brief do_action
     * Should be defined by user.
     */
    virtual void do_action() = 0;

    /**
     * @brief set_up
     * May be defined by user.
     */
    virtual void set_up()
    {
        // No-op.
    }

    /**
     * @brief tear_down
     * May be defined by user.
     */
    virtual void tear_down()
    {
        // No-op.
    }

    /**
     * @brief next_random_int32
     * Get next random int32_t value in specified bounds.
     * @param start Min value.
     * @param end Max value.
     * @return Next random value within bounds.
     */
    int32_t next_random_int32(int32_t start, int32_t end)
    {
        std::uniform_int_distribution<int32_t> distr(start, end);

        return distr(random_device);
    }

    /**
     * @brief next_random_double
     * Get next random double value in specified bounds.
     * @param start Min value.
     * @param end Max value.
     * @return Next random value within bounds.
     */
    double next_random_double(double start, double end)
    {
        std::uniform_real_distribution<double> distr(start, end);

        return distr(random_device);
    }

    /**
     * @brief run
     * Run benchmark.
     * @param warmup_secs Warmup time in seconds.
     * @param duration_secs Benchmark duration in seconds.
     * @return Exit code.
     */
    int run(int32_t warmup_secs, int32_t duration_secs)
    {
        using namespace std::chrono;

        int retcode = 0;

        try
        {
            set_up();

            std::cout << "Time, sec | Operations/sec | Latency, nsec" << std::endl;

            std::vector<int64_t> arr;
            arr.reserve(CALCULATIONS_TO_ACCUMULATE + 1);

            int32_t lastSeenSec = 0;

            int32_t diff = 0;
            int32_t counter = 0;
            int32_t lastSeenCounter = 0;
            int32_t latency = 0;

            auto now = steady_clock::now();
            auto begin = now;
            while (now - begin < seconds(duration_secs))
            {
                int32_t secsPassed = duration_cast<seconds>(now - begin).count();

                if (secsPassed != lastSeenSec)
                {
                    lastSeenSec = secsPassed;
                    diff = counter - lastSeenCounter;
                    lastSeenCounter = counter;

                    if (now - begin > seconds(warmup_secs))
                    {
                        std::cout << time(NULL) << " | "
                                  << diff << " | "
                                  << latency << std::endl;
                    }
                }
                counter++;

                int64_t action_nanos_pass = measure_action();

                arr.push_back(action_nanos_pass);

                if (arr.size() > CALCULATIONS_TO_ACCUMULATE)
                {
                    latency = std::accumulate(arr.begin(), arr.end(), 0) / arr.size();
                    arr.clear();
                }

                now = steady_clock::now();
            }

            std::cout << "END" << std::endl;

            tear_down();
        }
        catch (const std::exception& err)
        {
            std::cout << "Runtime error: " << err.what() << std::endl;

            retcode = -1;
        }

        std::cout << std::endl;
        std::cout << std::endl;

        return retcode;
    }

private:
    /**
     * @brief measure_action
     * Measure action.
     * @param start Start of cache keys.
     * @param end End of cache keys.
     * @return Action duration in nanoseconds.
     */
    int64_t measure_action()
    {
        using namespace std::chrono;

        auto before = steady_clock::now();

        do_action();

        auto after = steady_clock::now();

        return duration_cast<nanoseconds>(after - before).count();
    }

    /// Random device.
    std::random_device random_device;
};

#endif // IGNITE_BENCHMARKS_BASIC_BENCHMARK_H

