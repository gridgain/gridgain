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

namespace benchmark
{

/**
 * Benchmark abstract class.
 */
class BasicBenchmark
{
public:
    enum { CALCULATIONS_TO_ACCUMULATE = 50 };

    /**
     * Default constructor.
     */
    BasicBenchmark()
    {
        // No-op.
    }

    virtual ~BasicBenchmark() = default;

    /**
     * Do action to measure.
     * Should be defined by user.
     */
    virtual void DoAction() = 0;

    /**
     * Set up environment for benchmark.
     * May be defined by user.
     */
    virtual void SetUp()
    {
        // No-op.
    }

    /**
     * Clean up after benchmark.
     * May be defined by user.
     */
    virtual void CleanUp()
    {
        // No-op.
    }

    /**
     * Load data for benchmark. Called once.
     * May be defined by user.
     */
    virtual void LoadData()
    {
        // No-op.
    }

    /**
     * Get next random int32_t value in specified bounds.
     *
     * @param start Min value.
     * @param end Max value.
     * @return Next random value within bounds.
     */
    int32_t NextRandomInt32(int32_t start, int32_t end)
    {
        std::uniform_int_distribution<int32_t> distr(start, end);

        return distr(randomDevice);
    }

    /**
     * Get next random double value in specified bounds.
     *
     * @param start Min value.
     * @param end Max value.
     * @return Next random value within bounds.
     */
    double NextRandomDouble(double start, double end)
    {
        std::uniform_real_distribution<double> distr(start, end);

        return distr(randomDevice);
    }

private:
    /// Random device.
    std::random_device randomDevice;
};

} // namespace benchmark

#endif // IGNITE_BENCHMARKS_BASIC_BENCHMARK_H

