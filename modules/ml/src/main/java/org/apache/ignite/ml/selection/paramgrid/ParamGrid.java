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

package org.apache.ignite.ml.selection.paramgrid;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Keeps the grid of parameters.
 */
public class ParamGrid {
    /** Parameter values by parameter index. */
    private Map<Integer, Double[]> paramValuesByParamIdx = new HashMap<>();

    /** Parameter names by parameter index. */
    private Map<Integer, String> paramNamesByParamIdx = new HashMap<>();

    /** Parameter counter. */
    private int paramCntr;

    /** Parameter search strategy. */
    private HyperParameterSearchingStrategy parameterSearchStrategy = HyperParameterSearchingStrategy.BRUT_FORCE;

    /** Satisfactory fitness to stop the hyperparameter search. */
    private double satisfactoryFitness = 0.5;

    /** Max tries to stop the hyperparameter search. */
    private int maxTries = 100;

    /** Seed. */
    private long seed = 1234L;

    /** */
    public Map<Integer, Double[]> getParamValuesByParamIdx() {
        return Collections.unmodifiableMap(paramValuesByParamIdx);
    }

    /**
     * Adds a grid for the specific hyper parameter.
     * @param paramName The parameter name.
     * @param params The array of the given hyper parameter values.
     * @return The updated ParamGrid.
     */
    public ParamGrid addHyperParam(String paramName, Double[] params) {
        paramValuesByParamIdx.put(paramCntr, params);
        paramNamesByParamIdx.put(paramCntr, paramName);
        paramCntr++;
        return this;
    }

    /**
     * Set up the hyperparameter searching strategy.
     *
     * @param parameterSearchStrategy Parameter search strategy.
     */
    public ParamGrid withParameterSearchStrategy(HyperParameterSearchingStrategy parameterSearchStrategy) {
        this.parameterSearchStrategy = parameterSearchStrategy;
        return this;
    }

    /**
     *
     */
    public HyperParameterSearchingStrategy getParameterSearchStrategy() {
        return parameterSearchStrategy;
    }

    /** */
    public String getParamNameByIndex(int idx) {
        return paramNamesByParamIdx.get(idx);
    }

    /** */
    public long getSeed() {
        return seed;
    }

    /**
     * Set up the seed number.
     *
     * @param seed Seed.
     */
    public ParamGrid withSeed(long seed) {
        this.seed = seed;
        return this;
    }

    /**
     *
     */
    public double getSatisfactoryFitness() {
        return satisfactoryFitness;
    }

    /**
     * Set up the satisfactory fitness to stop the hyperparameter search.
     *
     * @param fitness Fitness.
     */
    public ParamGrid withSatisfactoryFitness(double fitness) {
        this.satisfactoryFitness = fitness;
        return this;
    }

    /**
     *
     */
    public int getMaxTries() {
        return maxTries;
    }

    /**
     * Set up the max number of tries to stop the hyperparameter search.
     *
     * @param maxTries Max tries.
     */
    public ParamGrid withMaxTries(int maxTries) {
        this.maxTries = maxTries;
        return this;
    }
}
