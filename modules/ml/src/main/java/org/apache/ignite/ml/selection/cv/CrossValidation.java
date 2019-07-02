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

package org.apache.ignite.ml.selection.cv;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.BiFunction;
import java.util.function.DoubleConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.ml.IgniteModel;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.impl.cache.CacheBasedDatasetBuilder;
import org.apache.ignite.ml.dataset.impl.local.LocalDatasetBuilder;
import org.apache.ignite.ml.environment.LearningEnvironment;
import org.apache.ignite.ml.environment.LearningEnvironmentBuilder;
import org.apache.ignite.ml.environment.parallelism.Promise;
import org.apache.ignite.ml.math.functions.IgniteSupplier;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.pipeline.Pipeline;
import org.apache.ignite.ml.pipeline.PipelineMdl;
import org.apache.ignite.ml.preprocessing.Preprocessor;
import org.apache.ignite.ml.selection.paramgrid.ParamGrid;
import org.apache.ignite.ml.selection.paramgrid.ParameterSetGenerator;
import org.apache.ignite.ml.selection.scoring.cursor.CacheBasedLabelPairCursor;
import org.apache.ignite.ml.selection.scoring.cursor.LabelPairCursor;
import org.apache.ignite.ml.selection.scoring.cursor.LocalLabelPairCursor;
import org.apache.ignite.ml.selection.scoring.metric.Metric;
import org.apache.ignite.ml.selection.split.mapper.SHA256UniformMapper;
import org.apache.ignite.ml.selection.split.mapper.UniformMapper;
import org.apache.ignite.ml.trainers.DatasetTrainer;
import org.apache.ignite.ml.util.genetic.Chromosome;
import org.apache.ignite.ml.util.genetic.GeneticAlgorithm;
import org.jetbrains.annotations.NotNull;


/**
 * Cross validation score calculator. Cross validation is an approach that allows to avoid overfitting that is made the
 * following way: the training set is split into k smaller sets. The following procedure is followed for each of the k
 * “folds”:
 * <ul>
 * <li>A model is trained using k-1 of the folds as training data;</li>
 * <li>the resulting model is validated on the remaining part of the data (i.e., it is used as a test set to compute
 * a performance measure such as accuracy).</li>
 * </ul>
 *
 * @param <M> Type of model.
 * @param <L> Type of a label (truth or prediction).
 * @param <K> Type of a key in {@code upstream} data.
 * @param <V> Type of a value in {@code upstream} data.
 */
public class CrossValidation<M extends IgniteModel<Vector, L>, L, K, V> {
    public static final int SIZE_OF_POPULATION = 50; // TODO: improve situation with strange paramters doubled here and in GA
    /** Learning environment builder. */
    private LearningEnvironmentBuilder envBuilder = LearningEnvironmentBuilder.defaultBuilder();

    /** Learning Environment. */
    private LearningEnvironment environment = envBuilder.buildForTrainer();

    private DatasetTrainer<M, L> trainer;

    private Pipeline<K, V, Integer, Double> pipeline;

    private Metric<L> metric;

    private Ignite ignite;

    private IgniteCache<K, V> upstreamCache;

    private Map<K, V> upstreamMap;

    private Preprocessor<K, V> preprocessor;

    private IgniteBiPredicate<K, V> filter = (k, v) -> true;

    private int amountOfFolds;

    private int parts;

    private ParamGrid paramGrid;

    private boolean isRunningOnIgnite = true;

    private boolean isRunningOnPipeline = true;

    private UniformMapper<K, V> mapper = new SHA256UniformMapper<>();

    public CrossValidationResult tuneHyperParamterers() {
        switch (paramGrid.getParameterSearchStrategy()) {
            case BRUT_FORCE:
                return scoreBrutForceHyperparameterOptimiztion();
            case RANDOM_SEARCH:
                return scoreRandomSearchHyperparameterOptimiztion();
            case EVOLUTION_ALGORITHM:
                return scoreEvolutionAlgorithmSearchHyperparameterOptimization();
            default:
                throw new UnsupportedOperationException("This strategy "
                    + paramGrid.getParameterSearchStrategy().name() + " is not supported yet.");
        }
    }

    private CrossValidationResult scoreEvolutionAlgorithmSearchHyperparameterOptimization() {
        List<Double[]> paramSets = new ParameterSetGenerator(paramGrid.getParamValuesByParamIdx()).generate();

        // initialization
        List<Double[]> paramSetsCp = new ArrayList<>(paramSets);
        Collections.shuffle(paramSetsCp, new Random(paramGrid.getSeed())); // TODO: - unclear seed - extract CV seed

        List<Double[]> rndParamSets = paramSetsCp.subList(0, SIZE_OF_POPULATION);

        // fitness function for parallel calculation
        /*Function<Chromosome, Double> fitnessFunction = (Chromosome chromosome) -> {
            IgniteSupplier<TaskResult> task = ()->calculateScoresForFixedParamSet(chromosome.toDoubleArray());
            TaskResult res = environment.parallelismStrategy().submit(task).unsafeGet();
            return Arrays.stream(res.locScores).average().getAsDouble();
        };*/

        // TODO: analyze sequential code
        Function<Chromosome, Double> fitnessFunction = (Chromosome chromosome) -> {
            TaskResult res = calculateScoresForFixedParamSet(chromosome.toDoubleArray());
            return Arrays.stream(res.locScores).average().getAsDouble();
        };

        Random rnd = new Random(paramGrid.getSeed()); // common seed for shared lambdas can produce the same value on each function call? or sequent?

        BiFunction<Integer, Double, Double> mutator = (Integer geneIdx, Double geneValue) -> {
            Double newGeneVal;

            Double[] possibleGeneValues = paramGrid.getParamRawData().get(geneIdx);
            newGeneVal = possibleGeneValues[rnd.nextInt(possibleGeneValues.length)];  // TODO: - unclear seed - extract CV seed

            return newGeneVal;
        };

        GeneticAlgorithm ga = new GeneticAlgorithm();
        ga.setFitnessFunction(fitnessFunction);
        ga.setMutationOperator(mutator);
        ga.initializePopulation(rndParamSets);
        ga.run(environment);

        CrossValidationResult cvRes = new CrossValidationResult();
        cvRes.setBestScore(ga.getTheBestSolution());
        return cvRes;
    }



    // TODO: https://en.wikipedia.org/wiki/Random_search
    private CrossValidationResult scoreRandomSearchHyperparameterOptimiztion() {
        List<Double[]> paramSets = new ParameterSetGenerator(paramGrid.getParamValuesByParamIdx()).generate();

        List<Double[]> paramSetsCp = new ArrayList<>(paramSets);
        Collections.shuffle(paramSetsCp, new Random(paramGrid.getSeed()));

        CrossValidationResult cvRes = new CrossValidationResult();

        List<Double[]> rndParamSets = paramSetsCp.subList(0, paramGrid.getMaxTries());

        List<IgniteSupplier<TaskResult>> tasks = rndParamSets.stream()
            .map(paramSet -> (IgniteSupplier<TaskResult>)(()->calculateScoresForFixedParamSet(paramSet)))
            .collect(Collectors.toList());

        List<TaskResult> taskResults = environment.parallelismStrategy().submit(tasks).stream()
            .map(Promise::unsafeGet)
            .collect(Collectors.toList());

        taskResults.forEach(tr -> {
            cvRes.addScores(tr.locScores, tr.paramMap);

            final double locAvgScore = Arrays.stream(tr.locScores).average().orElse(Double.MIN_VALUE);

            if (locAvgScore >= cvRes.getBestAvgScore()) {
                cvRes.setBestScore(tr.locScores);
                cvRes.setBestHyperParams(tr.paramMap);
            }
        });

        return cvRes;
    }


    private CrossValidationResult scoreBrutForceHyperparameterOptimiztion() {
        List<Double[]> paramSets = new ParameterSetGenerator(paramGrid.getParamValuesByParamIdx()).generate();

        CrossValidationResult cvRes = new CrossValidationResult();

        List<IgniteSupplier<TaskResult>> tasks = paramSets.stream()
            .map(paramSet -> (IgniteSupplier<TaskResult>)(()->calculateScoresForFixedParamSet(paramSet)))
            .collect(Collectors.toList());

        List<TaskResult> taskResults = environment.parallelismStrategy().submit(tasks).stream()
            .map(Promise::unsafeGet)
            .collect(Collectors.toList());

        taskResults.forEach(tr -> {
            cvRes.addScores(tr.locScores, tr.paramMap);

            final double locAvgScore = Arrays.stream(tr.locScores).average().orElse(Double.MIN_VALUE);

            if (locAvgScore > cvRes.getBestAvgScore()) {
                cvRes.setBestScore(tr.locScores);
                cvRes.setBestHyperParams(tr.paramMap);
            }

        });

        return cvRes;
    }

    public class TaskResult {
        private Map<String, Double> paramMap;
        private double[] locScores;

        public TaskResult(Map<String, Double> paramMap, double[] locScores) {
            this.paramMap = paramMap;
            this.locScores = locScores;
        }

        public void setParamMap(Map<String, Double> paramMap) {
            this.paramMap = paramMap;
        }

        public void setLocScores(double[] locScores) {
            this.locScores = locScores;
        }
    }

    private TaskResult calculateScoresForFixedParamSet(Double[] paramSet) {
        Map<String, Double> paramMap = injectAndGetParametersFromPipeline(paramGrid, paramSet);

        double[] locScores = scoreByFolds();

        return new TaskResult(paramMap, locScores);
    }

    public double[] scoreByFolds() {
        double[] locScores;

        if (isRunningOnPipeline) locScores = isRunningOnIgnite ? scorePipelineOnIgnite() : scorePipelineLocally();
        else locScores = isRunningOnIgnite ? scoreOnIgnite() : scoreLocally();

        return locScores;
    }

    private double[] scorePipelineLocally() {
        return scorePipeline(
            predicate -> new LocalDatasetBuilder<>(
                upstreamMap,
                (k, v) -> filter.apply(k, v) && predicate.apply(k, v),
                parts
            ),
            (predicate, mdl) -> new LocalLabelPairCursor<>(
                upstreamMap,
                (k, v) -> filter.apply(k, v) && !predicate.apply(k, v),
                preprocessor,
                mdl
            )
        );
    }

    private double[] scorePipelineOnIgnite() {
        return scorePipeline(
            predicate -> new CacheBasedDatasetBuilder<>(
                ignite,
                upstreamCache,
                (k, v) -> filter.apply(k, v) && predicate.apply(k, v)
            ),
            (predicate, mdl) -> new CacheBasedLabelPairCursor<>(
                upstreamCache,
                (k, v) -> filter.apply(k, v) && !predicate.apply(k, v),
                ((PipelineMdl<K, V>) mdl).getPreprocessor(),
                mdl
            )
        );
    }

    @NotNull private Map<String, Double> injectAndGetParametersFromPipeline(ParamGrid paramGrid, Double[] paramSet) {
        Map<String, Double> paramMap = new HashMap<>();

        for (int paramIdx = 0; paramIdx < paramSet.length; paramIdx++) {
            DoubleConsumer setter = paramGrid.getSetterByIndex(paramIdx);

            Double paramVal = paramSet[paramIdx];
            setter.accept(paramVal);

            paramMap.put(paramGrid.getParamNameByIndex(paramIdx), paramVal);

        }
        return paramMap;
    }

    /**
     * Computes cross-validated metrics.
     *
     * @return Array of scores of the estimator for each run of the cross validation.
     */
    private double[] scoreOnIgnite() {
        return score(
            predicate -> new CacheBasedDatasetBuilder<>(
                ignite,
                upstreamCache,
                (k, v) -> filter.apply(k, v) && predicate.apply(k, v)
            ),
            (predicate, mdl) -> new CacheBasedLabelPairCursor<>(
                upstreamCache,
                (k, v) -> filter.apply(k, v) && !predicate.apply(k, v),
                preprocessor,
                mdl
            )
        );
    }

    /**
     * Computes cross-validated metrics.
     *
     * @return Array of scores of the estimator for each run of the cross validation.
     */
    private double[] scoreLocally() {
        return score(
            predicate -> new LocalDatasetBuilder<>(
                upstreamMap,
                (k, v) -> filter.apply(k, v) && predicate.apply(k, v),
                parts
            ),
            (predicate, mdl) -> new LocalLabelPairCursor<>(
                upstreamMap,
                (k, v) -> filter.apply(k, v) && !predicate.apply(k, v),
                preprocessor,
                mdl
            )
        );
    }

    /**
     * Computes cross-validated metrics.
     *
     * @param datasetBuilderSupplier Dataset builder supplier.
     * @param testDataIterSupplier   Test data iterator supplier.
     * @return Array of scores of the estimator for each run of the cross validation.
     */
    private double[] score(Function<IgniteBiPredicate<K, V>, DatasetBuilder<K, V>> datasetBuilderSupplier,
                           BiFunction<IgniteBiPredicate<K, V>, M, LabelPairCursor<L>> testDataIterSupplier) {

        double[] scores = new double[amountOfFolds];

        double foldSize = 1.0 / amountOfFolds;
        for (int i = 0; i < amountOfFolds; i++) {
            double from = foldSize * i;
            double to = foldSize * (i + 1);

            IgniteBiPredicate<K, V> trainSetFilter = (k, v) -> {
                double pnt = mapper.map(k, v);
                return pnt < from || pnt > to;
            };

            DatasetBuilder<K, V> datasetBuilder = datasetBuilderSupplier.apply(trainSetFilter);
            M mdl = trainer.fit(datasetBuilder, preprocessor); //TODO: IGNITE-11580

            try (LabelPairCursor<L> cursor = testDataIterSupplier.apply(trainSetFilter, mdl)) {
                scores[i] = metric.score(cursor.iterator());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        return scores;
    }

    /**
     * Computes cross-validated metrics.
     *
     * @param datasetBuilderSupplier Dataset builder supplier.
     * @param testDataIterSupplier   Test data iterator supplier.
     * @return Array of scores of the estimator for each run of the cross validation.
     */
    private double[] scorePipeline(Function<IgniteBiPredicate<K, V>, DatasetBuilder<K, V>> datasetBuilderSupplier,
                                   BiFunction<IgniteBiPredicate<K, V>, M, LabelPairCursor<L>> testDataIterSupplier) {

        double[] scores = new double[amountOfFolds];

        double foldSize = 1.0 / amountOfFolds;
        for (int i = 0; i < amountOfFolds; i++) {
            double from = foldSize * i;
            double to = foldSize * (i + 1);

            IgniteBiPredicate<K, V> trainSetFilter = (k, v) -> {
                double pnt = mapper.map(k, v);
                return pnt < from || pnt > to;
            };

            DatasetBuilder<K, V> datasetBuilder = datasetBuilderSupplier.apply(trainSetFilter);
            PipelineMdl<K, V> mdl = pipeline.fit(datasetBuilder);

            try (LabelPairCursor<L> cursor = testDataIterSupplier.apply(trainSetFilter, (M) mdl)) {
                scores[i] = metric.score(cursor.iterator());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        return scores;
    }

    public CrossValidation<M, L, K, V> withTrainer(DatasetTrainer<M, L> trainer) {
        this.trainer = trainer;
        return this;
    }

    public CrossValidation<M, L, K, V> withMetric(Metric<L> metric) {
        this.metric = metric;
        return this;
    }

    public CrossValidation<M, L, K, V> withIgnite(Ignite ignite) {
        this.ignite = ignite;
        return this;
    }

    public CrossValidation<M, L, K, V> withUpstreamCache(IgniteCache<K, V> upstreamCache) {
        this.upstreamCache = upstreamCache;
        return this;
    }

    public CrossValidation<M, L, K, V> withPreprocessor(Preprocessor<K, V> preprocessor) {
        this.preprocessor = preprocessor;
        return this;
    }

    public CrossValidation<M, L, K, V> withFilter(IgniteBiPredicate<K, V> filter) {
        this.filter = filter;
        return this;
    }

    public CrossValidation<M, L, K, V> withAmountOfFolds(int amountOfFolds) {
        this.amountOfFolds = amountOfFolds;
        return this;
    }

    public CrossValidation<M, L, K, V> withParamGrid(ParamGrid paramGrid) {
        this.paramGrid = paramGrid;
        return this;
    }

    public CrossValidation<M, L, K, V> isRunningOnIgnite(boolean runningOnIgnite) {
        isRunningOnIgnite = runningOnIgnite;
        return this;
    }

    public CrossValidation<M, L, K, V> isRunningOnPipeline(boolean runningOnPipeline) {
        isRunningOnPipeline = runningOnPipeline;
        return this;
    }


    public CrossValidation<M, L, K, V> withUpstreamMap(Map<K, V> upstreamMap) {
        this.upstreamMap = upstreamMap;
        return this;
    }

    public CrossValidation<M, L, K, V> withAmountOfParts(int parts) {
        this.parts = parts;
        return this;
    }

    /**
     * Changes learning Environment.
     *
     * @param envBuilder Learning environment builder.
     */
    // TODO: IGNITE-10441 Think about more elegant ways to perform fluent API.
    public CrossValidation<M,L,K,V> withEnvironmentBuilder(LearningEnvironmentBuilder envBuilder) {
        this.envBuilder = envBuilder;
        environment = envBuilder.buildForTrainer();

        return this;
    }


    public CrossValidation<M, L, K, V> withPipeline(Pipeline<K, V, Integer, Double> pipeline) {
        this.pipeline = pipeline;
        return this;
    }

    public CrossValidation<M, L, K, V> withMapper(UniformMapper<K, V> mapper) {
        this.mapper = mapper;
        return this;
    }

}
