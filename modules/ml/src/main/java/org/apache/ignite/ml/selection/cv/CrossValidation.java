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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.BiFunction;
import java.util.function.DoubleConsumer;
import java.util.function.Function;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.ml.IgniteModel;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.impl.cache.CacheBasedDatasetBuilder;
import org.apache.ignite.ml.dataset.impl.local.LocalDatasetBuilder;
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
import org.jetbrains.annotations.NotNull;

/**
 * Cross validation score calculator. Cross validation is an approach that allows to avoid overfitting that is made the
 * following way: the training set is split into k smaller sets. The following procedure is followed for each of the k
 * “folds”:
 * <ul>
 *    <li>A model is trained using k-1 of the folds as training data;</li>
 *    <li>the resulting model is validated on the remaining part of the data (i.e., it is used as a test set to compute
 * a performance measure such as accuracy).</li>
 * </ul>
 *
 * @param <M> Type of model.
 * @param <L> Type of a label (truth or prediction).
 * @param <K> Type of a key in {@code upstream} data.
 * @param <V> Type of a value in {@code upstream} data.
 */
public class CrossValidation<M extends IgniteModel<Vector, L>, L, K, V> {
    private DatasetTrainer<M, L> trainer;

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

    public CrossValidationResult score() {

        switch (paramGrid.getParameterSearchStrategy()) {
            case BRUT_FORCE:
                return scoreBrutForceHyperparameterOptimiztion();
            case RANDOM_SEARCH:
                return scoreRandomSearchHyperparameterOptimiztion();
            default:
                throw new UnsupportedOperationException("This strategy "
                    + paramGrid.getParameterSearchStrategy().name() + " is unsupported");
        }
    }


    // TODO: https://en.wikipedia.org/wiki/Random_search
    private CrossValidationResult scoreRandomSearchHyperparameterOptimiztion() {
        List<Double[]> paramSets = new ParameterSetGenerator(paramGrid.getParamValuesByParamIdx()).generate();

        CrossValidationResult cvRes = new CrossValidationResult();

        Random rnd = new Random(paramGrid.getSeed());

        List<Double[]> paramSetsCp = new ArrayList<>(paramSets);

        int maxTries = 0;
        while (cvRes.getBestAvgScore() <= paramGrid.getSatisfactoryFitness() && maxTries < paramGrid.getMaxTries() && !paramSetsCp.isEmpty()) {
            int idx = rnd.nextInt(paramSetsCp.size());
            Double[] paramSet = paramSetsCp.get(idx);

            updateCrossValidationResultForTheGivenParamSet(cvRes, paramSet);

            paramSetsCp.remove(idx);
            maxTries++;
        }
        return cvRes;

    }


    private CrossValidationResult scoreBrutForceHyperparameterOptimiztion() {
        List<Double[]> paramSets = new ParameterSetGenerator(paramGrid.getParamValuesByParamIdx()).generate();

        CrossValidationResult cvRes = new CrossValidationResult();

        paramSets.forEach(paramSet -> updateCrossValidationResultForTheGivenParamSet(cvRes, paramSet));

        return cvRes;
    }

    private void updateCrossValidationResultForTheGivenParamSet(CrossValidationResult cvRes, Double[] paramSet) {
        Map<String, Double> paramMap = injectAndGetParametersFromPipeline(paramGrid, paramSet);

        double[] locScores;
        if(isRunningOnIgnite) {
            locScores = score(trainer, metric, ignite, upstreamCache, filter, preprocessor,
                new SHA256UniformMapper<>(), amountOfFolds);
        } else
            locScores = score(trainer, metric, upstreamMap, filter, parts, preprocessor, amountOfFolds);


        cvRes.addScores(locScores, paramMap);

        final double locAvgScore = Arrays.stream(locScores).average().orElse(Double.MIN_VALUE);

        if (locAvgScore > cvRes.getBestAvgScore()) {
            cvRes.setBestScore(locScores);
            cvRes.setBestHyperParams(paramMap);
        }
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
     * @param trainer          Trainer of the model.
     * @param scoreCalculator  Score calculator.
     * @param ignite           Ignite instance.
     * @param upstreamCache    Ignite cache with {@code upstream} data.
     * @param preprocessor      Preprocessor.
     * @param cv               Number of folds.
     * @return Array of scores of the estimator for each run of the cross validation.
     */
    public double[] score(DatasetTrainer<M, L> trainer, Metric<L> scoreCalculator, Ignite ignite,
                          IgniteCache<K, V> upstreamCache, Preprocessor<K, V> preprocessor, int cv) {
        return score(trainer, scoreCalculator, ignite, upstreamCache, (k, v) -> true, preprocessor,
            new SHA256UniformMapper<>(), cv);
    }

    /**
     * Computes cross-validated metrics.
     *
     * @param trainer          Trainer of the model.
     * @param scoreCalculator  Base score calculator.
     * @param ignite           Ignite instance.
     * @param upstreamCache    Ignite cache with {@code upstream} data.
     * @param filter           Base {@code upstream} data filter.
     * @param preprocessor      Preprocessor.
     * @param cv               Number of folds.
     * @return Array of scores of the estimator for each run of the cross validation.
     */
    public double[] score(DatasetTrainer<M, L> trainer, Metric<L> scoreCalculator, Ignite ignite,
                          IgniteCache<K, V> upstreamCache, IgniteBiPredicate<K, V> filter,
                          Preprocessor<K, V> preprocessor, int cv) {
        return score(trainer, scoreCalculator, ignite, upstreamCache, filter, preprocessor,
            new SHA256UniformMapper<>(), cv);
    }





    /**
     * Computes cross-validated metrics.
     *
     * @param trainer          Trainer of the model.
     * @param scoreCalculator  Base score calculator.
     * @param ignite           Ignite instance.
     * @param upstreamCache    Ignite cache with {@code upstream} data.
     * @param filter           Base {@code upstream} data filter.
     * @param preprocessor      Preprocessor.
     * @param mapper           Mapper used to map a key-value pair to a point on the segment (0, 1).
     * @param cv               Number of folds.
     * @return Array of scores of the estimator for each run of the cross validation.
     */
    public double[] score(DatasetTrainer<M, L> trainer, Metric<L> scoreCalculator,
                          Ignite ignite, IgniteCache<K, V> upstreamCache, IgniteBiPredicate<K, V> filter,
                          Preprocessor<K, V> preprocessor,
                          UniformMapper<K, V> mapper, int cv) {

        return score(
            trainer,
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
            ),
            preprocessor,
            scoreCalculator,
            mapper,
            cv
        );
    }

    /**
     * Computes cross-validated metrics.
     *
     * @param trainer          Trainer of the model.
     * @param scoreCalculator  Base score calculator.
     * @param upstreamMap      Map with {@code upstream} data.
     * @param parts            Number of partitions.
     * @param preprocessor      Preprocessor.
     * @param cv               Number of folds.
     * @return Array of scores of the estimator for each run of the cross validation.
     */
    public double[] score(DatasetTrainer<M, L> trainer, Metric<L> scoreCalculator, Map<K, V> upstreamMap,
                          int parts, Preprocessor<K, V> preprocessor, int cv) {
        return score(trainer, scoreCalculator, upstreamMap, (k, v) -> true, parts, preprocessor,
            new SHA256UniformMapper<>(), cv);
    }

    /**
     * Computes cross-validated metrics.
     *
     * @param trainer          Trainer of the model.
     * @param scoreCalculator  Base score calculator.
     * @param upstreamMap      Map with {@code upstream} data.
     * @param filter           Base {@code upstream} data filter.
     * @param parts            Number of partitions.
     * @param preprocessor      Preprocessor.
     * @param cv               Number of folds.
     * @return Array of scores of the estimator for each run of the cross validation.
     */
    public double[] score(DatasetTrainer<M, L> trainer, Metric<L> scoreCalculator, Map<K, V> upstreamMap,
                          IgniteBiPredicate<K, V> filter, int parts, Preprocessor<K, V> preprocessor, int cv) {
        return score(trainer, scoreCalculator, upstreamMap, filter, parts, preprocessor,
            new SHA256UniformMapper<>(), cv);
    }

    /**
     * Computes cross-validated metrics.
     *
     * @param trainer          Trainer of the model.
     * @param scoreCalculator  Base score calculator.
     * @param upstreamMap      Map with {@code upstream} data.
     * @param filter           Base {@code upstream} data filter.
     * @param parts            Number of partitions.
     * @param preprocessor      Preprocessor.
     * @param mapper           Mapper used to map a key-value pair to a point on the segment (0, 1).
     * @param cv               Number of folds.
     * @return Array of scores of the estimator for each run of the cross validation.
     */
    public double[] score(DatasetTrainer<M, L> trainer, Metric<L> scoreCalculator, Map<K, V> upstreamMap,
                          IgniteBiPredicate<K, V> filter, int parts, Preprocessor<K, V> preprocessor, UniformMapper<K, V> mapper, int cv) {
        return score(
            trainer,
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
            ),
            preprocessor,
            scoreCalculator,
            mapper,
            cv
        );
    }

    /**
     * Computes cross-validated metrics.
     *
     * @param trainer                Trainer of the model.
     * @param datasetBuilderSupplier Dataset builder supplier.
     * @param testDataIterSupplier   Test data iterator supplier.
     * @param scoreCalculator        Base score calculator.
     * @param mapper                 Mapper used to map a key-value pair to a point on the segment (0, 1).
     * @param cv                     Number of folds.
     * @return Array of scores of the estimator for each run of the cross validation.
     */
    private double[] score(DatasetTrainer<M, L> trainer, Function<IgniteBiPredicate<K, V>,
        DatasetBuilder<K, V>> datasetBuilderSupplier,
                           BiFunction<IgniteBiPredicate<K, V>, M, LabelPairCursor<L>> testDataIterSupplier,
                           Preprocessor<K, V> preprocessor,
                           Metric<L> scoreCalculator, UniformMapper<K, V> mapper, int cv) {

        double[] scores = new double[cv];

        double foldSize = 1.0 / cv;
        for (int i = 0; i < cv; i++) {
            double from = foldSize * i;
            double to = foldSize * (i + 1);

            IgniteBiPredicate<K, V> trainSetFilter = (k, v) -> {
                double pnt = mapper.map(k, v);
                return pnt < from || pnt > to;
            };

            DatasetBuilder<K, V> datasetBuilder = datasetBuilderSupplier.apply(trainSetFilter);
            M mdl = trainer.fit(datasetBuilder, preprocessor); //TODO: IGNITE-11580

            try (LabelPairCursor<L> cursor = testDataIterSupplier.apply(trainSetFilter, mdl)) {
                scores[i] = scoreCalculator.score(cursor.iterator());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        return scores;
    }


    /**
     * Computes cross-validated metrics with a passed parameter grid.
     *
     * The real cross-validation training will be called each time for each parameter set.
     *
     * @param pipeline        Pipeline of stages.
     * @param scoreCalculator Base score calculator.
     * @param ignite          Ignite instance.
     * @param upstreamCache   Ignite cache with {@code upstream} data.
     * @param filter          Base {@code upstream} data filter.
     * @param amountOfFolds   Amount of folds.
     * @param paramGrid       Parameter grid.
     * @return Array of scores of the estimator for each run of the cross validation.
     */
    public CrossValidationResult score(Pipeline<K, V, Integer, Double> pipeline,
                                       Metric<L> scoreCalculator,
                                       Ignite ignite,
                                       IgniteCache<K, V> upstreamCache,
                                       IgniteBiPredicate<K, V> filter,
                                       int amountOfFolds,
                                       ParamGrid paramGrid) {

        List<Double[]> paramSets = new ParameterSetGenerator(paramGrid.getParamValuesByParamIdx()).generate();

        CrossValidationResult cvRes = new CrossValidationResult();

        paramSets.forEach(paramSet -> {
            Map<String, Double> paramMap = injectAndGetParametersFromPipeline(paramGrid, paramSet);

            double[] locScores = scorePipeline(
                pipeline,
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
                ),
                scoreCalculator,
                new SHA256UniformMapper<>(),
                amountOfFolds
            );


            cvRes.addScores(locScores, paramMap);

            final double locAvgScore = Arrays.stream(locScores).average().orElse(Double.MIN_VALUE);

            if (locAvgScore > cvRes.getBestAvgScore()) {
                cvRes.setBestScore(locScores);
                cvRes.setBestHyperParams(paramMap);
                System.out.println(paramMap.toString());
            }
        });

        return cvRes;

    }

    /**
     * Computes cross-validated metrics.
     *
     * @param pipeline               Pipeline of stages.
     * @param datasetBuilderSupplier Dataset builder supplier.
     * @param testDataIterSupplier   Test data iterator supplier.
     * @param scoreCalculator        Base score calculator.
     * @param mapper                 Mapper used to map a key-value pair to a point on the segment (0, 1).
     * @param cv                     Number of folds.
     * @return Array of scores of the estimator for each run of the cross validation.
     */
    private double[] scorePipeline(Pipeline<K, V, Integer, Double> pipeline,
                                   Function<IgniteBiPredicate<K, V>, DatasetBuilder<K, V>> datasetBuilderSupplier,
                                   BiFunction<IgniteBiPredicate<K, V>, M, LabelPairCursor<L>> testDataIterSupplier,
                                   Metric<L> scoreCalculator,
                                   UniformMapper<K, V> mapper,
                                   int cv
    ) {

        double[] scores = new double[cv];

        double foldSize = 1.0 / cv;
        for (int i = 0; i < cv; i++) {
            double from = foldSize * i;
            double to = foldSize * (i + 1);

            IgniteBiPredicate<K, V> trainSetFilter = (k, v) -> {
                double pnt = mapper.map(k, v);
                return pnt < from || pnt > to;
            };

            DatasetBuilder<K, V> datasetBuilder = datasetBuilderSupplier.apply(trainSetFilter);
            PipelineMdl<K, V> mdl = pipeline.fit(datasetBuilder);

            try (LabelPairCursor<L> cursor = testDataIterSupplier.apply(trainSetFilter, (M) mdl)) {
                scores[i] = scoreCalculator.score(cursor.iterator());
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

    public CrossValidation<M, L, K, V>  withMetric(Metric<L> metric) {
        this.metric = metric;
        return this;
    }

    public CrossValidation<M, L, K, V>  withIgnite(Ignite ignite) {
        this.ignite = ignite;
        return this;
    }

    public CrossValidation<M, L, K, V>  withUpstreamCache(IgniteCache<K, V> upstreamCache) {
        this.upstreamCache = upstreamCache;
        return this;
    }

    public CrossValidation<M, L, K, V>  withPreprocessor(Preprocessor<K, V> preprocessor) {
        this.preprocessor = preprocessor;
        return this;
    }

    public CrossValidation<M, L, K, V>  withFilter(IgniteBiPredicate<K, V> filter) {
        this.filter = filter;
        return this;
    }

    public CrossValidation<M, L, K, V>  withAmountOfFolds(int amountOfFolds) {
        this.amountOfFolds = amountOfFolds;
        return this;
    }

    public CrossValidation<M, L, K, V>  withParamGrid(ParamGrid paramGrid) {
        this.paramGrid = paramGrid;
        return this;
    }

    public CrossValidation<M, L, K, V>  isRunningOnIgnite(boolean runningOnIgnite) {
        isRunningOnIgnite = runningOnIgnite;
        return this;
    }

    public CrossValidation<M, L, K, V>  isRunningOnPipeline(boolean runningOnPipeline) {
        isRunningOnPipeline = runningOnPipeline;
        return this;
    }


    public CrossValidation<M, L, K, V>  withUpstreamMap(Map<K, V> upstreamMap) {
        this.upstreamMap = upstreamMap;
        return this;
    }

    public CrossValidation<M, L, K, V>  withAmountOfParts(int parts) {
        this.parts = parts;
        return this;
    }
}
