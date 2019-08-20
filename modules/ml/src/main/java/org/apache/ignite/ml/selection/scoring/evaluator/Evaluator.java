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

package org.apache.ignite.ml.selection.scoring.evaluator;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.ml.IgniteModel;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.impl.cache.CacheBasedDatasetBuilder;
import org.apache.ignite.ml.dataset.impl.local.LocalDatasetBuilder;
import org.apache.ignite.ml.dataset.primitive.FeatureMatrixWithLabelsOnHeapData;
import org.apache.ignite.ml.dataset.primitive.FeatureMatrixWithLabelsOnHeapDataBuilder;
import org.apache.ignite.ml.dataset.primitive.builder.context.EmptyContextBuilder;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.environment.LearningEnvironment;
import org.apache.ignite.ml.environment.LearningEnvironmentBuilder;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.preprocessing.Preprocessor;
import org.apache.ignite.ml.selection.scoring.evaluator.aggregator.MetricStatsAggregator;
import org.apache.ignite.ml.selection.scoring.evaluator.context.EvaluationContext;
import org.apache.ignite.ml.selection.scoring.evaluator.metric.Metric;
import org.apache.ignite.ml.selection.scoring.evaluator.metric.MetricName;
import org.apache.ignite.ml.structures.LabeledVector;

/**
 * Evaluator that computes metrics from predictions and ground truth values.
 */
public class Evaluator {
    /**
     * Computes the given metric on the given cache.
     *
     * @param ignite The instance of Ignite.
     * @param dataCache The given cache.
     * @param mdl The model.
     * @param preprocessor The preprocessor.
     * @param metric The binary classification metric.
     * @param <K> The type of cache entry key.
     * @param <V> The type of cache entry value.
     * @return Computed metric.
     */
    public static <K, V> double evaluate(Ignite ignite,
        IgniteCache<K, V> dataCache,
        IgniteModel<Vector, Double> mdl,
        Preprocessor<K, V> preprocessor,
        Metric metric
    ) {

        return evaluate(mdl, new CacheBasedDatasetBuilder<>(ignite, dataCache), preprocessor, metric);
    }

    /**
     * Computes the given metric on the given cache.
     *
     * @param dataCache The given local data.
     * @param mdl The model.
     * @param preprocessor The preprocessor.
     * @param metric The binary classification metric.
     * @param <K> The type of cache entry key.
     * @param <V> The type of cache entry value.
     * @return Computed metric.
     */
    public static <K, V> double evaluate(Map<K, V> dataCache,
        IgniteModel<Vector, Double> mdl,
        Preprocessor<K, V> preprocessor,
        Metric metric
    ) {

        return evaluate(mdl, new LocalDatasetBuilder<>(dataCache, 1), preprocessor, metric);
    }

    /**
     * Computes the given metric on the given cache.
     *
     * @param ignite The instance of Ignite.
     * @param dataCache The given cache.
     * @param mdl The model.
     * @param preprocessor The preprocessor.
     * @param metric The binary classification metric.
     * @param <K> The type of cache entry key.
     * @param <V> The type of cache entry value.
     * @return Computed metric.
     */
    public static <K, V> double evaluate(Ignite ignite,
        IgniteCache<K, V> dataCache,
        IgniteBiPredicate<K, V> filter,
        IgniteModel<Vector, Double> mdl,
        Preprocessor<K, V> preprocessor,
        Metric metric
    ) {

        return evaluate(mdl, new CacheBasedDatasetBuilder<>(ignite, dataCache, filter), preprocessor, metric);
    }

    /**
     * Computes the given metric on the given cache.
     *
     * @param dataCache The given cache.
     * @param filter The given filter.
     * @param mdl The model.
     * @param preprocessor The preprocessor.
     * @param metric The binary classification metric.
     * @param <K> The type of cache entry key.
     * @param <V> The type of cache entry value.
     * @return Computed metric.
     */
    public static <K, V> double evaluate(Map<K, V> dataCache, IgniteBiPredicate<K, V> filter,
        IgniteModel<Vector, Double> mdl,
        Preprocessor<K, V> preprocessor,
        Metric metric) {
        return evaluate(mdl, new LocalDatasetBuilder<>(dataCache, filter, 1), preprocessor, metric);
    }

    public static <K, V> EvaluationResult evaluateBinaryClassification(Ignite ignite,
        IgniteCache<K, V> dataCache,
        IgniteModel<Vector, Double> mdl,
        Preprocessor<K, V> preprocessor) {

        return evaluateBinaryClassification(ignite, dataCache, (k, v) -> true, mdl, preprocessor);
    }

    public static <K, V> EvaluationResult evaluateBinaryClassification(Ignite ignite,
        IgniteCache<K, V> dataCache,
        IgniteBiPredicate<K, V> filter,
        IgniteModel<Vector, Double> mdl,
        Preprocessor<K, V> preprocessor) {

        return evaluate(mdl, new CacheBasedDatasetBuilder<>(ignite, dataCache, filter), preprocessor,
            MetricName.ACCURACY, MetricName.PRECISION, MetricName.RECALL, MetricName.F_MEASURE);
    }

    public static <K, V> EvaluationResult evaluateBinaryClassification(Map<K, V> dataCache,
        IgniteModel<Vector, Double> mdl,
        Preprocessor<K, V> preprocessor) {

        return evaluateBinaryClassification(dataCache, (k,v) -> true, mdl, preprocessor);
    }

    public static <K, V> EvaluationResult evaluateBinaryClassification(Map<K, V> dataCache,
        IgniteBiPredicate<K, V> filter,
        IgniteModel<Vector, Double> mdl,
        Preprocessor<K, V> preprocessor) {

        return evaluate(mdl, new LocalDatasetBuilder<>(dataCache, filter, 1), preprocessor,
            MetricName.ACCURACY, MetricName.PRECISION, MetricName.RECALL, MetricName.F_MEASURE);
    }

    public static <K, V> double evaluate(IgniteModel<Vector, Double> mdl, DatasetBuilder<K, V> datasetBuilder,
        Preprocessor<K, V> preprocessor, MetricName name) {
        return evaluate(mdl, datasetBuilder, preprocessor, new Metric[] {name.create()}).get();
    }

    public static <K, V> EvaluationResult evaluate(IgniteModel<Vector, Double> mdl, DatasetBuilder<K, V> datasetBuilder,
        Preprocessor<K, V> preprocessor, MetricName name1, MetricName name2, MetricName... other) {

        Metric[] metrics = new Metric[other.length + 2];
        metrics[0] = name1.create();
        metrics[1] = name2.create();
        for (int i = 0; i < other.length; i++)
            metrics[i + 2] = other[i].create();

        return evaluate(mdl, datasetBuilder, preprocessor, metrics);
    }

    public static <K, V> double evaluate(IgniteModel<Vector, Double> mdl, DatasetBuilder<K, V> datasetBuilder,
        Preprocessor<K, V> preprocessor, Metric metric) {
        return evaluate(mdl, datasetBuilder, preprocessor, new Metric[] {metric}).get();
    }

    public static <K, V> EvaluationResult evaluate(IgniteModel<Vector, Double> mdl,
        DatasetBuilder<K, V> datasetBuilder, Preprocessor<K, V> preprocessor,
        Metric m1, Metric m2, Metric... other) {
        Metric[] metrics = new Metric[other.length + 2];
        metrics[0] = m1;
        metrics[1] = m2;
        for (int i = 0; i < other.length; i++)
            metrics[i + 2] = other[i];

        try (Dataset<EmptyContext, FeatureMatrixWithLabelsOnHeapData> dataset = datasetBuilder.build(
            LearningEnvironmentBuilder.defaultBuilder(),
            new EmptyContextBuilder<>(),
            new FeatureMatrixWithLabelsOnHeapDataBuilder<>(preprocessor),
            LearningEnvironment.DEFAULT_TRAINER_ENV
        )) {
            return evaluate(mdl, dataset, metrics);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static <K, V> EvaluationResult evaluate(IgniteModel<Vector, Double> mdl,
        DatasetBuilder<K, V> datasetBuilder,
        Preprocessor<K, V> preprocessor, Metric[] metrics) {

        try (Dataset<EmptyContext, FeatureMatrixWithLabelsOnHeapData> dataset = datasetBuilder.build(
            LearningEnvironmentBuilder.defaultBuilder(),
            new EmptyContextBuilder<>(),
            new FeatureMatrixWithLabelsOnHeapDataBuilder<>(preprocessor),
            LearningEnvironment.DEFAULT_TRAINER_ENV
        )) {
            return evaluate(mdl, dataset, metrics);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    private static EvaluationResult evaluate(IgniteModel<Vector, Double> mdl,
        Dataset<EmptyContext, FeatureMatrixWithLabelsOnHeapData> dataset, Metric[] metrics) {
        final Map<MetricName, Metric> metricMap = new HashMap<>();
        final Map<MetricName, Class> metricToAggrCls = new HashMap<>();
        for (Metric metric : metrics) {
            MetricStatsAggregator aggregator = metric.makeAggregator();
            MetricName name = metric.name();

            metricToAggrCls.put(name, aggregator.getClass());
            metricMap.put(name, metric);
        }

        Map<MetricName, Double> res = new HashMap<>();

        final Map<Class, EvaluationContext> aggrClsToCtx = initEvaluationContexts(dataset, metrics);
        final Map<Class, MetricStatsAggregator> aggrClsToAggr = computeStats(mdl, dataset, aggrClsToCtx, metrics);

        for (Metric metric : metrics) {
            MetricName name = metric.name();
            Class aggrCls = metricToAggrCls.get(name);
            MetricStatsAggregator aggr = aggrClsToAggr.get(aggrCls);
            res.put(name, metricMap.get(name).initBy(aggr).value());
        }

        return new EvaluationResult(res);
    }

    @SuppressWarnings("unchecked")
    private static Map<Class, EvaluationContext> initEvaluationContexts(
        Dataset<EmptyContext, FeatureMatrixWithLabelsOnHeapData> dataset,
        Metric... metrics
    ) {
        long nonEmptyCtxsCnt = Arrays.stream(metrics)
            .map(x -> x.makeAggregator().initialContext())
            .filter(x -> ((EvaluationContext)x).needToCompute())
            .count();

        if (nonEmptyCtxsCnt == 0) {
            HashMap<Class, EvaluationContext> res = new HashMap<>();

            for (Metric m : metrics) {
                MetricStatsAggregator<Double, ?, ?> aggregator = m.makeAggregator();
                res.put(aggregator.getClass(), (EvaluationContext)m.makeAggregator().initialContext());
                return res;
            }
        }

        return dataset.compute(data -> {
            Map<Class, MetricStatsAggregator> aggrs = new HashMap<>();
            for (Metric m : metrics) {
                MetricStatsAggregator<Double, ?, ?> aggregator = m.makeAggregator();
                if (!aggrs.containsKey(aggregator.getClass()))
                    aggrs.put(aggregator.getClass(), aggregator);
            }

            Map<Class, EvaluationContext> aggrToEvCtx = new HashMap<>();
            aggrs.forEach((clazz, aggr) -> aggrToEvCtx.put(clazz, (EvaluationContext)aggr.initialContext()));

            for (int i = 0; i < data.getLabels().length; i++) {
                LabeledVector<Double> vector = VectorUtils.of(data.getFeatures()[i]).labeled(data.getLabels()[i]);
                aggrToEvCtx.values().stream().forEach(ctx -> ctx.aggregate(vector));
            }
            return aggrToEvCtx;
        }, (left, right) -> {
            if (left == null && right == null)
                return new HashMap<>();

            if (left == null)
                return right;
            if (right == null)
                return left;

            HashMap<Class, EvaluationContext> res = new HashMap<>();
            for (Class key : left.keySet()) {
                EvaluationContext ctx1 = left.get(key);
                EvaluationContext ctx2 = right.get(key);
                A.ensure(ctx1 != null && ctx2 != null, "ctx1 != null && ctx2 != null");
                res.put(key, ctx1.mergeWith(ctx2));
            }
            return res;
        });
    }

    @SuppressWarnings("unchecked")
    private static Map<Class, MetricStatsAggregator> computeStats(IgniteModel<Vector, Double> mdl,
        Dataset<EmptyContext, FeatureMatrixWithLabelsOnHeapData> dataset,
        Map<Class, EvaluationContext> ctxs, Metric... metrics) {

        return dataset.compute(data -> {
            Map<Class, MetricStatsAggregator> aggrs = new HashMap<>();
            for (Metric m : metrics) {
                MetricStatsAggregator aggregator = m.makeAggregator();
                EvaluationContext ctx = ctxs.get(aggregator.getClass());
                A.ensure(ctx != null, "ctx != null");
                aggregator.initByContext(ctx);

                if (!aggrs.containsKey(aggregator.getClass()))
                    aggrs.put(aggregator.getClass(), aggregator);
            }

            for (int i = 0; i < data.getLabels().length; i++) {
                LabeledVector<Double> vector = VectorUtils.of(data.getFeatures()[i]).labeled(data.getLabels()[i]);
                for (Class key : aggrs.keySet()) {
                    MetricStatsAggregator aggr = aggrs.get(key);
                    aggr.aggregate(mdl, vector);
                }
            }

            return aggrs;
        }, (left, right) -> {
            if (left == null && right == null)
                return new HashMap<>();
            if (left == null)
                return right;
            if (right == null)
                return left;

            HashMap<Class, MetricStatsAggregator> res = new HashMap<>();
            for (Class key : left.keySet()) {
                MetricStatsAggregator agg1 = left.get(key);
                MetricStatsAggregator agg2 = right.get(key);
                A.ensure(agg1 != null && agg2 != null, "agg1 != null && agg2 != null");
                res.put(key, agg1.mergeWith(agg2));
            }
            return res;
        });
    }
}
