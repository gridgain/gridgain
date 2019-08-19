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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.ml.IgniteModel;
import org.apache.ignite.ml.composition.ModelsComposition;
import org.apache.ignite.ml.composition.bagging.BaggedModel;
import org.apache.ignite.ml.composition.combinators.parallel.ModelsParallelComposition;
import org.apache.ignite.ml.composition.combinators.sequential.ModelsSequentialComposition;
import org.apache.ignite.ml.composition.stacking.StackedModel;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.primitive.FeatureMatrixWithLabelsOnHeapData;
import org.apache.ignite.ml.dataset.primitive.FeatureMatrixWithLabelsOnHeapDataBuilder;
import org.apache.ignite.ml.dataset.primitive.builder.context.EmptyContextBuilder;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.environment.LearningEnvironment;
import org.apache.ignite.ml.environment.LearningEnvironmentBuilder;
import org.apache.ignite.ml.knn.ann.ANNClassificationModel;
import org.apache.ignite.ml.knn.classification.KNNClassificationModel;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.multiclass.MultiClassModel;
import org.apache.ignite.ml.naivebayes.discrete.DiscreteNaiveBayesModel;
import org.apache.ignite.ml.naivebayes.gaussian.GaussianNaiveBayesModel;
import org.apache.ignite.ml.nn.MultilayerPerceptron;
import org.apache.ignite.ml.preprocessing.Preprocessor;
import org.apache.ignite.ml.selection.scoring.cursor.CacheBasedLabelPairCursor;
import org.apache.ignite.ml.selection.scoring.cursor.LabelPairCursor;
import org.apache.ignite.ml.selection.scoring.cursor.LocalLabelPairCursor;
import org.apache.ignite.ml.selection.scoring.evaluator.aggregator.MetricStatsAggregator;
import org.apache.ignite.ml.selection.scoring.evaluator.context.EvaluationContext;
import org.apache.ignite.ml.selection.scoring.evaluator.metric.Metric;
import org.apache.ignite.ml.selection.scoring.evaluator.metric.MetricName;
import org.apache.ignite.ml.selection.scoring.metric.AbstractMetrics;
import org.apache.ignite.ml.selection.scoring.metric.OldMetric;
import org.apache.ignite.ml.selection.scoring.metric.MetricValues;
import org.apache.ignite.ml.selection.scoring.metric.classification.BinaryClassificationMetricValues;
import org.apache.ignite.ml.selection.scoring.metric.classification.BinaryClassificationMetrics;
import org.apache.ignite.ml.selection.scoring.metric.regression.RegressionMetricValues;
import org.apache.ignite.ml.selection.scoring.metric.regression.RegressionMetrics;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.svm.SVMLinearClassificationModel;
import org.apache.ignite.ml.tree.DecisionTree;
import org.apache.ignite.ml.tree.DecisionTreeClassificationTrainer;
import org.apache.ignite.ml.tree.DecisionTreeNode;

/**
 * Evaluator that computes metrics from predictions and ground truth values.
 */
public class Evaluator {
    /**
     * Computes the given metric on the given cache.
     *
     * @param dataCache    The given cache.
     * @param mdl          The model.
     * @param preprocessor The preprocessor.
     * @param metric       The binary classification metric.
     * @param <K>          The type of cache entry key.
     * @param <V>          The type of cache entry value.
     * @return Computed metric.
     */
    public static <L, K, V> double evaluate(IgniteCache<K, V> dataCache,
                                            IgniteModel<Vector, L> mdl,
                                            Preprocessor<K, V> preprocessor,
                                            OldMetric<L> metric
    ) {

        return calculateMetric(
            dataCache,
            null,
            mdl,
            preprocessor,
            metric
        );
    }

    /**
     * Computes the given metric on the given cache.
     *
     * @param dataCache    The given local data.
     * @param mdl          The model.
     * @param preprocessor The preprocessor.
     * @param metric       The binary classification metric.
     * @param <K>          The type of cache entry key.
     * @param <V>          The type of cache entry value.
     * @return Computed metric.
     */
    public static <L, K, V> double evaluate(Map<K, V> dataCache,
                                            IgniteModel<Vector, L> mdl,
                                            Preprocessor<K, V> preprocessor,
                                            OldMetric<L> metric) {
        return calculateMetric(dataCache, null, mdl, preprocessor, metric);
    }

    /**
     * Computes the given metric on the given cache.
     *
     * @param dataCache    The given cache.
     * @param mdl          The model.
     * @param preprocessor The preprocessor.
     * @param metric       The binary classification metric.
     * @param <K>          The type of cache entry key.
     * @param <V>          The type of cache entry value.
     * @return Computed metric.
     */
    public static <L, K, V> double evaluate(IgniteCache<K, V> dataCache,
                                            IgniteBiPredicate<K, V> filter,
                                            IgniteModel<Vector, L> mdl,
                                            Preprocessor<K, V> preprocessor,
                                            OldMetric<L> metric) {

        return calculateMetric(
            dataCache, filter, mdl,
            preprocessor,
            metric
        );
    }

    /**
     * Computes the given metric on the given cache.
     *
     * @param dataCache    The given cache.
     * @param filter       The given filter.
     * @param mdl          The model.
     * @param preprocessor The preprocessor.
     * @param metric       The binary classification metric.
     * @param <L>          The type of label.
     * @param <K>          The type of cache entry key.
     * @param <V>          The type of cache entry value.
     * @return Computed metric.
     */
    public static <L, K, V> double evaluate(Map<K, V> dataCache, IgniteBiPredicate<K, V> filter,
                                            IgniteModel<Vector, L> mdl,
                                            Preprocessor<K, V> preprocessor,
                                            OldMetric<L> metric) {
        return calculateMetric(dataCache, filter, mdl, preprocessor, metric);
    }

    /**
     * Computes the given metrics on the given cache.
     *
     * @param dataCache    The given cache.
     * @param mdl          The model.
     * @param preprocessor The preprocessor.
     * @param <K>          The type of cache entry key.
     * @param <V>          The type of cache entry value.
     * @return Computed metric.
     */
    public static <K, V> BinaryClassificationMetricValues evaluate(IgniteCache<K, V> dataCache,
                                                                   IgniteModel<Vector, Double> mdl,
                                                                   Preprocessor<K, V> preprocessor) {
        return calcMetricValues(dataCache, null, mdl, preprocessor);
    }

    /**
     * Computes the given metrics on the given cache.
     *
     * @param dataCache    The given cache.
     * @param mdl          The model.
     * @param preprocessor The preprocessor.
     * @param <K>          The type of cache entry key.
     * @param <V>          The type of cache entry value.
     * @return Computed metric.
     */
    public static <K, V> BinaryClassificationMetricValues evaluate(Map<K, V> dataCache,
                                                                   IgniteModel<Vector, Double> mdl,
                                                                   Preprocessor<K, V> preprocessor) {
        return calcMetricValues(dataCache, null, mdl, preprocessor);
    }


    /**
     * Computes the given metrics on the given cache.
     *
     * @param dataCache    The given cache.
     * @param filter       The given filter.
     * @param mdl          The model.
     * @param preprocessor The preprocessor.
     * @param <K>          The type of cache entry key.
     * @param <V>          The type of cache entry value.
     * @return Computed metric.
     */
    public static <K, V> BinaryClassificationMetricValues evaluate(IgniteCache<K, V> dataCache,
                                                                                           IgniteBiPredicate<K, V> filter,
                                                                                           IgniteModel<Vector, Double> mdl,
                                                                                           Preprocessor<K, V> preprocessor) {
        return calcMetricValues(
            dataCache, filter,
            mdl,
            preprocessor
        );
    }

    /**
     * Computes the given metrics on the given cache.
     *
     * @param dataCache    The given cache.
     * @param filter       The given filter.
     * @param mdl          The model.
     * @param preprocessor The preprocessor.
     * @param <K>          The type of cache entry key.
     * @param <V>          The type of cache entry value.
     * @return Computed metric.
     */
    public static <K, V> BinaryClassificationMetricValues evaluate(Map<K, V> dataCache, IgniteBiPredicate<K, V> filter,
                                                                   IgniteModel<Vector, Double> mdl,
                                                                   Preprocessor<K, V> preprocessor) {
        return calcMetricValues(dataCache, filter, mdl, preprocessor);
    }

    /**
     * Computes the regression metrics on the given cache.
     *
     * @param dataCache Data cache.
     * @param filter Filter.
     * @param mdl Model.
     * @param preprocessor Preprocessor.
     * @param <K> The type of cache entry key.
     * @param <V> The type of cache entry value.
     * @return Computed metric.
     */
    public static <K, V> RegressionMetricValues evaluateRegression(IgniteCache<K, V> dataCache,
        IgniteBiPredicate<K, V> filter,
        IgniteModel<Vector, Double> mdl,
        Preprocessor<K, V> preprocessor) {
        return calcRegressionMetricValues(
            dataCache,
            filter,
            mdl,
            preprocessor
        );
    }

    /**
     * Computes the regression metrics on the given cache.
     *
     * @param dataCache Data cache.
     * @param filter Filter.
     * @param mdl Model.
     * @param preprocessor Preprocessor.
     * @param <K> The type of cache entry key.
     * @param <V> The type of cache entry value.
     * @return Computed metric.
     */
    private static <K, V> RegressionMetricValues calcRegressionMetricValues(IgniteCache<K, V> dataCache,
        IgniteBiPredicate<K, V> filter,
        IgniteModel<Vector, Double> mdl,
        Preprocessor<K, V> preprocessor) {
        return calcMetricValues(dataCache, filter, mdl, preprocessor, new RegressionMetrics());
    }

    /**
     * Computes the given metrics on the given cache.
     *
     * @param dataCache    The given cache.
     * @param filter       The given filter.
     * @param mdl          The model.
     * @param preprocessor The preprocessor.
     * @param <K>          The type of cache entry key.
     * @param <V>          The type of cache entry value.
     * @return Computed metric.
     */
    private static <K, V> BinaryClassificationMetricValues calcMetricValues(IgniteCache<K, V> dataCache,
                                                                            IgniteBiPredicate<K, V> filter,
                                                                            IgniteModel<Vector, Double> mdl,
                                                                            Preprocessor<K, V> preprocessor) {
        BinaryClassificationMetricValues metricValues;
        BinaryClassificationMetrics binaryMetrics = new BinaryClassificationMetrics();

        try (LabelPairCursor<Double> cursor = new CacheBasedLabelPairCursor<>(
            dataCache,
            filter,
            preprocessor,
            mdl
        )) {
            metricValues = binaryMetrics.scoreAll(cursor.iterator());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return metricValues;
    }

    /**
     * Computes the given metrics on the given cache.
     *
     * @param dataCache    The given cache.
     * @param filter       The given filter.
     * @param mdl          The model.
     * @param preprocessor The preprocessor.
     * @param <K>          The type of cache entry key.
     * @param <V>          The type of cache entry value.
     * @return Computed metric.
     */
    private static <K, V> BinaryClassificationMetricValues calcMetricValues(Map<K, V> dataCache,
                                                                            IgniteBiPredicate<K, V> filter,
                                                                            IgniteModel<Vector, Double> mdl,
                                                                            Preprocessor<K, V> preprocessor) {
        BinaryClassificationMetricValues metricValues;
        BinaryClassificationMetrics binaryMetrics = new BinaryClassificationMetrics();

        try (LabelPairCursor<Double> cursor = new LocalLabelPairCursor<>(
            dataCache,
            filter,
            preprocessor,
            mdl
        )) {
            metricValues = binaryMetrics.scoreAll(cursor.iterator());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return metricValues;
    }

    /**
     * Computes the given metric on the given cache.
     *
     * @param dataCache    The given cache.
     * @param filter       The given filter.
     * @param mdl          The model.
     * @param preprocessor The preprocessor.
     * @param metric       The binary classification metric.
     * @param <L>          The type of label.
     * @param <K>          The type of cache entry key.
     * @param <V>          The type of cache entry value.
     * @return Computed metric.
     */
    private static <L, K, V> double calculateMetric(IgniteCache<K, V> dataCache, IgniteBiPredicate<K, V> filter,
                                                    IgniteModel<Vector, L> mdl, Preprocessor<K, V> preprocessor, OldMetric<L> metric) {
        double metricRes;

        try (LabelPairCursor<L> cursor = new CacheBasedLabelPairCursor<>(
            dataCache,
            filter,
            preprocessor,
            mdl
        )) {
            metricRes = metric.score(cursor.iterator());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return metricRes;
    }

    /**
     * Computes the given metric on the given cache.
     *
     * @param dataCache    The given cache.
     * @param filter       The given filter.
     * @param mdl          The model.
     * @param preprocessor The preprocessor.
     * @param metric       The binary classification metric.
     * @param <L>          The type of label.
     * @param <K>          The type of cache entry key.
     * @param <V>          The type of cache entry value.
     * @return Computed metric.
     */
    private static <L, K, V> double calculateMetric(Map<K, V> dataCache, IgniteBiPredicate<K, V> filter,
                                                    IgniteModel<Vector, L> mdl, Preprocessor<K, V> preprocessor, OldMetric<L> metric) {
        double metricRes;

        try (LabelPairCursor<L> cursor = new LocalLabelPairCursor<>(
            dataCache,
            filter,
            preprocessor,
            mdl
        )) {
            metricRes = metric.score(cursor.iterator());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return metricRes;
    }

    /**
     * Computes regression metrics on the given cache.
     *
     * @param dataCache The given cache.
     * @param filter The given filter.
     * @param mdl The model.
     * @param preprocessor The preprocessor.
     * @param <K> The type of cache entry key.
     * @param <V> The type of cache entry value.
     * @return Computed metric.
     */
    private static <K, V, M extends MetricValues> M calcMetricValues(IgniteCache<K, V> dataCache,
        IgniteBiPredicate<K, V> filter,
        IgniteModel<Vector, Double> mdl,
        Preprocessor<K, V> preprocessor, AbstractMetrics<M> metrics) {
        M metricValues;

        try (LabelPairCursor<Double> cursor = new CacheBasedLabelPairCursor<>(
            dataCache,
            filter,
            preprocessor,
            mdl
        )) {
            metricValues = metrics.scoreAll(cursor.iterator());
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }

        return metricValues;
    }

    @SuppressWarnings("unchecked")
    public static <K,V> EvaluationResult evaluate(IgniteModel<Vector, Double> mdl,
        DatasetBuilder<K, V> datasetBuilder, Preprocessor<K, V> preprocessor, MetricName ... metrics) {
        final Map<MetricName, Metric> metricMap = new HashMap<>();
        final Map<MetricName, Class> metricToAggrCls = new HashMap<>();
        for (MetricName name : metrics) {
            Metric metric = name.create();
            MetricStatsAggregator aggregator = metric.makeAggregator();

            metricToAggrCls.put(name, aggregator.getClass());
            metricMap.put(name, metric);
        }

        Map<MetricName, Double> res = new HashMap<>();
        try (Dataset<EmptyContext, FeatureMatrixWithLabelsOnHeapData> dataset = datasetBuilder.build(
            LearningEnvironmentBuilder.defaultBuilder(),
            new EmptyContextBuilder<>(),
            new FeatureMatrixWithLabelsOnHeapDataBuilder<>(preprocessor),
            LearningEnvironment.DEFAULT_TRAINER_ENV
        )) {
            final Map<Class, EvaluationContext> aggrClsToCtx = initEvaluationContexts(dataset, metrics);
            final Map<Class, MetricStatsAggregator> aggrClsToAggr = computeStats(mdl, dataset, aggrClsToCtx, metrics);

            for (MetricName name : metrics) {
                Class aggrCls = metricToAggrCls.get(name);
                MetricStatsAggregator aggr = aggrClsToAggr.get(aggrCls);
                res.put(name, metricMap.get(name).initBy(aggr).value());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return new EvaluationResult(res);
    }

    @SuppressWarnings("unchecked")
    private static Map<Class, EvaluationContext> initEvaluationContexts(
        Dataset<EmptyContext, FeatureMatrixWithLabelsOnHeapData> dataset,
        MetricName ... metrics
    ) {
        List<Metric> ms = Arrays.stream(metrics).map(MetricName::create).collect(Collectors.toList());

        long nonEmptyCtxsCnt = ms.stream()
            .map(x -> x.makeAggregator().initialContext())
            .filter(x -> ((EvaluationContext) x).needToCompute())
            .count();

        if (nonEmptyCtxsCnt == 0) {
            HashMap<Class, EvaluationContext> res = new HashMap<>();

            for (Metric m : ms) {
                MetricStatsAggregator<Double, ?, ?> aggregator = m.makeAggregator();
                res.put(aggregator.getClass(), EvaluationContext.empty());
                return res;
            }
        }

        return dataset.compute(data -> {
            Map<Class, MetricStatsAggregator> aggrs = new HashMap<>();
            for (Metric m : ms) {
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
    private static Map<Class, MetricStatsAggregator> computeStats(
        IgniteModel<Vector, Double> mdl, Dataset<EmptyContext, FeatureMatrixWithLabelsOnHeapData> dataset,
        Map<Class, EvaluationContext> ctxs, MetricName ... metrics) {

        return dataset.compute(data -> {
            Map<Class, MetricStatsAggregator> aggrs = new HashMap<>();
            for (MetricName m : metrics) {
                MetricStatsAggregator aggregator = m.create().makeAggregator();
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
