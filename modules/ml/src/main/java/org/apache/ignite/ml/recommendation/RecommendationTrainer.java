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

package org.apache.ignite.ml.recommendation;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.primitive.builder.context.EmptyContextBuilder;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.environment.LearningEnvironment;
import org.apache.ignite.ml.environment.LearningEnvironmentBuilder;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.recommendation.util.MatrixFactorizationGradient;
import org.apache.ignite.ml.recommendation.util.RecommendationDatasetData;
import org.apache.ignite.ml.recommendation.util.RecommendationDatasetDataBuilder;

/**
 * Trainer of the recommendation system.
 */
public class RecommendationTrainer {
    /** Environment builder. */
    private LearningEnvironmentBuilder environmentBuilder = LearningEnvironmentBuilder.defaultBuilder();

    /** Trainer learning environment. */
    private LearningEnvironment trainerEnvironment = environmentBuilder.buildForTrainer();

    /** Batch size of stochastic gradient descent. The size of a dataset used on each step of SGD. */
    private int batchSize = 1000;

    /** Regularization parameter. */
    private double regParam = 0.0;

    /** Learning rate. */
    private double learningRate = 10.0;

    /** Max number of SGD iterations. */
    private double maxIterations = 1000;

    /** Number of rows/cols in matrices after factorization. */
    private int k = 10;

    /**
     * Fits prediction model.
     *
     * @param datasetBuilder Dataset builder.
     * @param <K> Type of a key in {@code upstream} data.
     * @param <O> Type of an object.
     * @param <S> Type of a subject.
     * @return Trained recommendation model.
     */
    public <K, O extends Serializable, S extends Serializable> RecommendationModel<O, S> fit(
        DatasetBuilder<K, ? extends ObjectSubjectRatingTriplet<O, S>> datasetBuilder) {
        try (Dataset<EmptyContext, RecommendationDatasetData<O, S>> dataset = datasetBuilder.build(
            environmentBuilder,
            new EmptyContextBuilder<>(),
            new RecommendationDatasetDataBuilder<>(),
            trainerEnvironment
        )) {
            // Collect total set of objects and subjects (their identifiers).
            Set<O> objects = dataset.compute(RecommendationDatasetData::getObjects, RecommendationTrainer::join);
            Set<S> subjects = dataset.compute(RecommendationDatasetData::getSubjects, RecommendationTrainer::join);

            // Generate initial model (object and subject matrices) initializing them with random values.
            Map<O, Vector> objMatrix = generateRandomVectorForEach(objects, trainerEnvironment.randomNumbersGenerator());
            Map<S, Vector> subjMatrix = generateRandomVectorForEach(subjects, trainerEnvironment.randomNumbersGenerator());

            // SGD steps.
            // TODO: GG-22916 Add convergence check into recommendation system SGD
            for (int i = 0; i < maxIterations; i++) {
                int seed = i;

                // Calculate gradient on reach partition and aggregate results.
                MatrixFactorizationGradient<O, S> grad = dataset.compute(
                    (data, env) -> data.calculateGradient(
                        objMatrix,
                        subjMatrix,
                        batchSize,
                        seed ^ env.partition(),
                        regParam,
                        learningRate
                    ),
                    RecommendationTrainer::sum
                );

                // Apply aggregated gradient.
                grad.applyGradient(objMatrix, subjMatrix);
            }

            return new RecommendationModel<>(objMatrix, subjMatrix);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Generates a random vector with length {@link #k} for each given object.
     *
     * @param objects Collection of object.
     * @param rnd Random generator.
     * @param <T> Type of an object.
     * @return Pairs of objects and generated vectors.
     */
    private <T> Map<T, Vector> generateRandomVectorForEach(Collection<T> objects, Random rnd) {
        Map<T, Vector> res = new HashMap<>();
        for (T obj : objects)
            res.put(obj, randomVector(k, rnd));

        return res;
    }

    /**
     * Joins two sets ({@code null} values are acceptable).
     *
     * @param a First set.
     * @param b Second set.
     * @param <T> Type of set elements.
     * @return Joined set.
     */
    private static <T> Set<T> join(Set<T> a, Set<T> b) {
        if (a == null)
            return b;

        if (b != null)
            a.addAll(b);

        return a;
    }

    /**
     * Set up learning environment builder.
     *
     * @param environmentBuilder Learning environment builder.
     * @return This object.
     */
    public RecommendationTrainer withLearningEnvironmentBuilder(LearningEnvironmentBuilder environmentBuilder) {
        this.environmentBuilder = environmentBuilder;

        return this;
    }

    /**
     * Set up trainer learning environment.
     *
     * @param trainerEnvironment Trainer learning environment.
     * @return This object.
     */
    public RecommendationTrainer withTrainerEnvironment(LearningEnvironment trainerEnvironment) {
        this.trainerEnvironment = trainerEnvironment;

        return this;
    }

    /**
     * Set up batch size parameter.
     *
     * @param batchSize Batch size of stochastic gradient descent. The size of a dataset used on each step of SGD.
     * @return This object.
     */
    public RecommendationTrainer withBatchSize(int batchSize) {
        this.batchSize = batchSize;

        return this;
    }

    /**
     * Set up regularization parameter.
     *
     * @param regParam Regularization parameter.
     * @return This object.
     */
    public RecommendationTrainer withRegularizer(double regParam) {
        this.regParam = regParam;

        return this;
    }

    /**
     * Set up learning rate parameter.
     *
     * @param learningRate Learning rate.
     * @return This object.
     */
    public RecommendationTrainer withLearningRate(double learningRate) {
        this.learningRate = learningRate;

        return this;
    }

    /**
     * Set up max iterations parameter.
     *
     * @param maxIterations Max iterations.
     * @return This object.
     */
    public RecommendationTrainer withMaxIterations(int maxIterations) {
        this.maxIterations = maxIterations;

        return this;
    }

    /**
     * Set up {@code k} parameter (number of rows/cols in matrices after factorization).
     *
     * @param k Number of rows/cols in matrices after factorization
     * @return This object.
     */
    public RecommendationTrainer withK(int k) {
        this.k = k;

        return this;
    }

    /**
     * Returns sum of two matrix factorization gradients.
     *
     * @param a First gradient.
     * @param b Second gradient.
     * @param <O> Type of object.
     * @param <S> Type ot subject.
     * @return Sum of two matrix factorization gradients.
     */
    private static <O extends Serializable, S extends Serializable> MatrixFactorizationGradient<O, S> sum(
        MatrixFactorizationGradient<O, S> a,
        MatrixFactorizationGradient<O, S> b) {
        return new MatrixFactorizationGradient<>(
            sum(a == null ? null : a.getObjGrad(), b == null ? null : b.getObjGrad()),
            sum(a == null ? null : a.getSubjGrad(), b == null ? null : b.getSubjGrad())
        );
    }

    /**
     * Returns sum of two matrices.
     *
     * @param a First matrix.
     * @param b Second matrix.
     * @param <T> Type of a key.
     * @return Sum of two matrices.
     */
    private static <T> Map<T, Vector> sum(Map<T, Vector> a, Map<T, Vector> b) {
        if (a == null)
            return b;

        if (b == null)
            return a;

        Map<T, Vector> res = new HashMap<>();

        for (Map<T, Vector> map : Arrays.asList(a, b)) {
            for (Map.Entry<T, Vector> e : map.entrySet()) {
                Vector vector = res.get(e.getKey());
                res.put(e.getKey(), vector == null ? e.getValue() : e.getValue().plus(vector));
            }
        }

        return Collections.unmodifiableMap(res);
    }

    /**
     * Generates a new randomized vector with length {@code k} and max value {@code max}.
     *
     * @param k Cardinality of the vector.
     * @param rnd Random.
     * @return Randomized vector.
     */
    private static Vector randomVector(int k, Random rnd) {
        double[] vector = new double[k];
        for (int i = 0; i < vector.length; i++)
            vector[i] = rnd.nextDouble();

        return VectorUtils.of(vector);
    }
}
