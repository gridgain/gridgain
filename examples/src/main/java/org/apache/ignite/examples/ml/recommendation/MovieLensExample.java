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

package org.apache.ignite.examples.ml.recommendation;

import java.util.Scanner;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.ml.dataset.impl.cache.CacheBasedDatasetBuilder;
import org.apache.ignite.ml.environment.LearningEnvironmentBuilder;
import org.apache.ignite.ml.recommendation.ObjectSubjectRatingTriplet;
import org.apache.ignite.ml.recommendation.RecommendationModel;
import org.apache.ignite.ml.recommendation.RecommendationTrainer;

/**
 * Example of recommendation system based on MovieLens dataset (see https://grouplens.org/datasets/movielens/).
 */
public class MovieLensExample {
    /** Path to MovieLens dataset (ratings). */
    private static final String MOVIELENS_DATASET = "datasets/ratings.csv";

    /** Run example. */
    public static void main(String[] args) {
        System.out.println();
        System.out.println(">>> Recommendation system over cache based dataset usage example started.");
        // Start ignite grid.
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println(">>> Ignite grid started.");

            IgniteCache<Integer, RatingPoint> movielensCache = loadMovieLensDataset(ignite, 10_000);
            try {
                LearningEnvironmentBuilder envBuilder = LearningEnvironmentBuilder.defaultBuilder().withRNGSeed(1);
                RecommendationTrainer trainer = new RecommendationTrainer()
                    .withMaxIterations(1000)
                    .withBatchSize(100)
                    .withLearningRate(1)
                    .withLearningEnvironmentBuilder(envBuilder)
                    .withTrainerEnvironment(envBuilder.buildForTrainer());

                RecommendationModel<Integer, Integer> mdl = trainer.fit(
                    new CacheBasedDatasetBuilder<>(ignite, movielensCache)
                );

                double mean = 0;
                try (QueryCursor<Cache.Entry<Integer, RatingPoint>> cursor = movielensCache.query(new ScanQuery<>())) {
                    for (Cache.Entry<Integer, RatingPoint> e : cursor) {
                        ObjectSubjectRatingTriplet<Integer, Integer> triplet = e.getValue();
                        mean += triplet.getRating();
                    }
                    mean /= movielensCache.size();
                }

                double tss = 0, rss = 0;
                try (QueryCursor<Cache.Entry<Integer, RatingPoint>> cursor = movielensCache.query(new ScanQuery<>())) {
                    for (Cache.Entry<Integer, RatingPoint> e : cursor) {
                        ObjectSubjectRatingTriplet<Integer, Integer> triplet = e.getValue();
                        tss += Math.pow(triplet.getRating() - mean, 2);
                        rss += Math.pow(triplet.getRating() - mdl.predict(triplet), 2);
                    }
                }

                double r2 = 1.0 - rss / tss;

                System.out.println("R2 score: " + r2);

            } finally {
                movielensCache.destroy();
            }
        } finally {
            System.out.flush();
        }
    }

    /**
     * Loads MovieLens dataset into cache.
     *
     * @param ignite Ignite instance.
     * @param cnt Number of rating point to be loaded.
     * @return Ignite cache with loaded MovieLens dataset.
     */
    private static IgniteCache<Integer, RatingPoint> loadMovieLensDataset(Ignite ignite, int cnt) {
        CacheConfiguration<Integer, RatingPoint> cacheConfiguration = new CacheConfiguration<>();
        cacheConfiguration.setAffinity(new RendezvousAffinityFunction(false, 10));
        cacheConfiguration.setName("MOVIELENS");

        IgniteCache<Integer, RatingPoint> dataCache = ignite.createCache(cacheConfiguration);

        Scanner scanner = new Scanner(MovieLensExample.class.getClassLoader()
            .getResourceAsStream(MOVIELENS_DATASET));
        scanner.nextLine(); // Skip header.
        int seq = 0;
        while (scanner.hasNextLine()) {
            String[] line = scanner.nextLine().split(",");
            int userId = Integer.valueOf(line[0]);
            int movieId = Integer.valueOf(line[1]);
            double rating = Double.valueOf(line[2]);

            dataCache.put(seq++, new RatingPoint(movieId, userId, rating));

            if (seq == cnt)
                break;
        }

        return dataCache;
    }

    /**
     * Rating point that represents a result of assesment of a single movie by a single user.
     */
    private static class RatingPoint extends ObjectSubjectRatingTriplet<Integer, Integer> {
        /** */
        private static final long serialVersionUID = -7301471870043910312L;

        /**
         * Constructs a new instance of rating point.
         *
         * @param movieId Movie identifier.
         * @param userId User identifier..
         * @param rating Rating.
         */
        public RatingPoint(Integer movieId, Integer userId, Double rating) {
            super(movieId, userId, rating);
        }
    }
}
