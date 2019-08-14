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
import org.apache.ignite.ml.recommendation.ObjectSubjectRatingTriplet;
import org.apache.ignite.ml.recommendation.RecommendationModel;
import org.apache.ignite.ml.recommendation.RecommendationTrainer;

/**
 * Example of recommendation system based on "MovieLens" dataset.
 */
public class MovieLensExample {
    /** Run example. */
    public static void main(String[] args) {
        System.out.println();
        System.out.println(">>> Linear regression model over cache based dataset usage example started.");
        // Start ignite grid.
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println(">>> Ignite grid started.");
            CacheConfiguration<Integer, ObjectSubjectRatingTriplet<Integer, Integer>> cacheConfiguration = new CacheConfiguration<>();
            cacheConfiguration.setAffinity(new RendezvousAffinityFunction(false, 10));
            cacheConfiguration.setName("MOVIELENS");

            IgniteCache<Integer, ObjectSubjectRatingTriplet<Integer, Integer>> dataCache = ignite.createCache(cacheConfiguration);

            try {
                Scanner scanner = new Scanner(MovieLensExample.class.getClassLoader().getResourceAsStream("datasets/ratings.csv"));
                scanner.nextLine(); // Skip header.
                int seq = 0;
                while (scanner.hasNextLine()) {
                    String[] line = scanner.nextLine().split(",");
                    int userId = Integer.valueOf(line[0]);
                    int movieId = Integer.valueOf(line[1]);
                    double rating = Double.valueOf(line[2]);

                    ObjectSubjectRatingTriplet<Integer, Integer> pnt = new ObjectSubjectRatingTriplet<>(userId, movieId, rating);
                    dataCache.put(seq++, pnt);
                }

                RecommendationTrainer trainer = new RecommendationTrainer()
                    .withMaxIterations(10000)
                    .withBatchSize(1000);

                RecommendationModel<Integer, Integer> mdl = trainer.fit(new CacheBasedDatasetBuilder<>(ignite, dataCache));

                int correct = 0, incorrect = 0;
                QueryCursor<Cache.Entry<Integer, ObjectSubjectRatingTriplet<Integer, Integer>>> cursor = dataCache.query(new ScanQuery<>());
                for (Cache.Entry<Integer, ObjectSubjectRatingTriplet<Integer, Integer>> e : cursor) {
                    ObjectSubjectRatingTriplet<Integer, Integer> triplet = e.getValue();
                    double predicted = Math.round(mdl.predict(triplet) * 2) / 2.0;
                    if (Math.abs(triplet.getRating() - predicted) < 0.1) correct++;
                    else incorrect++;
                }

                System.out.println("Accuracy: " + 1.0 * correct / (correct + incorrect));

            } finally {
                dataCache.destroy();
            }
        } finally {
            System.out.flush();
        }
    }
}
