/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 * 
 * Commons Clause Restriction
 * 
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 * 
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 * 
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.ml.inference.builder;

import java.io.Serializable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.ignite.ml.inference.Model;
import org.apache.ignite.ml.inference.parser.ModelParser;
import org.apache.ignite.ml.inference.reader.ModelReader;

/**
 * Implementation of asynchronous inference model builder that builds model processed locally utilizing specified number
 * of threads.
 */
public class ThreadedModelBuilder implements AsyncModelBuilder {
    /** Number of threads to be utilized for model inference. */
    private final int threads;

    /**
     * Constructs a new instance of threaded inference model builder.
     *
     * @param threads Number of threads to be utilized for model inference.
     */
    public ThreadedModelBuilder(int threads) {
        this.threads = threads;
    }

    /** {@inheritDoc} */
    @Override public <I extends Serializable, O extends Serializable> Model<I, Future<O>> build(
        ModelReader reader, ModelParser<I, O, ?> parser) {
        return new ThreadedInfModel<>(parser.parse(reader.read()), threads);
    }

    /**
     * Threaded inference model that performs inference in multiply threads.
     *
     * @param <I> Type of model input.
     * @param <O> Type of model output.
     */
    private static class ThreadedInfModel<I extends Serializable, O extends Serializable>
        implements Model<I, Future<O>> {
        /** Inference model. */
        private final Model<I, O> mdl;

        /** Thread pool. */
        private final ExecutorService threadPool;

        /**
         * Constructs a new instance of threaded inference model.
         *
         * @param mdl Inference model.
         * @param threads Thread pool.
         */
        ThreadedInfModel(Model<I, O> mdl, int threads) {
            this.mdl = mdl;
            this.threadPool = Executors.newFixedThreadPool(threads);
        }

        /** {@inheritDoc} */
        @Override public Future<O> predict(I input) {
            return threadPool.submit(() -> mdl.predict(input));
        }

        /** {@inheritDoc} */
        @Override public void close() {
            threadPool.shutdown();
        }
    }
}
