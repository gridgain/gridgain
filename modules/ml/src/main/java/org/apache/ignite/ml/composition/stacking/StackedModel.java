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

package org.apache.ignite.ml.composition.stacking;

import org.apache.ignite.ml.IgniteModel;
import org.apache.ignite.ml.composition.combinators.parallel.ModelsParallelComposition;

/**
 * This is a wrapper for model produced by {@link StackedDatasetTrainer}.
 * Model consisting of two layers:
 * <pre>
 *     1. Submodels layer {@code (IS -> IA)}.
 *     2. Aggregator layer {@code (IA -> O)}.
 * </pre>
 * Submodels layer is a {@link ModelsParallelComposition} of several models {@code IS -> IA} each of them getting same input
 * {@code IS} and produce own output, these outputs outputs {@code [IA]}
 * are combined into a single output with a given binary "merger" operator {@code IA -> IA -> IA}. Result of merge
 * is then passed to the aggregator layer.
 * Aggregator layer consists of a model {@code IA -> O}.
 *
 * @param <IS> Type of submodels input.
 * @param <IA> Type of submodels output (same as aggregator model input).
 * @param <O> Type of aggregator model output.
 * @param <AM> Type of aggregator model.
 */
public class StackedModel<IS, IA, O, AM extends IgniteModel<IA, O>> implements IgniteModel<IS, O> {
    /** Model to wrap. */
    private IgniteModel<IS, O> mdl;

    /**
     * Construct instance of this class from {@link IgniteModel}.
     * @param mdl
     */
    StackedModel(IgniteModel<IS, O> mdl) {
        this.mdl = mdl;
    }

    /** {@inheritDoc} */
    @Override public O predict(IS is) {
        return mdl.predict(is);
    }

    /** {@inheritDoc} */
    @Override public void close() {
        mdl.close();
    }
}
