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

import java.util.ArrayList;
import org.apache.ignite.ml.IgniteModel;
import org.apache.ignite.ml.environment.LearningEnvironmentBuilder;
import org.apache.ignite.ml.math.functions.IgniteBinaryOperator;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.trainers.DatasetTrainer;

/**
 * {@link DatasetTrainer} with same type of input and output of submodels.
 *
 * @param <I> Type of submodels input.
 * @param <O> Type of aggregator model output.
 * @param <AM> Type of aggregator model.
 * @param <L> Type of labels.
 */
public class SimpleStackedDatasetTrainer<I, O, AM extends IgniteModel<I, O>, L> extends StackedDatasetTrainer<I, I, O, AM, L> {
    /**
     * Construct instance of this class.
     *
     * @param aggregatingTrainer Aggregator trainer.
     * @param aggregatingInputMerger Function used to merge submodels outputs into one.
     * @param submodelInput2AggregatingInputConverter Function used to convert input of submodel to output of submodel
     * this function is used if user chooses to keep original features.
     */
    public SimpleStackedDatasetTrainer(DatasetTrainer<AM, L> aggregatingTrainer,
        IgniteBinaryOperator<I> aggregatingInputMerger,
        IgniteFunction<I, I> submodelInput2AggregatingInputConverter,
        IgniteFunction<Vector, I> vector2SubmodelInputConverter,
        IgniteFunction<I, Vector> submodelOutput2VectorConverter) {
        super(aggregatingTrainer,
            aggregatingInputMerger,
            submodelInput2AggregatingInputConverter,
            new ArrayList<>(),
            vector2SubmodelInputConverter,
            submodelOutput2VectorConverter);
    }

    /**
     * Construct instance of this class.
     *
     * @param aggregatingTrainer Aggregator trainer.
     * @param aggregatingInputMerger Function used to merge submodels outputs into one.
     */
    public SimpleStackedDatasetTrainer(DatasetTrainer<AM, L> aggregatingTrainer,
        IgniteBinaryOperator<I> aggregatingInputMerger) {
        super(aggregatingTrainer, aggregatingInputMerger, IgniteFunction.identity());
    }

    /**
     * Constructs instance of this class.
     */
    public SimpleStackedDatasetTrainer() {
        super();
    }

    //TODO: IGNITE-10441 -- Look for options to avoid boilerplate overrides.
    /** {@inheritDoc} */
    @Override public <M1 extends IgniteModel<I, I>> SimpleStackedDatasetTrainer<I, O, AM, L> addTrainer(
        DatasetTrainer<M1, L> trainer) {
        return (SimpleStackedDatasetTrainer<I, O, AM, L>)super.addTrainer(trainer);
    }

    /** {@inheritDoc} */
    @Override public SimpleStackedDatasetTrainer<I, O, AM, L> withAggregatorTrainer(
        DatasetTrainer<AM, L> aggregatorTrainer) {
        return (SimpleStackedDatasetTrainer<I, O, AM, L>)super.withAggregatorTrainer(aggregatorTrainer);
    }

    /** {@inheritDoc} */
    @Override public SimpleStackedDatasetTrainer<I, O, AM, L> withOriginalFeaturesDropped() {
        return (SimpleStackedDatasetTrainer<I, O, AM, L>)super.withOriginalFeaturesDropped();
    }

    /** {@inheritDoc} */
    @Override public SimpleStackedDatasetTrainer<I, O, AM, L> withOriginalFeaturesKept(
        IgniteFunction<I, I> submodelInput2AggregatingInputConverter) {
        return (SimpleStackedDatasetTrainer<I, O, AM, L>)super.withOriginalFeaturesKept(
            submodelInput2AggregatingInputConverter);
    }

    /** {@inheritDoc} */
    @Override public SimpleStackedDatasetTrainer<I, O, AM, L> withAggregatorInputMerger(IgniteBinaryOperator<I> merger) {
        return (SimpleStackedDatasetTrainer<I, O, AM, L>)super.withAggregatorInputMerger(merger);
    }

    /** {@inheritDoc} */
    @Override public SimpleStackedDatasetTrainer<I, O, AM, L> withEnvironmentBuilder(
        LearningEnvironmentBuilder envBuilder) {
        return (SimpleStackedDatasetTrainer<I, O, AM, L>)super.withEnvironmentBuilder(envBuilder);
    }

    /** {@inheritDoc} */
    @Override public <L1> SimpleStackedDatasetTrainer<I, O, AM, L1> withConvertedLabels(IgniteFunction<L1, L> new2Old) {
        return (SimpleStackedDatasetTrainer<I, O, AM, L1>)super.withConvertedLabels(new2Old);
    }

    /**
     * Keep original features using {@link IgniteFunction#identity()} as submodelInput2AggregatingInputConverter.
     *
     * @return This object.
     */
    public SimpleStackedDatasetTrainer<I, O, AM, L> withOriginalFeaturesKept() {
        return (SimpleStackedDatasetTrainer<I, O, AM, L>)super.withOriginalFeaturesKept(IgniteFunction.identity());
    }
}
