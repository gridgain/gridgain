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

package org.apache.ignite.ml.composition.combinators.sequential;

import java.util.List;
import org.apache.ignite.ml.IgniteModel;
import org.apache.ignite.ml.math.functions.IgniteFunction;

/**
 * Sequential composition of models.
 * Sequential composition is a model consisting of two models {@code mdl1 :: I -> O1, mdl2 :: O1 -> O2} with prediction
 * corresponding to application of composition {@code mdl1 `andThen` mdl2} to input.
 *
 * @param <I> Type of input of the first model.
 * @param <O1> Type of output of the first model (and input of second).
 * @param <O2> Type of output of the second model.
 */
public class ModelsSequentialComposition<I, O1, O2> implements IgniteModel<I, O2> {
    /** First model. */
    private IgniteModel<I, O1> mdl1;

    /** Second model. */
    private IgniteModel<O1, O2> mdl2;

    /**
     * Get sequential composition of submodels with same type.
     *
     * @param lst List of submodels.
     * @param output2Input Function for conversion output to input.
     * @param <I> Type of input of submodel.
     * @param <O> Type of output of submodel.
     * @return Sequential composition of submodels with same type.
     */
    public static <I, O> ModelsSequentialComposition<I, O, O> ofSame(List<? extends IgniteModel<I, O>> lst,
        IgniteFunction<O, I> output2Input) {
        assert lst.size() >= 2;

        if (lst.size() == 2)
            return new ModelsSequentialComposition<>(lst.get(0),
                lst.get(1).andBefore(output2Input));

        return new ModelsSequentialComposition<>(lst.get(0),
            ofSame(lst.subList(1, lst.size()), output2Input).andBefore(output2Input));
    }

    /**
     * Construct instance of this class from two given models.
     *
     * @param mdl1 First model.
     * @param mdl2 Second model.
     */
    public ModelsSequentialComposition(IgniteModel<I, O1> mdl1, IgniteModel<O1, O2> mdl2) {
        this.mdl1 = mdl1;
        this.mdl2 = mdl2;
    }

    /**
     * Get first model.
     *
     * @return First model.
     */
    public IgniteModel<I, O1> firstModel() {
        return mdl1;
    }

    /**
     * Get second model.
     *
     * @return Second model.
     */
    public IgniteModel<O1, O2> secondModel() {
        return mdl2;
    }

    /** {@inheritDoc} */
    @Override public O2 predict(I i1) {
        return mdl1.andThen(mdl2).predict(i1);
    }

    /** {@inheritDoc} */
    @Override public void close() {
        mdl1.close();
        mdl2.close();
    }
}
