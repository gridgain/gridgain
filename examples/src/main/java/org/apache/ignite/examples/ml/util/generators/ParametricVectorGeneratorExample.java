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

package org.apache.ignite.examples.ml.util.generators;

import java.io.IOException;
import org.apache.ignite.ml.math.Tracer;
import org.apache.ignite.ml.util.generators.DataStreamGenerator;
import org.apache.ignite.ml.util.generators.primitives.scalar.UniformRandomProducer;
import org.apache.ignite.ml.util.generators.primitives.vector.ParametricVectorGenerator;

/**
 * Examples of using {@link ParametricVectorGenerator} for generating two dimensional data.
 * {@link ParametricVectorGenerator} allows to create surfaces in N-dinensional spaces where each
 * dimension depends on one parameter 't'. In such generator just one random producer is used, it
 * defines a set of values for parameter 't'.
 */
public class ParametricVectorGeneratorExample {
    /**
     * Run example.
     *
     * @param args Args.
     */
    public static void main(String... args) throws IOException {
        // Example of Archimedean spiral.
        DataStreamGenerator spiral = new ParametricVectorGenerator(
            new UniformRandomProducer(-50, 50), //'t' will be in [-50, 50] range
            t -> Math.cos(Math.abs(t)) * Math.abs(t),
            t -> Math.sin(Math.abs(t)) * Math.abs(t)
        ).asDataStream();

        Tracer.showClassificationDatasetHtml("Spiral", spiral, 20000, 0, 1, false);

        // Example of heart shape.
        DataStreamGenerator heart = new ParametricVectorGenerator(new UniformRandomProducer(-50, 50),
            t -> 16 * Math.pow(Math.sin(t), 3),
            t -> 13 * Math.cos(t) - 5 * Math.cos(2 * t) - 2 * Math.cos(3 * t) - Math.cos(4 * t)
        ).asDataStream();

        Tracer.showClassificationDatasetHtml("Heart", heart, 2000, 0, 1, false);

        // Example of butterfly-like shape.
        DataStreamGenerator butterfly = new ParametricVectorGenerator(
            new UniformRandomProducer(-100, 100), //'t' will be in [-100, 100] range
            t -> 10 * Math.sin(t) * (Math.exp(Math.cos(t)) - 2 * Math.cos(4 * t) - Math.pow(Math.sin(t / 12), 5)),
            t -> 10 * Math.cos(t) * (Math.exp(Math.cos(t)) - 2 * Math.cos(4 * t) - Math.pow(Math.sin(t / 12), 5))
        ).asDataStream();

        Tracer.showClassificationDatasetHtml("Butterfly", butterfly, 2000, 0, 1, false);
    }
}
