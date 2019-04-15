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

package org.apache.ignite.ml.mleap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import ml.combust.mleap.core.types.ScalarType;
import ml.combust.mleap.core.types.StructField;
import ml.combust.mleap.core.types.StructType;
import ml.combust.mleap.runtime.frame.DefaultLeapFrame;
import ml.combust.mleap.runtime.frame.Row;
import ml.combust.mleap.runtime.frame.Transformer;
import ml.combust.mleap.runtime.javadsl.LeapFrameBuilder;
import org.apache.ignite.ml.inference.Model;
import org.apache.ignite.ml.math.primitives.vector.NamedVector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import scala.collection.immutable.Set;
import scala.collection.immutable.Stream;
import scala.util.Try;

/**
 * MLeap model imported and wrapped to be compatible with Apache Ignite infrastructure.
 */
public class MLeapModel implements Model<NamedVector, Double> {
    /** MLeap model (transformer in terms of MLeap). */
    private final Transformer transformer;

    /** List of field names. */
    private final List<String> schema;

    /** Output field name. */
    private final String outputFieldName;

    /**
     * Constructs a new instance of MLeap model.
     *
     * @param transformer MLpeap model (transformer in terms of MLeap).
     * @param schema List of field names.
     * @param outputFieldName Output field name.
     */
    public MLeapModel(Transformer transformer, List<String> schema, String outputFieldName) {
        this.transformer = transformer;
        this.schema = new ArrayList<>(schema);
        this.outputFieldName = outputFieldName;
    }

    /** {@inheritDoc} */
    @Override public Double predict(NamedVector input) {
        LeapFrameBuilder builder = new LeapFrameBuilder();
        List<StructField> structFields = new ArrayList<>();

        List<Object> values = new ArrayList<>();
        for (String fieldName : input.getKeys()) {
            structFields.add(new StructField(fieldName, ScalarType.Double()));
            values.add(input.get(fieldName));
        }

        StructType schema = builder.createSchema(structFields);

        List<Row> rows = new ArrayList<>();
        rows.add(builder.createRowFromIterable(values));

        DefaultLeapFrame inputFrame = builder.createFrame(schema, rows);

        return predict(inputFrame);
    }

    /**
     * Makes a prediction using default column order specified in {@link #schema}.
     *
     * @param input Input arguments.
     * @return Prediction result.
     */
    public double predict(Double[] input) {
        if (input.length != schema.size())
            throw new IllegalArgumentException("Input size is not equal to schema size");

        Map<String, Double> vec = IntStream.range(0, input.length)
            .boxed()
            .collect(Collectors.toMap(schema::get, i -> input[i]));

        return predict(VectorUtils.of(vec));
    }

    /**
     * Makes a prediction using MLeap API.
     *
     * @param inputFrame Input MLeap frame.
     * @return Prediction result.
     */
    public double predict(DefaultLeapFrame inputFrame) {
        DefaultLeapFrame outputFrame = transformer.transform(inputFrame).get();

        Try<DefaultLeapFrame> resFrame = outputFrame.select(new Set.Set1<>(outputFieldName).toSeq());
        DefaultLeapFrame frame = resFrame.get();

        Stream<?> stream = (Stream<?>)frame.productElement(1);
        Row row = (Row)stream.head();

        return (Double)row.get(0);
    }

    /** {@inheritDoc} */
    @Override public void close() {
        transformer.close();
    }
}
