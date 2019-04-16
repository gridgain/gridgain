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

import java.net.URL;
import java.util.HashMap;
import org.apache.ignite.ml.inference.builder.SingleModelBuilder;
import org.apache.ignite.ml.inference.builder.SyncModelBuilder;
import org.apache.ignite.ml.inference.reader.FileSystemModelReader;
import org.apache.ignite.ml.inference.reader.ModelReader;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link MLeapModelParser}.
 */
public class MLeapModelParserTest {
    /** Test model resource name. */
    private static final String TEST_MODEL_RESOURCE = "datasets/scikit-airbnb.rf.zip";

    /** Parser. */
    private final MLeapModelParser parser = new MLeapModelParser();

    /** Model builder. */
    private final SyncModelBuilder mdlBuilder = new SingleModelBuilder();

    /** */
    @Test
    public void testParseAndPredict() {
        URL url = MLeapModelParserTest.class.getClassLoader().getResource(TEST_MODEL_RESOURCE);
        if (url == null)
            throw new IllegalStateException("File not found [resource_name=" + TEST_MODEL_RESOURCE + "]");

        ModelReader reader = new FileSystemModelReader(url.getPath());

        try (MLeapModel mdl = mdlBuilder.build(reader, parser)) {
            HashMap<String, Double> input = new HashMap<>();
            input.put("imp_bathrooms", 1.0);
            input.put("imp_bedrooms", 1.0);
            input.put("imp_security_deposit", 1.0);
            input.put("imp_cleaning_fee", 1.0);
            input.put("imp_extra_people", 1.0);
            input.put("imp_number_of_reviews", 1.0);
            input.put("imp_square_feet", 1.0);
            input.put("imp_review_scores_rating", 1.0);

            double prediction = mdl.predict(VectorUtils.of(input));

            assertEquals(95.3919, prediction, 1e-5);
        }
    }
}
