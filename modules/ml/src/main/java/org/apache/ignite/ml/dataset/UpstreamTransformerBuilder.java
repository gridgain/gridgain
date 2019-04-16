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

package org.apache.ignite.ml.dataset;

import java.io.Serializable;
import org.apache.ignite.ml.environment.LearningEnvironment;

/**
 * Builder of {@link UpstreamTransformer}.
 */
@FunctionalInterface
public interface UpstreamTransformerBuilder extends Serializable {
    /**
     * Create {@link UpstreamTransformer} based on learning environment.
     *
     * @param env Learning environment.
     * @return Upstream transformer.
     */
    public UpstreamTransformer build(LearningEnvironment env);

    /**
     * Combunes two builders (this and other respectfully)
     * <pre>
     * env -> transformer1
     * env -> transformer2
     * </pre>
     * into
     * <pre>
     * env -> transformer2 . transformer1
     * </pre>
     *
     * @param other Builder to combine with.
     * @return Compositional builder.
     */
    public default UpstreamTransformerBuilder andThen(UpstreamTransformerBuilder other) {
        UpstreamTransformerBuilder self = this;
        return env -> {
            UpstreamTransformer transformer1 = self.build(env);
            UpstreamTransformer transformer2 = other.build(env);

            return upstream -> transformer2.transform(transformer1.transform(upstream));
        };
    }

    /**
     * Returns identity upstream transformer.
     *
     * @param <K> Type of keys in upstream.
     * @param <V> Type of values in upstream.
     * @return Identity upstream transformer.
     */
    public static <K, V> UpstreamTransformerBuilder identity() {
        return env -> upstream -> upstream;
    }
}
