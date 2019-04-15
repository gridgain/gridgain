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

package org.apache.ignite.ml.inference.storage.descriptor;

import java.util.Iterator;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.ml.inference.ModelDescriptor;

/**
 * Storage that allows to load, keep and get access to model descriptors (see {@link ModelDescriptor}).
 */
public interface ModelDescriptorStorage extends Iterable<IgniteBiTuple<String, ModelDescriptor>> {
    /**
     * Saves the specified model descriptor with the specified model identifier.
     *
     * @param mdlId Model identifier.
     * @param mdl Model descriptor.
     */
    public void put(String mdlId, ModelDescriptor mdl);

    /**
     * Saves the specified model descriptor with the specified model identifier if it's not saved yet.
     *
     * @param mdlId Model identifier.
     * @param mdl Model descriptor.
     * @return {@code true} if model descriptor has been successfully saved, otherwise {@code false}.
     */
    public boolean putIfAbsent(String mdlId, ModelDescriptor mdl);

    /**
     * Returns model descriptor saved for the specified model identifier.
     *
     * @param mdlId Model identifier.
     * @return Model descriptor.
     */
    public ModelDescriptor get(String mdlId);

    /**
     * Removes model descriptor for the specified model descriptor.
     *
     * @param mdlId Model identifier.
     */
    public void remove(String mdlId);

    /**
     * Returns iterator of model descriptors stored in this model descriptor storage. The objects produces by the
     * iterator are pairs of model identifier and model descriptor.
     *
     * @return Iterator of pairs of model identifier and model descriptor.
     */
    public Iterator<IgniteBiTuple<String, ModelDescriptor>> iterator();
}