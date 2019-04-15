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

package org.apache.ignite.console.configuration;

/**
 * Service class with information about property in configuration state.
 */
public class FieldProcessingInfo {
    /** Property name. */
    private final String name;

    /** Count of occurrence in configuration class. */
    private int occurrence;

    /** Deprecated sign of property getter or setter method. */
    private boolean deprecated;

    /**
     * Constructor.
     *
     * @param name Property name.
     * @param occurrence Count of occurrence in configuration class.
     * @param deprecated Deprecated sign of property getter or setter method.
     */
    public FieldProcessingInfo(String name, int occurrence, boolean deprecated) {
        this.name = name;
        this.occurrence = occurrence;
        this.deprecated = deprecated;
    }

    /**
     * @return Property name.
     */
    public String getName() {
        return name;
    }

    /**
     * @return Count of occurrence in configuration class.
     */
    public int getOccurrence() {
        return occurrence;
    }

    /**
     * @return Deprecated sign of property getter or setter method.
     */
    public boolean isDeprecated() {
        return deprecated;
    }

    /**
     * Increase occurrence count.
     *
     * @return {@code this} for chaining.
     */
    public FieldProcessingInfo next() {
        occurrence += 1;

        return this;
    }

    /**
     * Set deprecated state.
     *
     * @param state Deprecated state of checked method.
     * @return {@code this} for chaining.
     */
    public FieldProcessingInfo deprecated(boolean state) {
        deprecated = deprecated || state;

        return this;
    }
}
