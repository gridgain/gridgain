/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.spi.systemview.view;

import java.util.Iterator;
import java.util.Map;

/**
 * System view with filtering capabilities.
 *
 * @param <R> Type of the row.
 */
public interface FiltrableSystemView<R> extends SystemView<R> {
    /**
     * @param filter Filter for a view ({@code null} or empty filter means no filtering).
     * @return Iterator for filtered system view content.
     */
    public Iterator<R> iterator(Map<String, Object> filter);
}
