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

package org.gridgain.dto.span;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * DTO for span list.
 */
public class SpanList implements Serializable {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Span list. */
    private List<Span> list = new ArrayList<>();

    /**
     * Default constructor.
     */
    public SpanList() {
    }

    /**
     * @param list Span list.
     */
    public SpanList(List<Span> list) {
        this.list = list;
    }

    /**
     * Return list of spans.
     */
    public List<Span> getList() {
        return list;
    }

    /**
     * @param list Span list.
     */
    public SpanList setList(List<Span> list) {
        this.list = list;
        return this;
    }
}
