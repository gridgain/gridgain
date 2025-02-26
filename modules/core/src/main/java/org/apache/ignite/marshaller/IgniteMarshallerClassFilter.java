/*
 * Copyright 2025 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.marshaller;

import java.util.Objects;
import org.apache.ignite.internal.ClassSet;
import org.apache.ignite.lang.IgnitePredicate;

/**
 * The Marshall class filter is used to avoid deserialization of some classes,
 * which can contain code injection or are exposed to vulnerability.
 */
public class IgniteMarshallerClassFilter implements IgnitePredicate<String> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final transient ClassSet whiteList;

    /** */
    private final transient ClassSet blackList;

    /**
     * @param whiteList Class white list.
     * @param blackList Class black list.
     */
    public IgniteMarshallerClassFilter(ClassSet whiteList, ClassSet blackList) {
        this.whiteList = whiteList;
        this.blackList = blackList;
    }

    /** {@inheritDoc} */
    @Override public boolean apply(String s) {
        // Allows all arrays.
        if ((blackList != null || whiteList != null) && s.charAt(0) == '[')
            return true;

        return (blackList == null || !blackList.contains(s)) && (whiteList == null || whiteList.contains(s));
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (!(o instanceof IgniteMarshallerClassFilter))
            return false;

        IgniteMarshallerClassFilter other = (IgniteMarshallerClassFilter)o;

        return Objects.equals(whiteList, other.whiteList) && Objects.equals(blackList, other.blackList);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(whiteList, blackList);
    }
}
