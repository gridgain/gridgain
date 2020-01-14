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
package org.apache.ignite.testframework.discovery;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.hamcrest.Description;
import org.hamcrest.Factory;
import org.hamcrest.Matcher;
import org.hamcrest.collection.IsIterableContainingInOrder;

/**
 * Matcher which match the given iterator to the expected patterns.
 * Each conformity should happen only one time in a given order.
 *
 * In easy words, this class filters all objects from the given list which corresponded to configured matchers in any order and then the resulted list matches to configured matchers by {@link IsIterableContainingInOrder} laws.
 *
 * Example. For given list [1, 2, 1, 2, 3], with matchers:
 * * [1, 2, 3] - false - '1' matched two times.
 * * [1, 3] - false - '1' matched two times.
 * * [2, 1, 3, 2] - false - order is wrong
 * * [1, 1, 3] - true
 */
public class HasOneTimeEachInOrder<T> extends IsIterableContainingInOrder<T> {
    /** Expected matchers. */
    private final List<Matcher<? super T>> matchers;

    /**
     * @param matchers Expected matchers.
     */
    private HasOneTimeEachInOrder(List<Matcher<? super T>> matchers) {
        super(matchers);

        this.matchers = matchers;
    }

    /** {@inheritDoc} */
    @Override protected boolean matchesSafely(Iterable<? extends T> iterable, Description mismatchDesc) {
        Iterator<T> objIt = ((Iterable<T>)iterable).iterator();
        List<T> filtered = new ArrayList<>();

        while (objIt.hasNext()) {
            T obj = objIt.next();

            for (Matcher<? super T> m : matchers) {
                if (m.matches(obj) && filtered.add(obj))
                    break;
            }
        }

        return super.matchesSafely(filtered, mismatchDesc);
    }

    /**
     * Matcher which match the given iterator to the expected patterns. Each conformity should happen only one time in a
     * given order. List matching started only when the first match would be found.
     *
     * @param matchers Expected match pattern.
     * @param <T> Type of checked object.
     * @return Matcher.
     */
    @SafeVarargs
    @Factory
    public static <T> Matcher<Iterable<? extends T>> hasOneTimeEachInOrder(Matcher<? super T>... matchers) {
        return new HasOneTimeEachInOrder<>(Arrays.asList(matchers));
    }
}
