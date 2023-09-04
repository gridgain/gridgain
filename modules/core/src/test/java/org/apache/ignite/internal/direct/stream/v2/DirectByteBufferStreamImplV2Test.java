/*
* Copyright 2023 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.direct.stream.v2;

import java.util.Arrays;
import java.util.Collections;
import org.apache.ignite.plugin.extensions.communication.IgniteMessageFactory;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static java.util.Collections.emptyList;
import static org.apache.ignite.internal.direct.stream.v2.DirectByteBufferStreamImplV2.lastTypesWithLimit;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Tests for {@link DirectByteBufferStreamImplV2}.
 */
public class DirectByteBufferStreamImplV2Test {
    private final DirectByteBufferStreamImplV2 stream = new DirectByteBufferStreamImplV2(
        mock(IgniteMessageFactory.class));

    /**
     * Tests that colTypes is output as null when col is null.
     */
    @Test
    public void toStringColTypesIsNullWhenColIsNull() {
        String str = stream.toString();

        assertThat(str, containsString("colTypes=null"));
    }

    /**
     * Tests that colTypes is output with all element types when they fit the limit.
     */
    @Test
    public void toStringColTypesContainsAllTypesWhenLessThan10() {
        GridTestUtils.setFieldValue(stream, "col", Arrays.asList(1, "two"));

        String str = stream.toString();

        assertThat(str, containsString("colTypes=[Integer, String]"));
    }

    /**
     * Tests that colTypes is output with ellipsis when not all elements fit the limit.
     */
    @Test
    public void toStringColTypesContainsEllipsisWhenMoreThan10() {
        GridTestUtils.setFieldValue(stream, "col", Collections.nCopies(11, 42));

        String str = stream.toString();

        assertThat(str, containsString("colTypes=[..., Integer, "));
    }

    /**
     * Tests that objArrTypes is output as null when objArr is null.
     */
    @Test
    public void toStringObjArrTypesIsNullWhenColIsNull() {
        String str = stream.toString();

        assertThat(str, containsString("objArrTypes=null"));
    }

    /**
     * Tests that objArrTypes is output with all element types when they fit the limit.
     */
    @Test
    public void toStringObjArrTypesContainsAllTypesWhenLessThan10() {
        GridTestUtils.setFieldValue(stream, "objArr", Arrays.asList(1, "two").toArray());

        String str = stream.toString();

        assertThat(str, containsString("objArrTypes=[Integer, String]"));
    }

    /**
     * Tests that objArrTypes is output with ellipsis when not all elements fit the limit.
     */
    @Test
    public void toStringObjArrTypesContainsEllipsisWhenMoreThan10() {
        GridTestUtils.setFieldValue(stream, "objArr", Collections.nCopies(11, 42).toArray());

        String str = stream.toString();

        assertThat(str, containsString("objArrTypes=[..., Integer, "));
    }

    /**
     * Tests that empty list is represented as [].
     */
    @Test
    public void lastTypesWithLimitGivesEmptyBracketsForEmptyList() {
        assertThat(lastTypesWithLimit(emptyList(), 1), is("[]"));
    }

    /**
     * Tests that a list fitting the limit is represented with types of all its elements.
     */
    @Test
    public void lastTypesWithLimitGivesTypesOfWholeListIfSizeFitsLimit() {
        assertThat(lastTypesWithLimit(Arrays.asList(1, "two"), 2), is("[Integer, String]"));
    }

    /**
     * Tests that a list not fitting the limit is represented as an ellipsis and the types of the elements of its tail.
     */
    @Test
    public void lastTypesWithLimitGivesEllipsisIfListIsLongerThanLimit() {
        assertThat(lastTypesWithLimit(Arrays.asList(1, "two", 3), 2), is("[..., String, Integer]"));
    }

    /**
     * Tests that for {@code null} elements, 'null' is output as their representation instead of type names.
     */
    @Test
    public void lastTypesWithLimitGivesNullsForNulls() {
        assertThat(lastTypesWithLimit(Arrays.asList(1, null), 10), is("[Integer, null]"));
    }
}
