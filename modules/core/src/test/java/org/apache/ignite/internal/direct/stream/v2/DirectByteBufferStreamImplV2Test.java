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

package org.apache.ignite.internal.direct.stream.v2;

import java.util.Arrays;
import java.util.Collections;
import org.apache.ignite.plugin.extensions.communication.IgniteMessageFactory;
import org.junit.Test;

import static org.hamcrest.Matchers.containsString;
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
        stream.col = Arrays.asList(1, "two");

        String str = stream.toString();

        assertThat(str, containsString("colTypes=[Integer, String]"));
    }

    /**
     * Tests that colTypes is output with ellipsis when not all elements fit the limit.
     */
    @Test
    public void toStringColTypesContainsEllipsisWhenMoreThan10() {
        stream.col = Collections.nCopies(11, 42);

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
        stream.objArr = Arrays.asList(1, "two").toArray();

        String str = stream.toString();

        assertThat(str, containsString("objArrTypes=[Integer, String]"));
    }

    /**
     * Tests that objArrTypes is output with ellipsis when not all elements fit the limit.
     */
    @Test
    public void toStringObjArrTypesContainsEllipsisWhenMoreThan10() {
        stream.objArr = Collections.nCopies(11, 42).toArray();

        String str = stream.toString();

        assertThat(str, containsString("objArrTypes=[..., Integer, "));
    }
}
