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

package org.apache.ignite.console.utils;

import org.junit.Test;

import static org.apache.ignite.console.utils.Utils.secured;
import static org.junit.Assert.assertEquals;

/**
 * Test for Web Console utils.
 */
public class UtilsTest {
    /**
     * GG-26394 Test case 5: Should correctly generate secured string.
     */
    @Test
    public void shouldGenerateSecureString() {
        assertEquals("", secured((String)null));
        assertEquals("", secured(""));
        assertEquals("1", secured("1"));
        assertEquals("*2", secured("12"));
        assertEquals("**3", secured("123"));
        assertEquals("***4", secured("1234"));
        assertEquals("*2345", secured("12345"));
        assertEquals("**3456", secured("123456"));
    }
}
