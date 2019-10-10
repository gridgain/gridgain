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

package org.apache.ignite.console.messages;

import org.junit.Assert;
import org.junit.Test;

import static org.apache.ignite.console.messages.WebConsoleMessageSource.message;

/**
 * Web console message source test.
 */
public class WebConsoleMessageSourceTest {
    /**
     * Should replace message code without args.
     */
    @Test
    public void shouldReplaceMessageCodeWithoudArgs() {
        Assert.assertEquals("Simple message",  message("test.simple-message"));
    }

    /**
     * Should replace message code with args.
     */
    @Test
    public void shouldReplaceMessageCodeWithArgs() {
        Assert.assertEquals("Message with args: 1, 2, 3",  message("test.args-message", 1, 2, 3));
    }

    /**
     * Should replace message code with string format args.
     */
    @Test
    public void shouldReplaceMessageCodeWithStringFormatArgs() {
        Assert.assertEquals("Message with args: 1.25, 2.1, 3",  message("test.format-args-message", 1.25d, 2.1f, 3));
    }
}
