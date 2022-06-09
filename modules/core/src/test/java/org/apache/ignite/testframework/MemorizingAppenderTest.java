/*
 * Copyright 2022 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.testframework;

import java.util.List;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

/**
 * Tests for {@link MemorizingAppender}.
 */
public class MemorizingAppenderTest {
    /**
     * The instance under test.
     */
    private final MemorizingAppender appender = new MemorizingAppender();

    /***/
    @Before
    public void installAppender() {
        appender.installSelfOn(MemorizingAppenderTest.class);
    }

    /***/
    @After
    public void removeAppender() {
        appender.removeSelfFrom(MemorizingAppenderTest.class);
    }

    /**
     * Tests that MemorizingAppender memorizes logging events.
     */
    @Test
    public void memorizesLoggingEvents() {
        Logger.getLogger(MemorizingAppenderTest.class).info("Hello!");

        List<LoggingEvent> events = appender.events();

        assertThat(events, hasSize(1));

        LoggingEvent event = events.get(0);

        assertThat(event.getLevel(), is(Level.INFO));
        assertThat(event.getRenderedMessage(), is("Hello!"));
    }
}
