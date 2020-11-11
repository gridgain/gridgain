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

import org.apache.ignite.events.DiscoveryEvent;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Factory;
import org.hamcrest.Matcher;

/**
 * Matcher to check if given object is {@link DiscoveryEvent} which expected type.
 */
public class IsDiscoveryEvent<T> extends BaseMatcher<T> {
    /** Expected value of {@link org.apache.ignite.events.EventType}. */
    private final int evtType;

    /**
     * @param evtType Expected value of {@link org.apache.ignite.events.EventType}.
     */
    private IsDiscoveryEvent(int evtType) {
        this.evtType = evtType;
    }

    /** {@inheritDoc} */
    @Override public boolean matches(Object msg) {
        return msg instanceof DiscoveryEvent && ((DiscoveryEvent)msg).type() == evtType;
    }

    /** {@inheritDoc} */
    @Override public void describeTo(Description desc) {
        desc.appendValue("DiscoveryEvent(type=" + evtType + ")");
    }

    /**
     * Expected value of {@link org.apache.ignite.events.EventType}.
     *
     * @param evtType Event type.
     * @param <T> Type of matcher.
     * @return Matcher.
     */
    @Factory
    public static <T> Matcher<T> isDiscoveryEvent(int evtType) {
        return new IsDiscoveryEvent<>(evtType);
    }
}
