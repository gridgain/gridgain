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

import java.util.function.Predicate;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.events.DiscoveryCustomEvent;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Factory;
import org.hamcrest.Matcher;

/**
 * Matcher to check if given object either {@link TestDiscoveryCustomMessage} or {@link DiscoveryCustomEvent} with
 * {@link TestDiscoveryCustomMessage} which contains expected id.
 */
public class IsDiscoveryEventMessage<T> extends BaseMatcher<DiscoveryEvent> {
    /** Expected class of message. */
    private final Class<T> msgType;

    /** The condition which message should be corresponded to. */
    private final Predicate<T> pred;

    /**
     * @param type Expected class of message.
     * @param pred The condition which message should be corresponded to.
     */
    public IsDiscoveryEventMessage(Class<T> type, Predicate<T> pred) {
        msgType = type;
        this.pred = pred;
    }

    /** {@inheritDoc} */
    @Override public boolean matches(Object evt) {
        if (evt instanceof DiscoveryCustomEvent) {
            DiscoveryCustomMessage msg = ((DiscoveryCustomEvent)evt).customMessage();

            return msgType.isAssignableFrom(msg.getClass()) && pred.test(msgType.cast(msg));
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public void describeTo(Description desc) {
        desc.appendValue("Class(" + msgType + ") with predicate(" + pred + ")");
    }

    /**
     * Matcher to check if given object either {@link TestDiscoveryCustomMessage} or {@link DiscoveryCustomEvent} with
     * {@link TestDiscoveryCustomMessage} which contains expected id.
     *
     * @param type Expected class of message.
     * @param pred The condition which message should be corresponded to.
     * @param <T> Type of matcher.
     * @return Matcher.
     */
    @Factory
    public static <T> Matcher<DiscoveryEvent> isDiscoveryEventMessage(Class<T> type, Predicate<T> pred) {
        return new IsDiscoveryEventMessage<>(type, pred);
    }

    /**
     * Matcher to check if given object {@link DiscoveryCustomEvent} with {@link TestDiscoveryCustomMessage} which
     * contains expected id.
     *
     * @param expVal Expected value of {@link TestDiscoveryCustomMessage}.
     * @return Matcher.
     */
    @Factory
    public static Matcher<DiscoveryEvent> isTestEventMessage(String expVal) {
        return new IsDiscoveryEventMessage<>(TestDiscoveryCustomMessage.class, (msg) -> msg.value().equals(expVal));
    }
}
