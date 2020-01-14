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
import org.apache.ignite.internal.events.DiscoveryCustomEvent;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryCustomEventMessage;
import org.hamcrest.Factory;
import org.hamcrest.Matcher;

import static org.apache.ignite.testframework.GridTestUtils.unmarshallDiscovery;

/**
 * Matcher to check if given object either {@link TestDiscoveryCustomMessage} or {@link DiscoveryCustomEvent} with
 * {@link TestDiscoveryCustomMessage} which contains expected id.
 */
public class IsDiscoveryCustomMessage<T> extends IsDiscoveryMessage<T> {
    /**
     * @param type Expected class of message.
     * @param pred The condition which message should be corresponded to.
     */
    private IsDiscoveryCustomMessage(Class<T> type, Predicate<T> pred) {
        super(type, pred);
    }

    /** {@inheritDoc} */
    @Override public boolean matches(Object msg) {
        return msg instanceof TcpDiscoveryCustomEventMessage
            && super.matches(unmarshallDiscovery((TcpDiscoveryCustomEventMessage)msg));
    }

    /**
     * Matcher to check if given object either {@link TestDiscoveryCustomMessage} or {@link DiscoveryCustomEvent} with
     * {@link TestDiscoveryCustomMessage} which contains expected id.
     *
     * @param expVal Expected value of {@link TestDiscoveryCustomMessage}.
     * @return Matcher.
     */
    @Factory
    public static Matcher<TcpDiscoveryAbstractMessage> isTestMessage(String expVal) {
        return new IsDiscoveryCustomMessage<>(TestDiscoveryCustomMessage.class, (msg) -> msg.value().equals(expVal));
    }
}
