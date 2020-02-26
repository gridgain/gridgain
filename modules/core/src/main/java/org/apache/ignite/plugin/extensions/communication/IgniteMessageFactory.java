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

package org.apache.ignite.plugin.extensions.communication;

import java.util.function.Supplier;
import org.apache.ignite.IgniteException;

/**
 * Message factory for all communication messages registered using {@link #register(short, Supplier)} method call.
 */
public interface IgniteMessageFactory extends MessageFactory {
    /**
     * Register message factory with given direct type. All messages must be registered during construction
     * of class which implements this interface. Any invocation of this method after initialization is done must
     * throw {@link IllegalStateException} exception.
     *
     * @param directType Direct type.
     * @param supplier Message factory.
     * @throws IgniteException In case of attempt to register message with direct type which is already registered.
     * @throws IllegalStateException On any invocation of this method when class which implements this interface
     * is alredy constructed.
     */
    public void register(short directType, Supplier<Message> supplier) throws IgniteException;
}
