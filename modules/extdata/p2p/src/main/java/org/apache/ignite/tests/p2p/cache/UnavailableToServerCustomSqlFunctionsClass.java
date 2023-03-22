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

package org.apache.ignite.tests.p2p.cache;

/**
 * Class represents a piece of custom configuration that can be passed to
 * {@link org.apache.ignite.configuration.CacheConfiguration#setSqlFunctionClasses(Class[])} property.
 *
 * This class is used to represent a situation when it is available on the classpath of a client node
 * and unavailable on server node.
 *
 * In that case request to start a cache or create a cache template should be rejected on the client side
 * without compromizing stability of the rest of the cluster.
 */
public class UnavailableToServerCustomSqlFunctionsClass {
}
