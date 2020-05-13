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

package org.apache.ignite.internal.processors.security;

import java.util.HashMap;
import java.util.Map;
import javax.cache.configuration.Factory;

import static org.apache.ignite.internal.processors.security.impl.TestAdditionalSecurityPluginProvider.
    ADDITIONAL_SECURITY_CLIENT_VERSION;
import static org.apache.ignite.internal.processors.security.impl.TestAdditionalSecurityPluginProvider.
    ADDITIONAL_SECURITY_CLIENT_VERSION_ATTR;

/**
 * Creates user attributes for thin clients.
 */
public class UserAttributesFactory implements Factory<Map<String, String>> {
    /** {@inheritDoc} */
    @Override public Map<String, String> create() {
        HashMap<String, String> map = new HashMap<>();

        map.put(ADDITIONAL_SECURITY_CLIENT_VERSION_ATTR, ADDITIONAL_SECURITY_CLIENT_VERSION);

        return map;
    }
}
