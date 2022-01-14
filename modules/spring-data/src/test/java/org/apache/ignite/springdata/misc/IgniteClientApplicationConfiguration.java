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

package org.apache.ignite.springdata.misc;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.springdata.repository.config.EnableIgniteRepositories;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static org.apache.ignite.springdata.compoundkey.CompoundKeyApplicationConfiguration.CLI_CONN_PORT;

/** Spring Application configuration for repository testing in case thin client is used for accessing the cluster. */
@Configuration
@EnableIgniteRepositories({"org.apache.ignite.springdata.compoundkey", "org.apache.ignite.springdata.misc"})
public class IgniteClientApplicationConfiguration {
    /** @return {@link Ignite} node instance. */
    @Bean
    public IgniteEx igniteServerNode() {
        IgniteConfiguration cfg = new IgniteConfiguration()
            .setClientConnectorConfiguration(new ClientConnectorConfiguration().setPort(CLI_CONN_PORT))
            .setCacheConfiguration(new CacheConfiguration<Integer, Person>("PersonCache")
                .setIndexedTypes(Integer.class, Person.class));

        return (IgniteEx)Ignition.start(cfg);
    }

    /** Ignite client instance bean with default name. */
    @Bean
    public IgniteClient igniteInstance() {
        return Ignition.startClient(new ClientConfiguration().setAddresses("127.0.0.1:" + CLI_CONN_PORT));
    }
}
