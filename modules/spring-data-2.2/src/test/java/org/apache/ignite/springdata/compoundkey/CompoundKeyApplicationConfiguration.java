/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.springdata.compoundkey;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.springdata22.repository.config.EnableIgniteRepositories;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Spring application configuration
 * */
@Configuration
@EnableIgniteRepositories
public class CompoundKeyApplicationConfiguration {
    /** */
    public static final int CLI_CONN_PORT = 10810;

    /**
     * Ignite instance bean
     * */
    @Bean
    public Ignite igniteInstance() {
        return Ignition.start(new IgniteConfiguration()
            .setClientConnectorConfiguration(new ClientConnectorConfiguration().setPort(CLI_CONN_PORT)));
    }
}
