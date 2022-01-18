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

package org.apache.ignite.springdata;

import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.springdata.misc.IgniteClientApplicationConfiguration;
import org.apache.ignite.springdata.misc.PersonRepository;
import org.apache.ignite.springdata.misc.PersonRepositoryWithCompoundKey;
import org.apache.ignite.springdata.repository.config.EnableIgniteRepositories;
import org.junit.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static org.apache.ignite.springdata.compoundkey.CompoundKeyApplicationConfiguration.CLI_CONN_PORT;

/** Tests Spring Data CRUD operation when thin client is used for accessing the Ignite cluster. */
public class IgniteClientSpringDataCrudSelfTest extends IgniteSpringDataCrudSelfTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() {
        ctx = new AnnotationConfigApplicationContext();

        ctx.register(IgniteClientApplicationConfiguration.class);
        ctx.refresh();

        repo = ctx.getBean(PersonRepository.class);
        repoWithCompoundKey = ctx.getBean(PersonRepositoryWithCompoundKey.class);
        ignite = ctx.getBean(IgniteEx.class);
    }

    /**
     * Tests repository configuration in case {@link ClientConfiguration} is used to provide access to Ignite cluster.
     */
    @Test
    public void testRepositoryWithClientConfiguration() {
        ctx = new AnnotationConfigApplicationContext();

        ctx.register(TestApplicationConfiguration.class);
        ctx.refresh();

        assertTrue(ctx.getBean(PersonRepository.class).count() > 0);
    }

    /** */
    @Configuration
    @EnableIgniteRepositories("org.apache.ignite.springdata.misc")
    static class TestApplicationConfiguration {
        /** Ignite client configuration bean. */
        @Bean
        public ClientConfiguration igniteCfg() {
           return new ClientConfiguration().setAddresses("127.0.0.1:" + CLI_CONN_PORT);
        }
    }
}
