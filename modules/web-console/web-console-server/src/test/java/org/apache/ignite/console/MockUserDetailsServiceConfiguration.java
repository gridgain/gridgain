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

package org.apache.ignite.console;

import java.util.Collections;
import org.apache.ignite.console.dto.Account;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.provisioning.InMemoryUserDetailsManager;

/**
 * Test configuration with user service mock.
 */
@TestConfiguration
public class MockUserDetailsServiceConfiguration {
    /** Test account email. */
    public static final String TEST_EMAIL = "test@test.com";

    /**
     * Mocked implementation of {@code UserDetailsManager}.
     */
    @Bean
    @Primary
    public UserDetailsService userDetailsService(){
        Account acc = new Account(
            TEST_EMAIL,
            "password",
            null,
            null,
            null,
            null,
            null
        );

        return new InMemoryUserDetailsManager(Collections.singletonList(acc));
    }
}
