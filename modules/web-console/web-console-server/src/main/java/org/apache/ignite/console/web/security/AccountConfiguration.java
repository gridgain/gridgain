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

package org.apache.ignite.console.web.security;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.core.userdetails.UserDetailsChecker;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;

/**
 * Account configuration.
 **/
@Configuration
public class AccountConfiguration {
    /** */
    @Value("${app.activation.enabled:false}")
    private boolean activationEnabled;

    /**
     * @return Service for encoding user passwords.
     */
    @Bean
    public PasswordEncoder encoder() {
        // Pbkdf2PasswordEncoder is compatible with passport.js, but BCryptPasswordEncoder is recommended by Spring.
        // We can return to Pbkdf2PasswordEncoder if we decided to import old users.
        //  Pbkdf2PasswordEncoder encoder = new Pbkdf2PasswordEncoder("", 25000, HASH_WIDTH); // HASH_WIDTH = 512
        //
        //  encoder.setAlgorithm(PBKDF2WithHmacSHA256);
        //  encoder.setEncodeHashAsBase64(true);

        return new BCryptPasswordEncoder();
    }

    /**
     * @return Checker for status of the loaded <tt>UserDetails</tt> object.
     */
    @Bean
    public UserDetailsChecker checker() {
        return new AccountStatusChecker(activationEnabled);
    }

}
