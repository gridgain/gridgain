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

package org.apache.ignite.console.config;

import java.nio.charset.StandardCharsets;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;

/**
 * Account configuration.
 **/
@Configuration
@ConfigurationProperties("account")
public class AccountConfiguration {
    /** Flag if sign up disabled and new accounts can be created only by administrator. */
    private boolean disableSignup;

    /**
     * @return Service for encoding user passwords.
     */
    @Bean
    public PasswordEncoder passwordEncoder() {
        return new CustomPasswordEncoder();
    }

    /**
     * @return {@code true} if sign up disabled and new accounts can be created only by administrator.
     */
    public boolean isDisableSignup() {
        return disableSignup;
    }

    /**
     * @param disableSignup {@code true} if signup disabled and new accounts can be created only by administrator.
     * @return {@code this} for chaining.
     */
    public AccountConfiguration setDisableSignup(boolean disableSignup) {
        this.disableSignup = disableSignup;

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(AccountConfiguration.class, this);
    }

    /**
     * Custom password encoder that encode new passwords with BCrypt and can verify old passwords imported from MongoDB.
     */
    private static class CustomPasswordEncoder implements PasswordEncoder {
        /** */
        private static final Logger log = LoggerFactory.getLogger(CustomPasswordEncoder.class);

        /** Number of hash iterations, taken from passportjs */
        private static final int ITERATIONS = 25000;

        /** Key length, taken from passportjs */
        private static final int KEY_LEN = 512 * 8;

        /** */
        private final PasswordEncoder bcrypt = new BCryptPasswordEncoder();

        /** {@inheritDoc} */
        @Override public String encode(CharSequence rawPassword) {
            return bcrypt.encode(rawPassword);
        }

        /** {@inheritDoc} */
        @Override public boolean matches(CharSequence rawPassword, String encodedPassword) {
            if (encodedPassword.startsWith("PBKDF2:")) {
                String[] parts = encodedPassword.split(":");

                if (parts.length != 3) {
                    log.error("Invalid hash in PBKDF2 format: " + encodedPassword);

                    return false;
                }

                String salt = parts[1];
                String hash = parts[2];

                PBEKeySpec spec = new PBEKeySpec(
                    rawPassword.toString().toCharArray(),
                    salt.getBytes(StandardCharsets.UTF_8),
                    ITERATIONS,
                    KEY_LEN);

                try {
                    SecretKeyFactory skf = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256");

                    String encoded = U.byteArray2HexString(skf.generateSecret(spec).getEncoded());

                    return hash.equalsIgnoreCase(encoded);
                }
                catch (Throwable e) {
                    log.error("Failed to check password", e);
                }
            }

            return bcrypt.matches(rawPassword, encodedPassword);
        }
    }
}
