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

import org.apache.ignite.internal.util.typedef.internal.S;
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
        // Pbkdf2PasswordEncoder is compatible with passport.js, but BCryptPasswordEncoder is recommended by Spring.
        // We can return to Pbkdf2PasswordEncoder if we decided to import old users.
        //  Pbkdf2PasswordEncoder encoder = new Pbkdf2PasswordEncoder("", 25000, HASH_WIDTH); // HASH_WIDTH = 512
        //
        //  encoder.setAlgorithm(PBKDF2WithHmacSHA256);
        //  encoder.setEncodeHashAsBase64(true);


//        /** TODO IGNITE-5617 Revert to 25000 before release. */
//        private static final int ITERATIONS = 10;
//
//        /** */
//        private static final int KEY_LEN = 512 * 8;

//        /**
//         * @return Salt for hashing.
//         */
//        private String generateSalt() {
//            byte[] salt = new byte[32];
//
//            rnd.nextBytes(salt);
//
//            return U.byteArray2HexString(salt);
//        }
//

//        private String computeHash(String pwd, String salt) throws Exception {
//            // TODO IGNITE-5617: How about to re-hash on first successful compare?
//            PBEKeySpec spec = new PBEKeySpec(
//                pwd.toCharArray(),
//                salt.getBytes(StandardCharsets.UTF_8), // For compatibility with hashing data imported from NodeJS.
//                ITERATIONS,
//                KEY_LEN);
//
//            SecretKeyFactory skf = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256");
//
//            byte[] hash = skf.generateSecret(spec).getEncoded();
//
//            return U.byteArray2HexString(hash);
//        }

        return new BCryptPasswordEncoder();
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
}
