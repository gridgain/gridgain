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

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import org.springframework.security.test.context.support.WithSecurityContext;

/**
 * Run test method with a mocked user.
 */
@Retention(RetentionPolicy.RUNTIME)
@WithSecurityContext(factory = WithMockTestUserSecurityContextFactory.class)
public @interface WithMockTestUser {
    /** User email. */
    String username() default "test@test.com";
    /** User password */
    String password() default "password";
    /** Agent token. */
    String token() default "token";
}
