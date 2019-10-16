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

import org.apache.ignite.console.dto.Account;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.test.context.support.WithSecurityContextFactory;

/**
 * An API that works with WithUserTestExcecutionListener for creating a context.
 */
public class WithMockTestUserSecurityContextFactory implements WithSecurityContextFactory<WithMockTestUser> {
    /** {@inheritDoc} */
    @Override public SecurityContext createSecurityContext(WithMockTestUser annotation) {
        Account principal = new Account(
            annotation.username(),
            annotation.password(),
            null,
            null,
            null,
            null,
            null
        );

        principal.setToken(annotation.token());
        
        Authentication authentication =
            new UsernamePasswordAuthenticationToken(principal, annotation.password(), principal.getAuthorities());

        SecurityContext ctx = SecurityContextHolder.createEmptyContext();

        ctx.setAuthentication(authentication);
        
        return ctx;
    }
}
