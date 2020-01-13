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

import org.apache.ignite.console.dto.Account;
import org.apache.ignite.console.repositories.AccountsRepository;
import org.apache.ignite.console.tx.TransactionManager;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.security.authentication.DefaultAuthenticationEventPublisher;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;

/**
 * Account lockout strategy to prevent brute-force password.
 */
public class AuthenticationEventPublisher extends DefaultAuthenticationEventPublisher {
    /** Accounts repository. */
    private final AccountsRepository repo;

    /** Transaction manager. */
    private final TransactionManager txMgr;

    /**
     * @param publisher Account authentication configuration.
     * @param repo Account repository.
     * @param txMgr Transaction manager.
     */
    public AuthenticationEventPublisher(
        ApplicationEventPublisher publisher,
        AccountsRepository repo,
        TransactionManager txMgr
    ) {
        super(publisher);

        this.repo = repo;
        this.txMgr = txMgr;
    }

    /** {@inheritDoc} */
    @Override public void publishAuthenticationSuccess(Authentication authentication) {
        if (authentication.getPrincipal() instanceof Account) {
            Account acc0 = (Account)authentication.getPrincipal();

            if (acc0.getFailedLoginAttempts() > 0) {
                txMgr.doInTransaction(() -> {
                    Account acc = repo.getById(acc0.getId());

                    acc.setFailedLoginAttempts(0);
                    acc.setLastFailedLogin(0);

                    repo.save(acc);
                });
            }
        }

        super.publishAuthenticationSuccess(authentication);
    }

    /** {@inheritDoc} */
    @Override public void publishAuthenticationFailure(AuthenticationException e, Authentication authentication) {
        if (authentication.getPrincipal() instanceof String) {
            txMgr.doInTransaction(() -> {
                Account acc = repo.getByEmail((String)authentication.getPrincipal());

                acc.setFailedLoginAttempts(Math.min(acc.getFailedLoginAttempts() + 1, Integer.MAX_VALUE));
                acc.setLastFailedLogin(U.currentTimeMillis());

                repo.save(acc);
            });
        }

        super.publishAuthenticationFailure(e, authentication);
    }
}
