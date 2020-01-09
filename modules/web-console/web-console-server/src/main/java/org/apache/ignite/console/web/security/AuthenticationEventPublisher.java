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

import org.apache.ignite.console.config.AccountAuthenticationConfiguration;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.console.repositories.AccountsRepository;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.security.authentication.DefaultAuthenticationEventPublisher;
import org.springframework.security.authentication.LockedException;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;

import static org.apache.ignite.console.messages.WebConsoleMessageSource.message;

/**
 * Account lockout strategy to prevent brute-force password.
 */
public class AuthenticationEventPublisher extends DefaultAuthenticationEventPublisher {
    /** Account authentication configuration. */
    private AccountAuthenticationConfiguration cfg;

    /** Accounts repository. */
    private AccountsRepository repo;

    /**
     * @param publisher Account authentication configuration.
     * @param cfg Account authentication configuration.
     * @param repo Account repository.
     */
    public AuthenticationEventPublisher(
        ApplicationEventPublisher publisher,
        AccountAuthenticationConfiguration cfg,
        AccountsRepository repo
    ) {
        super(publisher);

        this.cfg = cfg;
        this.repo = repo;
    }

    /** {@inheritDoc} */
    @Override public void publishAuthenticationSuccess(Authentication authentication) {
        if (authentication.getPrincipal() instanceof Account) {
            Account acc = (Account)authentication.getPrincipal();

            int attemptsCnt = acc.getFailedLoginAttempts();

            if (attemptsCnt >= cfg.getMaxAttempts())
                throw new LockedException(message("err.account-too-many-attempts"));

            if (attemptsCnt > 0) {
                acc = repo.getById(acc.getId());

                long attemptsInterval = (long)Math.pow(cfg.getInterval(), Math.log(attemptsCnt + 1));
                long calculatedInterval = Math.min(attemptsInterval, cfg.getMaxInterval());

                if (U.currentTimeMillis() - acc.getLastFailedLogin() < calculatedInterval) {
                    acc.setFailedLoginAttempts(attemptsCnt + 1);
                    acc.setLastFailedLogin(U.currentTimeMillis());

                    repo.save(acc);

                    throw new LockedException(message("err.account-attempt-too-soon"));
                }

                acc.setFailedLoginAttempts(0);

                repo.save(acc);
            }
        }

        super.publishAuthenticationSuccess(authentication);
    }

    /** {@inheritDoc} */
    @Override public void publishAuthenticationFailure(AuthenticationException e, Authentication authentication) {
        if (authentication.getPrincipal() instanceof String) {
            Account acc = repo.getByEmail((String)authentication.getPrincipal());

            acc.setFailedLoginAttempts(acc.getFailedLoginAttempts() + 1);
            acc.setLastFailedLogin(U.currentTimeMillis());

            repo.save(acc);
        }

        super.publishAuthenticationFailure(e, authentication);
    }
}
