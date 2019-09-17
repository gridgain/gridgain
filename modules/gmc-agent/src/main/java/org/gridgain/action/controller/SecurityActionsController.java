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

package org.gridgain.action.controller;

import org.apache.ignite.IgniteAuthenticationException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.gridgain.action.annotation.ActionController;
import org.gridgain.action.annotation.WithoutAuth;
import org.gridgain.agent.AgentUtils;
import org.gridgain.dto.action.AuthenticateCredentials;
import org.gridgain.action.Session;
import org.gridgain.action.SessionRegistry;

import java.util.concurrent.CompletableFuture;

/**
 * Controller for security actions.
 */
@WithoutAuth
@ActionController("SecurityActions")
public class SecurityActionsController {
    /** Context. */
    private final GridKernalContext ctx;

    /** Registry. */
    private final SessionRegistry registry;

    /** Logger. */
    private final IgniteLogger log;

    /**
     * @param ctx Context.
     */
    public SecurityActionsController(GridKernalContext ctx) {
        this.ctx = ctx;
        log = ctx.log(SecurityActionsController.class);
        registry = SessionRegistry.getInstance(ctx);
    }

    /**
     * @param reqCreds Request credentials.
     */
    public CompletableFuture<String> authenticate(AuthenticateCredentials reqCreds) {
        CompletableFuture<String> fut = new CompletableFuture<>();

        try {
            Session ses = authenticate0(reqCreds);
            registry.saveSession(ses);

            if (log.isDebugEnabled())
                log.debug("Next sessionId were generated for request: " +
                        "[sessionId=" + ses.id() + "]");

            fut.complete(ses.id().toString());
        }
        catch (Exception e) {
            fut.completeExceptionally(e);
        }

        return fut;
    }

    /**
     * Authenticates remote client.
     *
     * @return Authentication subject context.
     * @throws IgniteCheckedException If authentication failed.
     */
    private Session authenticate0(AuthenticateCredentials reqCreds) throws IgniteCheckedException {
        boolean authenticationEnabled = ctx.authentication().enabled();
        boolean securityEnabled = ctx.security().enabled();

        if (reqCreds.getCredentials() == null)
            throw new IgniteAuthenticationException("Failed to autheticate, please provide credentials");

        Session ses = Session.random();
        ses.credentials(reqCreds.getCredentials());
        ses.address(reqCreds.getAddress());

        if (securityEnabled) {
            ses.securityContext(AgentUtils.authenticate(ctx.security(), ses));
            ses.lastInvalidateTime(U.currentTimeMillis());
        } else if (authenticationEnabled) {
            SecurityCredentials creds = ses.credentials();

            String login = null;

            if (creds.getLogin() instanceof String)
                login = (String) creds.getLogin();

            String pwd = null;

            if (creds.getPassword() instanceof String)
                pwd = (String) creds.getPassword();

            ses.authorizationContext(ctx.authentication().authenticate(login, pwd));
        }

        return ses;
    }
}
