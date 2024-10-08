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

package org.apache.ignite.internal.processors.odbc;

import java.net.InetSocketAddress;
import java.security.cert.Certificate;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.authentication.AuthorizationContext;
import org.apache.ignite.internal.processors.authentication.IgniteAccessControlException;
import org.apache.ignite.internal.processors.security.SecurityContext;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.plugin.security.AuthenticationContext;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.plugin.security.SecuritySubjectType.REMOTE_CLIENT;

/**
 * Base connection context.
 */
public abstract class ClientListenerAbstractConnectionContext implements ClientListenerConnectionContext {
    /** Kernal context. */
    protected final GridKernalContext ctx;

    /** Nio session. */
    protected final GridNioSession ses;

    /** Security context or {@code null} if security is disabled. */
    private SecurityContext secCtx;

    /** Connection ID. */
    private long connId;

    /** Authorization context. */
    private AuthorizationContext authCtx;

    /** User attributes. */
    protected Map<String, String> userAttrs;

    /**
     * Describes the client connection:
     * - thin cli: "cli:host:port@user_name"
     * - thin JDBC: "jdbc-thin:host:port@user_name"
     * - ODBC: "odbc:host:port@user_name"
     *
     * Used by the running query view to display query initiator.
     */
    private String clientDesc;

    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     * @param ses Client's NIO session.
     * @param connId Connection ID.
     */
    protected ClientListenerAbstractConnectionContext(GridKernalContext ctx, GridNioSession ses,
        long connId) {
        this.ctx = ctx;
        this.connId = connId;
        this.ses = ses;
    }

    /**
     * @return Kernal context.
     */
    public GridKernalContext kernalContext() {
        return ctx;
    }

    /** {@inheritDoc} */
    @Nullable @Override public SecurityContext securityContext() {
        return secCtx;
    }

    /** {@inheritDoc} */
    @Nullable @Override public AuthorizationContext authorizationContext() {
        return authCtx;
    }

    /** {@inheritDoc} */
    @Override public long connectionId() {
        return connId;
    }

    /**
     * Perform authentication.
     *
     * @return Auth context.
     * @throws IgniteCheckedException If failed.
     */
    protected AuthorizationContext authenticate(InetSocketAddress remoteAddr, Certificate[] certificates, String user, String pwd)
        throws IgniteCheckedException {
        if (ctx.security().enabled())
            authCtx = authenticateExternal(remoteAddr, certificates, user, pwd).authorizationContext();
        else if (ctx.authentication().enabled()) {
            if (F.isEmpty(user))
                throw new IgniteAccessControlException("Unauthenticated sessions are prohibited.");

            authCtx = ctx.authentication().authenticate(user, pwd);

            if (authCtx == null)
                throw new IgniteAccessControlException("Unknown authentication error.");
        }
        else
            authCtx = null;

        return authCtx;
    }

    /**
     * Do 3-rd party authentication.
     */
    private AuthenticationContext authenticateExternal(InetSocketAddress remoteAddr, Certificate[] certificates, String user, String pwd)
        throws IgniteCheckedException {
        SecurityCredentials cred = new SecurityCredentials(user, pwd);

        AuthenticationContext authCtx = new AuthenticationContext();

        authCtx.subjectType(REMOTE_CLIENT);
        authCtx.subjectId(UUID.randomUUID());
        authCtx.nodeAttributes(F.isEmpty(userAttrs) ? Collections.emptyMap() : userAttrs);
        authCtx.credentials(cred);
        authCtx.certificates(certificates);
        authCtx.address(remoteAddr);
        authCtx.allAddresses(Collections.singletonList(remoteAddr));

        secCtx = ctx.security().authenticate(authCtx);

        if (secCtx == null) {
            throw new IgniteAccessControlException(
                String.format("The user name or password is incorrect [userName=%s]", user)
            );
        }

        return authCtx;
    }

    /** {@inheritDoc} */
    @Override public void onDisconnected() {
        if (ctx.security().enabled())
            ctx.security().onSessionExpired(secCtx.subject().id());
    }

    /**
     *
     */
    protected void initClientDescriptor(String prefix) {
        clientDesc = prefix + ":" + ses.remoteAddress().getHostString() + ":" + ses.remoteAddress().getPort();

        if (authCtx != null)
            clientDesc += "@" + authCtx.userName();
        else if (secCtx != null)
            clientDesc += "@" + secCtx.subject().login();
    }

    /**
     * Describes the client connection:
     * - thin cli: "cli:host:port@user_name"
     * - thin JDBC: "jdbc-thin:host:port@user_name"
     * - ODBC: "odbc:host:port@user_name"
     *
     * Used by the running query view to display query initiator.
     *
     * @return Client descriptor string.
     */
    public String clientDescriptor() {
        return clientDesc;
    }

    /** {@inheritDoc} */
    @Override public Map<String, String> attributes() {
        return F.isEmpty(userAttrs) ? Collections.emptyMap() : Collections.unmodifiableMap(userAttrs);
    }
}
