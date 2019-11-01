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

package org.apache.ignite.agent.action;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.IgniteAuthenticationException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.management.ManagementConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.agent.utils.AgentUtils.authenticate;

/**
 * Security session registry.
 */
public class SessionRegistry {
    /** Instance. */
    private static volatile SessionRegistry instance;

    /** Context. */
    private final GridKernalContext ctx;

    /** Logger. */
    private final IgniteLogger log;

    /** SessionId-Session map. */
    private final ConcurrentMap<UUID, Session> sesId2Ses = new ConcurrentHashMap<>();

    /** Session time to live. */
    private final Duration sesTtl;

    /** Interval to invalidate session tokens. */
    private final Duration sesTokTtl;

    /**
     * @param ctx Context.
     */
    private SessionRegistry(GridKernalContext ctx) {
        this.ctx = ctx;
        this.log = ctx.log(SessionRegistry.class);

        ManagementConfiguration cfg = ctx.managementConsole().configuration();

        sesTtl = Duration.ofMillis(cfg.getSecuritySessionTimeout());
        sesTokTtl = Duration.ofMillis(cfg.getSecuritySessionExpirationTimeout());
    }

    /**
     * @param ctx Context.
     */
    public static SessionRegistry getInstance(GridKernalContext ctx) {
        SessionRegistry locInstance = instance;

        if (locInstance == null) {
            synchronized (SessionRegistry.class) {
                locInstance = instance;

                if (locInstance == null)
                    instance = locInstance = new SessionRegistry(ctx);
            }
        }

        return locInstance;
    }

    /**
     * @param ses Session.
     */
    public void saveSession(Session ses) {
        sesId2Ses.put(ses.id(), ses);
    }

    /**
     * @param sesId Session ID.
     * @return Not null session.
     * @throws IgniteAuthenticationException If failed.
     */
    public Session getSession(UUID sesId) throws IgniteCheckedException {
        if (sesId == null)
            throw new IgniteAuthenticationException("Invalid session ID: null");

        Session ses = sesId2Ses.get(sesId);

        if (ses == null)
            throw new IgniteAuthenticationException("Session not found for ID: " + sesId);

        if (!ses.touch() || ses.isTimedOut(sesTtl.toMillis())) {
            sesId2Ses.remove(ses.id(), ses);

            if (ctx.security().enabled() && ses.securityContext() != null && ses.securityContext().subject() != null)
                ctx.security().onSessionExpired(ses.securityContext().subject().id());

            return null;
        }

        if (ses.isSessionExpired(sesTokTtl.toMillis())) {
            ses.securityContext(authenticate(ctx.security(), ses));
            ses.lastInvalidateTime(U.currentTimeMillis());
            sesId2Ses.put(ses.id(), ses);
        }

        return ses;
    }
}
