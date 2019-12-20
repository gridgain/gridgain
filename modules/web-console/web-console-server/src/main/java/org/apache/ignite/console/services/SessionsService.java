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

package org.apache.ignite.console.services;

import org.apache.ignite.console.common.SessionAttribute;
import org.apache.ignite.console.tx.TransactionManager;
import org.apache.ignite.internal.util.typedef.F;
import org.springframework.session.ExpiringSession;
import org.springframework.session.Session;
import org.springframework.session.SessionRepository;
import org.springframework.stereotype.Service;

/**
 * Service to managing {@link Session} instances.
 */
@Service
public class SessionsService {
    /** */
    private final TransactionManager txMgr;

    /** */
    private final SessionRepository<ExpiringSession> sesRepo;

    /**
     * @param txMgr Transactions manager.
     * @param sesRepo Repositories to work with sessions.
     */
    public SessionsService(TransactionManager txMgr, SessionRepository<ExpiringSession> sesRepo) {
        this.txMgr = txMgr;
        this.sesRepo = sesRepo;
    }

    /**
     * @param attr Session attribute.
     */
    public String get(SessionAttribute attr) {
        ExpiringSession ses = getSession(attr.getSessionId());

        return ses != null ? ses.getAttribute(attr.getName()) : null;
    }

    /**
     * @param attr Session attribute.
     * @param val the value of the attribute to set. If null, the attribute will be removed.
     */
    public void update(SessionAttribute attr, String val) {
        txMgr.doInTransaction(() -> {
            ExpiringSession ses = getSession(attr.getSessionId());

            if (ses != null && !F.eq(ses.getAttribute(attr.getName()), val)) {
                ses.setAttribute(attr.getName(), val);

                sesRepo.save(ses);
            }
        });
    }

    /**
     * @param attr Session attribute.
     */
    public void remove(SessionAttribute attr) {
        update(attr, null);
    }

    /**
     * Gets the {@link Session} by the {@link Session#getId()} or null if no {@link Session} is found.
     */
    private ExpiringSession getSession(String sesId) {
        return sesRepo.getSession(sesId);
    }
}
