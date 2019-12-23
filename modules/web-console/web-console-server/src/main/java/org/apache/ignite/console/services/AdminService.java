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

import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.console.dto.Announcement;
import org.apache.ignite.console.event.Event;
import org.apache.ignite.console.event.EventPublisher;
import org.apache.ignite.console.json.JsonArray;
import org.apache.ignite.console.json.JsonObject;
import org.apache.ignite.console.repositories.AnnouncementRepository;
import org.apache.ignite.console.repositories.ConfigurationsRepository;
import org.apache.ignite.console.tx.TransactionManager;
import org.apache.ignite.console.web.model.ConfigurationKey;
import org.apache.ignite.console.web.model.SignUpRequest;
import org.apache.ignite.console.web.socket.TransitionService;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.session.ExpiringSession;
import org.springframework.session.FindByIndexNameSessionRepository;
import org.springframework.stereotype.Service;

import static org.apache.ignite.console.event.AccountEventType.ACCOUNT_CREATE_BY_ADMIN;
import static org.apache.ignite.console.event.AccountEventType.ACCOUNT_DELETE;
import static org.apache.ignite.console.repositories.ConfigurationsRepository.CACHES_CNT;
import static org.apache.ignite.console.repositories.ConfigurationsRepository.MODELS_CNT;
import static org.springframework.session.FindByIndexNameSessionRepository.PRINCIPAL_NAME_INDEX_NAME;

/**
 * Service to handle administrator actions.
 */
@Service
public class AdminService {
    /** */
    private final TransactionManager txMgr;

    /** */
    private final AccountsService accountsSrv;

    /** */
    private final ConfigurationsService cfgsSrv;

    /** */
    private final NotebooksService notebooksSrv;

    /** */
    private final ActivitiesService activitiesSrv;

    /** */
    protected EventPublisher evtPublisher;

    /** */
    private final AnnouncementRepository annRepo;

    /** */
    private final TransitionService transitionSrvc;

    /** */
    private final FindByIndexNameSessionRepository<ExpiringSession> sesRepo;

    /** */
    private final ConfigurationsRepository cfgsRepo;

    /**
     * @param txMgr Transactions manager.
     * @param accountsSrv Service to work with accounts.
     * @param cfgsSrv Service to work with configurations.
     * @param notebooksSrv Service to work with notebooks.
     * @param activitiesSrv Service to work with activities.
     * @param evtPublisher Service to publish events.
     * @param annRepo Repository to work with announcement.
     * @param transitionSrvc Transition service.
     * @param sesRepo Sessions repository.
     * @param cfgsRepo Configurations repository.
     */
    public AdminService(
        TransactionManager txMgr,
        AccountsService accountsSrv,
        ConfigurationsService cfgsSrv,
        NotebooksService notebooksSrv,
        ActivitiesService activitiesSrv,
        EventPublisher evtPublisher,
        AnnouncementRepository annRepo,
        TransitionService transitionSrvc,
        FindByIndexNameSessionRepository<ExpiringSession> sesRepo,
        ConfigurationsRepository cfgsRepo
    ) {
        this.txMgr = txMgr;
        this.accountsSrv = accountsSrv;
        this.cfgsSrv = cfgsSrv;
        this.notebooksSrv = notebooksSrv;
        this.activitiesSrv = activitiesSrv;
        this.evtPublisher = evtPublisher;
        this.annRepo = annRepo;
        this.transitionSrvc = transitionSrvc;
        this.sesRepo = sesRepo;
        this.cfgsRepo = cfgsRepo;
    }

    /**
     * @param startDate Start date.
     * @param endDate End date.
     * @return List of all users.
     */
    public JsonArray list(long startDate, long endDate) {
        List<Account> accounts = accountsSrv.list();

        JsonArray res = new JsonArray();

        accounts.forEach(account -> {
            Map<String, ExpiringSession> ses = sesRepo.findByIndexNameAndIndexValue(
                PRINCIPAL_NAME_INDEX_NAME,
                account.getUsername()
            );

            long lastLogin = ses.values().stream().map(ExpiringSession::getCreationTime).max(Long::compareTo).orElse(-1L);
            long lastActivity = ses.values().stream().map(ExpiringSession::getLastAccessedTime).max(Long::compareTo).orElse(-1L);

            JsonArray clusters = cfgsRepo.loadClusters(new ConfigurationKey(account.getId(), false));

            int caches = 0;
            int models = 0;

            for (Object item : clusters) {
                JsonObject json = (JsonObject)item;

                caches += json.getInteger(CACHES_CNT, 0);
                models += json.getInteger(MODELS_CNT, 0);
            }

            res.add(new JsonObject()
                .add("id", account.getId())
                .add("firstName", account.getFirstName())
                .add("lastName", account.getLastName())
                .add("admin", account.isAdmin())
                .add("email", account.getUsername())
                .add("company", account.getCompany())
                .add("country", account.getCountry())
                .add("lastLogin", lastLogin)
                .add("lastActivity", lastActivity)
                .add("activated", account.isEnabled())
                .add("counters", new JsonObject()
                    .add("clusters", clusters.size())
                    .add("caches", caches)
                    .add("models", models))
                .add("activitiesDetail", activitiesSrv.activitiesForPeriod(account.getId(), startDate, endDate))
            );
        });

        return res;
    }

    /**
     * Delete account by ID.
     *
     * @param accId Account ID.
     */
    public void delete(UUID accId) {
        Account acc = txMgr.doInTransaction(() -> {
            cfgsSrv.deleteByAccountId(accId);

            notebooksSrv.deleteByAccountId(accId);

            return accountsSrv.delete(accId);
        });

        evtPublisher.publish(new Event<>(ACCOUNT_DELETE, acc));
    }

    /**
     * @param accId Account ID.
     * @param admin Admin flag.
     */
    public void toggle(UUID accId, boolean admin) {
        accountsSrv.toggle(accId, admin);
    }

    /**
     * @param params SignUp params.
     */
    public Account registerUser(SignUpRequest params) {
        Account acc = accountsSrv.create(params);

        evtPublisher.publish(new Event<>(ACCOUNT_CREATE_BY_ADMIN, acc));

        return acc;
    }

    /** */
    @EventListener(ApplicationReadyEvent.class)
    public void initAnnouncement() {
        updateAnnouncement(annRepo.load());
    }

    /**
     * @param ann Announcement.
     */
    public void updateAnnouncement(Announcement ann) {
        annRepo.save(ann);

        transitionSrvc.broadcastToBrowsers(ann);
    }
}
