package org.apache.ignite.console.services;


import org.apache.ignite.console.TestConfiguration;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.console.event.EventPublisher;
import org.apache.ignite.console.event.user.UserCreateByAdminEvent;
import org.apache.ignite.console.event.user.UserDeleteEvent;
import org.apache.ignite.console.repositories.ConfigurationsRepository;
import org.apache.ignite.console.repositories.NotebooksRepository;
import org.apache.ignite.console.web.model.SignUpRequest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.UUID;

import static org.mockito.Mockito.*;

/**
 * Admin service test.
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = {TestConfiguration.class})
public class AdminServiceTest {
    /** Activities service. */
    @Autowired
    private AdminService adminSrvc;

    /** Event publisher. */
    @MockBean
    private EventPublisher evtPublisher;

    /** Configurations repository. */
    @MockBean
    private ConfigurationsRepository configurationsRepo;

    /** Notebooks repository. */
    @MockBean
    private NotebooksRepository notebooksRepo;

    /** Accounts service. */
    @MockBean
    private AccountsService accountsSrvc;

    /**
     * Should publish {@link org.apache.ignite.console.event.user.UserDeleteEvent}
     */
    @Test
    public void shouldPublishUserDeleteEvent() {
        when(accountsSrvc.delete(any(UUID.class)))
            .thenAnswer(invocation -> {
                Account acc = new Account();
                acc.setId(invocation.getArgumentAt(0, UUID.class));

                return acc;
            });

        UUID accId = UUID.randomUUID();
        adminSrvc.delete(accId);

        ArgumentCaptor<UserDeleteEvent> captor = ArgumentCaptor.forClass(UserDeleteEvent.class);
        verify(evtPublisher, times(1)).publish(captor.capture());

        Assert.assertEquals(accId, captor.getValue().getUser().getId());
    }

    /**
     * Should publish {@link org.apache.ignite.console.event.user.UserCreateByAdminEvent}
     */
    @Test
    public void shouldPublishUserCreateByAdminEvent() {
        when(accountsSrvc.create(any(SignUpRequest.class)))
                .thenAnswer(invocation -> {
                    SignUpRequest req = invocation.getArgumentAt(0, SignUpRequest.class);
                    Account acc = new Account();
                    acc.setEmail(req.getEmail());
                    acc.setPassword(req.getPassword());

                    return acc;
                });

        SignUpRequest req = new SignUpRequest();
        req.setEmail("mail@mail");
        req.setPassword("1");

        adminSrvc.registerUser(req);

        ArgumentCaptor<UserCreateByAdminEvent> captor = ArgumentCaptor.forClass(UserCreateByAdminEvent.class);
        verify(evtPublisher, times(1)).publish(captor.capture());

        Assert.assertEquals("mail@mail", captor.getValue().getUser().getEmail());
    }
}