package org.apache.ignite.console.services;

import org.apache.ignite.console.TestConfiguration;
import org.apache.ignite.console.event.EventPublisher;
import org.apache.ignite.console.event.user.ActivityUpdateEvent;
import org.apache.ignite.console.repositories.ActivitiesRepository;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.UUID;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Acctivities service test.
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = {TestConfiguration.class})
public class ActivitiesServiceTest {
    /** Activities service. */
    @Autowired
    private ActivitiesService activitiesSrvc;

    /** Event publisher. */
    @MockBean
    private EventPublisher evtPublisher;

    /** Activities repository. */
    @MockBean
    private ActivitiesRepository activitiesRepo;

    /**
     * Should publish {@link org.apache.ignite.console.event.user.ActivityUpdateEvent}
     */
    @Test
    public void shouldPublishActivityUpdateEvent() {
        UUID accId = UUID.randomUUID();
        activitiesSrvc.save(accId, "grp", "act");

        ArgumentCaptor<ActivityUpdateEvent> captor = ArgumentCaptor.forClass(ActivityUpdateEvent.class);
        verify(evtPublisher, times(1)).publish(captor.capture());

        Assert.assertEquals(accId, captor.getValue().accId());
    }
}
