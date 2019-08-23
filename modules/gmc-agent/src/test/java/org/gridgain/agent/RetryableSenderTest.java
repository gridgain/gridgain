package org.gridgain.agent;

import org.apache.ignite.IgniteLogger;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.with;

/**
 * Retryable sender test.
 */
public class RetryableSenderTest {
    /**
     * Should split list of elements into batches and send.
     */
    @Test
    public void shouldSendInBatches() {
        List<List<Integer>> results = new ArrayList<>();
        RetryableSender<Integer> snd = new RetryableSender<>(Mockito.mock(IgniteLogger.class), 10, results::add);

        snd.send(IntStream.range(0, 17).boxed().collect(Collectors.toList()));

        with().pollInterval(100, MILLISECONDS).await().atMost(1, SECONDS).until(() -> !results.isEmpty() && results.get(0).size() == 10);
        with().pollInterval(100, MILLISECONDS).await().atMost(1, SECONDS).until(() -> !results.isEmpty() && results.get(1).size() == 7);
    }

    /**
     * Should retry send elements if we can't send.
     */
    @Test
    public void shouldRetrySend() {
        List<List<Integer>> results = new ArrayList<>();
        AtomicBoolean shouldSnd = new AtomicBoolean(false);

        RetryableSender<Integer> snd = new RetryableSender<>(Mockito.mock(IgniteLogger.class), 10, (b) -> {
            if (!shouldSnd.get())
                throw new RuntimeException();
            else
                results.add(b);
        });


        snd.send(IntStream.range(0, 17).boxed().collect(Collectors.toList()));

        with().pollInterval(500, MILLISECONDS).await().atMost(10, SECONDS).until(() -> snd.retryCnt >= 1);

        shouldSnd.set(true);

        with().pollInterval(100, MILLISECONDS).await().atMost(10, SECONDS).until(() -> !results.isEmpty() && results.get(0).size() == 7);
        with().pollInterval(100, MILLISECONDS).await().atMost(10, SECONDS).until(() -> !results.isEmpty() && results.get(1).size() == 10);
    }

    /**
     * Should retry send elements if we can't send.
     */
    @Test
    public void shouldDropOldElementsFromQueue() {
        List<List<Integer>> results = new ArrayList<>();
        AtomicBoolean shouldSnd = new AtomicBoolean(false);

        RetryableSender<Integer> snd = new RetryableSender<>(Mockito.mock(IgniteLogger.class), 3, (b) -> {
            if (!shouldSnd.get())
                throw new RuntimeException();
            else
                results.add(b);
        });

        snd.send(IntStream.range(0, 27).boxed().collect(Collectors.toList()));
        with().pollInterval(500, MILLISECONDS).await().atMost(3, SECONDS).until(() -> snd.retryCnt == 2);

        shouldSnd.set(true);

        snd.send(IntStream.range(30, 60).boxed().collect(Collectors.toList()));
        with().pollInterval(100, MILLISECONDS).await().atMost(3, SECONDS).until(() -> results.size() == 3);

        Assert.assertEquals(30, (int) results.get(0).get(0));
        Assert.assertEquals(40, (int) results.get(1).get(0));
        Assert.assertEquals(50, (int) results.get(2).get(0));
    }
}