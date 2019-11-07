package org.apache.ignite.ssl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

/**
 * Checks that the same instance of {@link SSLContext} is returned from {@link SslContextFactory}.
 */
public class SingletonSSLContextFactoryTest {
    /**
     * @return {@link SslContextFactory} to use in the test.
     */
    private SslContextFactory createSslFactory() {
        SslContextFactory factory = new SslContextFactory();

        factory.setKeyStoreFilePath(GridTestUtils.keyStorePath("cluster"));
        factory.setKeyStorePassword(GridTestUtils.keyStorePassword().toCharArray());
        factory.setTrustStoreFilePath(GridTestUtils.keyStorePath("trustone"));
        factory.setTrustStorePassword(GridTestUtils.keyStorePassword().toCharArray());

        return factory;
    }

    /**
     * Checks that {@link SslContextFactory#create()} returns the same instance of {@link SSLContext} when called
     * from the same thread multiple times.
     */
    @Test
    public void testSingleThread() {
        int iterations = 100;

        Factory<SSLContext> sslCtxFactory = createSslFactory();

        SSLContext ctx = sslCtxFactory.create();

        for (int i = 0; i < iterations; i++) {
            assertSame("Two invocations to SSLContextFactory#create() returned different objects.",
                ctx, sslCtxFactory.create());
        }
    }

    /**
     * Checks that {@link SslContextFactory#create()} returns the same instance of {@link SSLContext} when called
     * from multiple threads concurrently.
     *
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void testMultipleThreads() throws IgniteCheckedException {
        int threadsNum = 4;
        int iterations = 100;

        List<IgniteInternalFuture> futs = new ArrayList<>(threadsNum);

        Factory<SSLContext> sslCtxFactory = createSslFactory();
        AtomicReference<SSLContext> ctx = new AtomicReference<>();

        CountDownLatch latch = new CountDownLatch(threadsNum);

        for (int i = 0; i < threadsNum; i++) {
            IgniteInternalFuture fut = GridTestUtils.runAsync(() -> {
                try {
                    for (int j = 0; j < iterations; j++) {
                        latch.countDown();
                        latch.await();

                        SSLContext newCtx = sslCtxFactory.create();

                        SSLContext oldCtx = ctx.getAndSet(newCtx);

                        if (oldCtx != null) {
                            assertSame(
                                "Two invocations to SSLContextFactory#create() returned different objects.",
                                oldCtx, newCtx);
                        }
                    }
                }
                catch (InterruptedException e) {
                    e.printStackTrace();

                    fail("Exception was thrown: " + e.getMessage());
                }
            });

            futs.add(fut);
        }

        for (IgniteInternalFuture fut : futs)
            fut.get();
    }
}
