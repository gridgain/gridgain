/*
 * Copyright 2025 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.testframework;

import com.google.common.collect.Lists;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Random;
import java.util.Scanner;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.cache.CacheException;
import javax.cache.configuration.Factory;
import javax.management.Attribute;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteFutureCancelledCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.client.ssl.GridSslBasicContextFactory;
import org.apache.ignite.internal.client.ssl.GridSslContextFactory;
import org.apache.ignite.internal.managers.discovery.CustomMessageWrapper;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearCacheAdapter;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.verify.IdleVerifyResultV2;
import org.apache.ignite.internal.processors.odbc.ClientListenerProcessor;
import org.apache.ignite.internal.processors.port.GridPortRecord;
import org.apache.ignite.internal.util.GridBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.GridAbsClosure;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.lang.IgnitePair;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
import org.apache.ignite.spi.discovery.DiscoverySpiListener;
import org.apache.ignite.ssl.SslContextFactory;
import org.apache.ignite.testframework.junits.GridAbstractTest;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;

import static java.lang.Long.parseLong;
import static java.util.Comparator.comparingLong;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;
import static org.apache.ignite.ssl.SslContextFactory.DFLT_KEY_ALGORITHM;
import static org.apache.ignite.ssl.SslContextFactory.DFLT_SSL_PROTOCOL;
import static org.apache.ignite.ssl.SslContextFactory.DFLT_STORE_TYPE;
import static org.apache.ignite.testframework.config.GridTestProperties.getProperty;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Utility class for tests.
 */
public final class GridTestUtils {
    /** Default busy wait sleep interval in milliseconds.  */
    public static final long DFLT_BUSYWAIT_SLEEP_INTERVAL = 200;

    /** */
    public static final long DFLT_TEST_TIMEOUT = 5 * 60 * 1000;

    /** yyyy-MM-dd. */
    public static final String LOCAL_DATE_REGEXP = "[0-9]{4}-(((0[13578]|(10|12))-(0[1-9]|[1-2][0-9]|3[0-1]))|(02-(0[1-9]|[1-2][0-9]))|((0[469]|11)-(0[1-9]|[1-2][0-9]|30)))";

    /** HH:mm:ss.SSS. */
    public static final String LOCAL_TIME_REGEXP = "(20|21|22|23|[01]\\d|\\d)((:[0-5]\\d){1,2})\\.\\d{3}";

    /** yyyy-MM-dd'T'HH:mm:ss.SSS. */
    public static final String LOCAL_DATETIME_REGEXP = LOCAL_DATE_REGEXP + "T" + LOCAL_TIME_REGEXP;

    /** */
    static final String ALPHABETH = "qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM1234567890_";

    /**
     * Hook object intervenes to discovery message handling
     * and thus allows to make assertions or other actions like skipping certain discovery messages.
     */
    public static class DiscoveryHook {
        /**
         * Handles discovery message before {@link DiscoverySpiListener#onDiscovery} invocation.
         *
         * @param msg Intercepted discovery message.
         */
        public void beforeDiscovery(DiscoverySpiCustomMessage msg) {
            if (msg instanceof CustomMessageWrapper)
                beforeDiscovery(unwrap((CustomMessageWrapper)msg));
        }

        /**
         * Handles {@link DiscoveryCustomMessage} before {@link DiscoverySpiListener#onDiscovery} invocation.
         *
         * @param customMsg Intercepted {@link DiscoveryCustomMessage}.
         */
        public void beforeDiscovery(DiscoveryCustomMessage customMsg) {
            // No-op.
        }

        /**
         * Handles discovery message after {@link DiscoverySpiListener#onDiscovery} completion.
         *
         * @param msg Intercepted discovery message.
         */
        public void afterDiscovery(DiscoverySpiCustomMessage msg) {
            if (msg instanceof CustomMessageWrapper)
                afterDiscovery(unwrap((CustomMessageWrapper)msg));
        }

        /**
         * Handles {@link DiscoveryCustomMessage} after {@link DiscoverySpiListener#onDiscovery} completion.
         *
         * @param customMsg Intercepted {@link DiscoveryCustomMessage}.
         */
        public void afterDiscovery(DiscoveryCustomMessage customMsg) {
            // No-op.
        }

        /**
         * @param ignite Ignite.
         */
        public void ignite(IgniteEx ignite) {
            // No-op.
        }

        /**
         * Obtains {@link DiscoveryCustomMessage} from {@link CustomMessageWrapper}.
         *
         * @param wrapper Wrapper of {@link DiscoveryCustomMessage}.
         * @return Unwrapped {@link DiscoveryCustomMessage}.
         */
        private DiscoveryCustomMessage unwrap(CustomMessageWrapper wrapper) {
            return U.field(wrapper, "delegate");
        }
    }

    /**
     * Injects {@link DiscoveryHook} into handling logic.
     */
    public static final class DiscoverySpiListenerWrapper implements DiscoverySpiListener {
        /** */
        private final DiscoverySpiListener delegate;

        /** Interceptor of discovery messages. */
        private final DiscoveryHook hook;

        /**
         * @param delegate Delegate.
         * @param hook Interceptor of discovery messages.
         */
        private DiscoverySpiListenerWrapper(DiscoverySpiListener delegate, DiscoveryHook hook) {
            this.hook = hook;
            this.delegate = delegate;
        }

        /** {@inheritDoc} */
        @Override public IgniteFuture<?> onDiscovery(
            int type,
            long topVer,
            ClusterNode node,
            Collection<ClusterNode> topSnapshot,
            @Nullable Map<Long, Collection<ClusterNode>> topHist,
            @Nullable DiscoverySpiCustomMessage spiCustomMsg
        ) {
            hook.beforeDiscovery(spiCustomMsg);

            IgniteFuture<?> fut = delegate.onDiscovery(type, topVer, node, topSnapshot, topHist, spiCustomMsg);

            fut.listen(f -> hook.afterDiscovery(spiCustomMsg));

            return fut;
        }

        /** {@inheritDoc} */
        @Override public void onLocalNodeInitialized(ClusterNode locNode) {
            delegate.onLocalNodeInitialized(locNode);
        }

        /**
         * @param delegate Delegate.
         * @param discoHook Interceptor of discovery messages.
         */
        public static DiscoverySpiListener wrap(DiscoverySpiListener delegate, DiscoveryHook discoHook) {
            return new DiscoverySpiListenerWrapper(delegate, discoHook);
        }
    }

    /** Test parameters scale factor util. */
    public static final class SF extends ScaleFactorUtil {

    }

    /** */
    private static final Map<Class<?>, String> addrs = new HashMap<>();

    /** */
    private static final Map<Class<? extends GridAbstractTest>, Integer> mcastPorts = new HashMap<>();

    /** */
    private static final Map<Class<? extends GridAbstractTest>, Integer> discoPorts = new HashMap<>();

    /** */
    private static final Map<Class<? extends GridAbstractTest>, Integer> commPorts = new HashMap<>();

    /** */
    private static int[] addr;

    /** */
    private static final int default_mcast_port = 50000;

    /** */
    private static final int max_mcast_port = 54999;

    /** */
    private static final int default_comm_port = 45000;

    /** */
    private static final int max_comm_port = 49999;

    /** */
    private static final int default_disco_port = 55000;

    /** */
    private static final int max_disco_port = 59999;

    /** */
    private static int mcastPort = default_mcast_port;

    /** */
    private static int discoPort = default_disco_port;

    /** */
    private static int commPort = default_comm_port;

    /** */
    private static final GridBusyLock busyLock = new GridBusyLock();

    /** */
    public static final ConcurrentMap<IgnitePair<UUID>, IgnitePair<Queue<Message>>> msgMap = new ConcurrentHashMap<>();

    /**
     * Ensure singleton.
     */
    private GridTestUtils() {
        // No-op.
    }

    /**
     * @param from From node ID.
     * @param to To node ID.
     * @param msg Message.
     * @param sent Sent or received.
     */
    public static void addMessage(UUID from, UUID to, Message msg, boolean sent) {
        IgnitePair<UUID> key = new IgnitePair<>(from, to);

        IgnitePair<Queue<Message>> val = msgMap.get(key);

        if (val == null) {
            IgnitePair<Queue<Message>> old = msgMap.putIfAbsent(key,
                val = new IgnitePair<Queue<Message>>(
                    new ConcurrentLinkedQueue<Message>(), new ConcurrentLinkedQueue<Message>()));

            if (old != null)
                val = old;
        }

        (sent ? val.get1() : val.get2()).add(msg);
    }

    /**
     * Dumps all messages tracked with {@link #addMessage(UUID, UUID, Message, boolean)} to std out.
     */
    public static void dumpMessages() {
        for (Map.Entry<IgnitePair<UUID>, IgnitePair<Queue<Message>>> entry : msgMap.entrySet()) {
            U.debug("\n" + entry.getKey().get1() + " [sent to] " + entry.getKey().get2());

            for (Message message : entry.getValue().get1())
                U.debug("\t" + message);

            U.debug(entry.getKey().get2() + " [received from] " + entry.getKey().get1());

            for (Message message : entry.getValue().get2())
                U.debug("\t" + message);
        }
    }

    /**
     * Checks that string {@param str} matches given regular expression {@param regexp}. Logs both strings
     * and throws {@link java.lang.AssertionError}, if not.
     *
     * @param log Logger (optional).
     * @param str String.
     * @param regexp Regular expression.
     */
    public static void assertMatches(@Nullable IgniteLogger log, String str, String regexp) {
        try {
            assertTrue(Pattern.compile(regexp).matcher(str).find());
        } catch (AssertionError e) {
            U.warn(log, String.format("String does not matches regexp: '%s':", regexp));
            U.warn(log, "String:");
            U.warn(log, str);

            throw e;
        }
    }

    /**
     * Checks that file contains line {@param str} matches given regular expression {@param regexp}. Logs both strings
     * and throws {@link java.lang.AssertionError}, if not.
     *
     * @param log Logger (optional).
     * @param filePath Absolute path to file.
     * @param charset File charset.
     * @param regexp Regular expression pattern.
     */
    public static void assertMatchesLine(@Nullable IgniteLogger log, String filePath, String charset, Pattern regexp) throws IOException {
        try (Scanner input = new Scanner(new FileInputStream(filePath), charset)) {
            input.useDelimiter("[\\s]*\\n[\\s]*");

            boolean found = false;

            while (input.hasNext()) {
                if (found = input.hasNext(regexp))
                    break;
                else
                    input.nextLine();
            }

            if (!found) {
                U.warn(log, String.format("File doesn't contain a line matching regexp: '%s'", regexp));
                U.warn(log, String.format("File name: '%s'", filePath));

                Assert.fail("File doesn't contain a line matching regexp");
            }
        }
    }

    /**
     * Checks that string {@param str} doesn't match given regular expression {@param regexp}. Logs both strings
     * and throws {@link java.lang.AssertionError}, if matches.
     *
     * @param log Logger (optional).
     * @param str String.
     * @param regexp Regular expression.
     */
    public static void assertNotMatches(@Nullable IgniteLogger log, String str, String regexp) {
        try {
            assertFalse(Pattern.compile(regexp).matcher(str).find());
        } catch (AssertionError e) {
            U.warn(log, String.format("String matches regexp: '%s', but shouldn't:", regexp));
            U.warn(log, "String:");
            U.warn(log, str);

            throw e;
        }
    }

    /**
     * Checks that string {@param str} doesn't contains substring {@param substr}. Logs both strings
     * and throws {@link java.lang.AssertionError}, if contains.
     *
     * @param log Logger (optional).
     * @param str String.
     * @param substr Substring.
     */
    public static void assertNotContains(@Nullable IgniteLogger log, String str, String substr) {
        try {
            assertFalse(str.contains(substr));
        } catch (AssertionError e) {
            U.warn(log, String.format("String contain substring: '%s', but shouldn't:", substr));
            U.warn(log, "String:");
            U.warn(log, str);

            throw e;
        }
    }

    /**
     * Checks that string {@param str} contains substring {@param substr}. Logs both strings
     * and throws {@link java.lang.AssertionError}, if not.
     *
     * @param log Logger (optional).
     * @param str String.
     * @param substr Substring.
     */
    public static void assertContains(@Nullable IgniteLogger log, String str, String substr) {
        try {
            assertTrue(str, str != null && str.contains(substr));
        } catch (AssertionError e) {
            U.warn(log, String.format("String does not contain substring: '%s':", substr));
            U.warn(log, "String:");
            U.warn(log, str);

            throw e;
        }
    }

    /**
     * Checks that collection {@param col} contains element {@param elem}. Logs collection, element
     * and throws {@link java.lang.AssertionError}, if not.
     *
     * @param log Logger (optional).
     * @param col Collection.
     * @param elem Element.
     */
    public static <C extends Collection<T>, T> void assertContains(@Nullable IgniteLogger log, C col, T elem) {
        try {
            assertTrue(col.contains(elem));
        } catch (AssertionError e) {
            U.warn(log, String.format("Collection does not contain: '%s':", elem));
            U.warn(log, "Collection:");
            U.warn(log, col);

            throw e;
        }
    }

    /**
     * Checks that collection {@param col} doesn't contains element {@param str}. Logs collection, element
     * and throws {@link java.lang.AssertionError}, if contains.
     *
     * @param log Logger (optional).
     * @param col Collection.
     * @param elem Element.
     */
    public static <C extends Collection<T>, T> void assertNotContains(@Nullable IgniteLogger log, C col, T elem) {
        try {
            assertFalse(col.contains(elem));
        } catch (AssertionError e) {
            U.warn(log, String.format("Collection contain element: '%s' but shouldn't:", elem));
            U.warn(log, "Collection:");
            U.warn(log, col);

            throw e;
        }
    }

    /**
     * Checks whether runnable throws expected exception or not.
     *
     * @param log Logger (optional).
     * @param run Runnable.
     * @param cls Exception class.
     * @param msg Exception message (optional). Check that raised exception message contains this substring.
     * @return Thrown throwable.
     */
    public static <T extends Throwable> T assertThrows(
        @Nullable IgniteLogger log,
        RunnableX run,
        Class<? extends T> cls,
        @Nullable String msg
    ) {
        return assertThrows(log, () -> {
            run.run();

            return null;
        }, cls, msg);
    }

    /**
     * Checks whether callable throws expected exception or not.
     *
     * @param log Logger (optional).
     * @param call Callable.
     * @param cls Exception class.
     * @param msg Exception message (optional). If provided exception message
     *      and this message should be equal.
     * @return Thrown throwable.
     */
    public static <T extends Throwable> T assertThrows(
        @Nullable IgniteLogger log,
        Callable<?> call,
        Class<? extends T> cls,
        @Nullable String msg
    ) {
        assert call != null;
        assert cls != null;

        try {
            call.call();
        }
        catch (Throwable e) {
            if (cls != e.getClass() && !cls.isAssignableFrom(e.getClass())) {
                if (e.getClass() == CacheException.class && e.getCause() != null && e.getCause().getClass() == cls)
                    e = e.getCause();
                else {
                    U.error(log, "Unexpected exception.", e);

                    fail("Exception class is not as expected [expected=" + cls + ", actual=" + e.getClass() + ']', e);
                }
            }

            if (msg != null && (e.getMessage() == null || !e.getMessage().contains(msg))) {
                U.error(log, "Unexpected exception message.", e);

                fail("Exception message is not as expected [expected=" + msg + ", actual=" + e.getMessage() + ']', e);
            }

            if (log != null) {
                if (log.isInfoEnabled())
                    log.info("Caught expected exception: " + e.getMessage());
            }
            else
                X.println("Caught expected exception: " + e.getMessage());

            return (T) e;
        }

        throw new AssertionError("Exception has not been thrown.");
    }

    /**
     * Checks whether callable throws an exception with specified cause.
     *
     * @param log Logger (optional).
     * @param call Callable.
     * @param cls Exception class.
     * @param msg Exception message (optional). If provided exception message
     *      and this message should be equal.
     * @return Thrown throwable.
     */
    public static Throwable assertThrowsAnyCause(@Nullable IgniteLogger log, Callable<?> call,
        Class<? extends Throwable> cls, @Nullable String msg) {
        assert call != null;
        assert cls != null;

        try {
            call.call();
        }
        catch (Throwable e) {
            Throwable t = e;

            while (t != null) {
                if (cls == t.getClass() && (msg == null || (t.getMessage() != null && t.getMessage().contains(msg)))) {
                    if (log != null && log.isInfoEnabled())
                        log.info("Caught expected exception: " + t.getMessage());

                    return t;
                }

                t = t.getCause();
            }

            fail("Unexpected exception", e);
        }

        throw new AssertionError("Exception has not been thrown.");
    }

    /**
     * Checks whether callable throws expected exception or its child or not.
     *
     * @param log Logger (optional).
     * @param call Callable.
     * @param cls Exception class.
     * @param msg Exception message (optional). If provided exception message
     *      and this message should be equal.
     * @return Thrown throwable.
     */
    @Nullable public static Throwable assertThrowsInherited(@Nullable IgniteLogger log, Callable<?> call,
        Class<? extends Throwable> cls, @Nullable String msg) {
        assert call != null;
        assert cls != null;

        try {
            call.call();
        }
        catch (Throwable e) {
            if (!cls.isAssignableFrom(e.getClass()))
                fail("Exception class is not as expected [expected=" + cls + ", actual=" + e.getClass() + ']', e);

            if (msg != null && (e.getMessage() == null || !e.getMessage().startsWith(msg)))
                fail("Exception message is not as expected [expected=" + msg + ", actual=" + e.getMessage() + ']', e);

            if (log != null) {
                if (log.isDebugEnabled())
                    log.debug("Caught expected exception: " + e.getMessage());
            }
            else
                X.println("Caught expected exception: " + e.getMessage());

            return e;
        }

        throw new AssertionError("Exception has not been thrown.");
    }

    /**
     * Checks whether callable throws exception, which is itself of a specified
     * class, or has a cause of the specified class.
     *
     * @param runnable Runnable.
     * @param cls Expected class.
     * @return Thrown throwable.
     */
    @Nullable public static Throwable assertThrowsWithCause(Runnable runnable, Class<? extends Throwable> cls) {
        return assertThrowsWithCause(new Callable<Integer>() {
            @Override public Integer call() throws Exception {
                runnable.run();

                return 0;
            }
        }, cls);
    }

    /**
     * Checks whether callable throws exception, which is itself of a specified
     * class, or has a cause of the specified class.
     *
     * @param call Callable.
     * @param cls Expected class.
     * @return Thrown throwable.
     */
    @Nullable public static Throwable assertThrowsWithCause(Callable<?> call, Class<? extends Throwable> cls) {
        assert call != null;
        assert cls != null;

        try {
            call.call();
        }
        catch (Throwable e) {
            if (!X.hasCause(e, cls))
                fail("Exception is neither of a specified class, nor has a cause of the specified class: " + cls, e);

            return e;
        }

        throw new AssertionError("Exception has not been thrown.");
    }

    /**
     * Checks whether closure throws exception, which is itself of a specified
     * class, or has a cause of the specified class.
     *
     * @param call Closure.
     * @param p Parameter passed to closure.
     * @param cls Expected class.
     * @return Thrown throwable.
     */
    public static <P> Throwable assertThrowsWithCause(IgniteInClosure<P> call, P p, Class<? extends Throwable> cls) {
        assert call != null;
        assert cls != null;

        try {
            call.apply(p);
        }
        catch (Throwable e) {
            if (!X.hasCause(e, cls))
                fail("Exception is neither of a specified class, nor has a cause of the specified class: " + cls, e);

            return e;
        }

        throw new AssertionError("Exception has not been thrown.");
    }

    /**
     * Asserts that the specified runnable completes within the specified timeout.
     *
     * @param msg Assertion message in case of timeout.
     * @param timeout Timeout.
     * @param timeUnit Timeout {@link TimeUnit}.
     * @param runnable {@link Runnable} to check.
     * @throws Exception In case of any exception distinct from {@link TimeoutException}.
     */
    public static void assertTimeout(String msg, long timeout, TimeUnit timeUnit, Runnable runnable) throws Exception {
        ExecutorService executorSvc = Executors.newSingleThreadExecutor();
        Future<?> fut = executorSvc.submit(runnable);

        try {
            fut.get(timeout, timeUnit);
        }
        catch (TimeoutException ignored) {
            fail(msg, null);
        }
        finally {
            executorSvc.shutdownNow();
        }
    }

    /**
     * Asserts that the specified runnable completes within the specified timeout.
     *
     * @param timeout Timeout.
     * @param timeUnit Timeout {@link TimeUnit}.
     * @param runnable {@link Runnable} to check.
     * @throws Exception In case of any exception distinct from {@link TimeoutException}.
     */
    public static void assertTimeout(long timeout, TimeUnit timeUnit, Runnable runnable) throws Exception {
        assertTimeout("Timeout occurred.", timeout, timeUnit, runnable);
    }

    /**
     * Throw assertion error with specified error message and initialized cause.
     *
     * @param msg Error message.
     * @param cause Error cause.
     * @return Assertion error.
     */
    private static AssertionError fail(String msg, @Nullable Throwable cause) {
        AssertionError e = new AssertionError(msg);

        if (cause != null)
            e.initCause(cause);

        throw e;
    }

    /**
     * Checks whether object's method call throws expected exception or not.
     *
     * @param log Logger (optional).
     * @param cls Exception class.
     * @param msg Exception message (optional). If provided exception message
     *      and this message should be equal.
     * @param obj Object to invoke method for.
     * @param mtd Object's method to invoke.
     * @param params Method parameters.
     * @return Thrown throwable.
     */
    @Nullable public static Throwable assertThrows(@Nullable IgniteLogger log, Class<? extends Throwable> cls,
        @Nullable String msg, final Object obj, final String mtd, final Object... params) {
        return assertThrows(log, new Callable() {
            @Override public Object call() throws Exception {
                return invoke(obj, mtd, params);
            }
        }, cls, msg);
    }

    /**
     * Asserts that each element in iterable has one-to-one correspondence with a
     * predicate from list.
     *
     * @param it Input iterable of elements.
     * @param ps Array of predicates (by number of elements in iterable).
     */
    public static <T> void assertOneToOne(Iterable<T> it, IgnitePredicate<T>... ps) {
        Collection<IgnitePredicate<T>> ps0 = new ArrayList<>(Arrays.asList(ps));
        Collection<T2<IgnitePredicate<T>, T>> passed = new ArrayList<>();

        for (T elem : it) {
            for (T2<IgnitePredicate<T>, T> p : passed) {
                if (p.get1().apply(elem))
                    throw new AssertionError("Two elements match one predicate [elem1=" + p.get2() +
                        ", elem2=" + elem + ", pred=" + p.get1() + ']');
            }

            IgnitePredicate<T> matched = null;

            for (IgnitePredicate<T> p : ps0) {
                if (p.apply(elem)) {
                    if (matched != null)
                        throw new AssertionError("Element matches more than one predicate [elem=" + elem +
                            ", pred1=" + p + ", pred2=" + matched + ']');

                    matched = p;
                }
            }

            if (matched == null) // None matched.
                throw new AssertionError("The element does not match [elem=" + elem +
                    ", numRemainingPreds=" + ps0.size() + ']');

            ps0.remove(matched);
            passed.add(new T2<>(matched, elem));
        }
    }

    /**
     * Every invocation of this method will never return a
     * repeating multicast port for a different test case.
     *
     * @param cls Class.
     * @return Next multicast port.
     */
    public static synchronized int getNextMulticastPort(Class<? extends GridAbstractTest> cls) {
        Integer portRet = mcastPorts.get(cls);

        if (portRet != null)
            return portRet;

        int startPort = mcastPort;

        while (true) {
            if (mcastPort >= max_mcast_port)
                mcastPort = default_mcast_port;
            else
                mcastPort++;

            if (startPort == mcastPort)
                break;

            portRet = mcastPort;

            MulticastSocket sock = null;

            try {
                sock = new MulticastSocket(portRet);

                break;
            }
            catch (IOException ignored) {
                // No-op.
            }
            finally {
                U.closeQuiet(sock);
            }
        }

        // Cache port to be reused by the same test.
        mcastPorts.put(cls, portRet);

        return portRet;
    }

    /**
     * Every invocation of this method will never return a
     * repeating communication port for a different test case.
     *
     * @param cls Class.
     * @return Next communication port.
     */
    public static synchronized int getNextCommPort(Class<? extends GridAbstractTest> cls) {
        Integer portRet = commPorts.get(cls);

        if (portRet != null)
            return portRet;

        if (commPort >= max_comm_port)
            commPort = default_comm_port;
        else
            // Reserve 10 ports per test.
            commPort += 10;

        portRet = commPort;

        // Cache port to be reused by the same test.
        commPorts.put(cls, portRet);

        return portRet;
    }

    /**
     * Every invocation of this method will never return a
     * repeating discovery port for a different test case.
     *
     * @param cls Class.
     * @return Next discovery port.
     */
    public static synchronized int getNextDiscoPort(Class<? extends GridAbstractTest> cls) {
        Integer portRet = discoPorts.get(cls);

        if (portRet != null)
            return portRet;

        if (discoPort >= max_disco_port)
            discoPort = default_disco_port;
        else
            discoPort += 10;

        portRet = discoPort;

        // Cache port to be reused by the same test.
        discoPorts.put(cls, portRet);

        return portRet;
    }

    /**
     * @return Free communication port number on localhost.
     * @throws IOException If unable to find a free port.
     */
    public static int getFreeCommPort() throws IOException {
        for (int port = default_comm_port; port < max_comm_port; port++) {
            try (ServerSocket sock = new ServerSocket(port)) {
                return sock.getLocalPort();
            }
            catch (IOException ignored) {
                // No-op.
            }
        }

        throw new IOException("Unable to find a free communication port.");
    }

    /**
     * Every invocation of this method will never return a
     * repeating multicast group for a different test case.
     *
     * @param cls Class.
     * @return Next multicast group.
     */
    public static synchronized String getNextMulticastGroup(Class<?> cls) {
        String addrStr = addrs.get(cls);

        if (addrStr != null)
            return addrStr;

        // Increment address.
        if (addr[3] == 255) {
            if (addr[2] == 255)
                assert false;
            else {
                addr[2] += 1;

                addr[3] = 1;
            }
        }
        else
            addr[3] += 1;

        // Convert address to string.
        StringBuilder b = new StringBuilder(15);

        for (int i = 0; i < addr.length; i++) {
            b.append(addr[i]);

            if (i < addr.length - 1)
                b.append('.');
        }

        addrStr = b.toString();

        // Cache address to be reused by the same test.
        addrs.put(cls, addrStr);

        return addrStr;
    }

    /**
     * Runs runnable object in specified number of threads.
     *
     * @param run Target runnable.
     * @param threadNum Number of threads.
     * @param threadName Thread name.
     * @return Execution time in milliseconds.
     * @throws Exception Thrown if at least one runnable execution failed.
     */
    public static long runMultiThreaded(Runnable run, int threadNum, String threadName) throws Exception {
        return runMultiThreaded(makeCallable(run, null), threadNum, threadName);
    }

    /**
     * Runs runnable object in specified number of threads.
     *
     * @param run Target runnable.
     * @param threadNum Number of threads.
     * @param threadName Thread name.
     * @return Future for the run. Future returns execution time in milliseconds.
     */
    public static IgniteInternalFuture<Long> runMultiThreadedAsync(Runnable run, int threadNum, String threadName) {
        return runMultiThreadedAsync(makeCallable(run, null), threadNum, threadName);
    }

    /**
     * Runs callable object in specified number of threads.
     *
     * @param call Callable.
     * @param threadNum Number of threads.
     * @param threadName Thread names.
     * @return Execution time in milliseconds.
     * @throws Exception If failed.
     */
    public static long runMultiThreaded(Callable<?> call, int threadNum, String threadName) throws Exception {
        List<Callable<?>> calls = Collections.<Callable<?>>nCopies(threadNum, call);

        return runMultiThreaded(calls, threadName);
    }

    /**
     * @param call Closure that receives thread index.
     * @param threadNum Number of threads.
     * @param threadName Thread names.
     * @return Execution time in milliseconds.
     * @throws Exception If failed.
     */
    public static long runMultiThreaded(final IgniteInClosure<Integer> call, int threadNum, String threadName)
        throws Exception {
        List<Callable<?>> calls = new ArrayList<>(threadNum);

        for (int i = 0; i < threadNum; i++) {
            final int idx = i;

            calls.add(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    call.apply(idx);

                    return null;
                }
            });
        }

        return runMultiThreaded(calls, threadName);
    }

    /**
     * Runs callable object in specified number of threads.
     *
     * @param call Callable.
     * @param threadNum Number of threads.
     * @param threadName Thread names.
     * @return Future for the run. Future returns execution time in milliseconds.
     */
    public static IgniteInternalFuture<Long> runMultiThreadedAsync(Callable<?> call, int threadNum, final String threadName) {
        final List<Callable<?>> calls = Collections.<Callable<?>>nCopies(threadNum, call);
        final GridTestSafeThreadFactory threadFactory = new GridTestSafeThreadFactory(threadName);

        IgniteInternalFuture<Long> runFut = runAsync(() -> runMultiThreaded(calls, threadFactory));

        GridFutureAdapter<Long> resFut = new GridFutureAdapter<Long>() {
            @Override public boolean cancel() throws IgniteCheckedException {
                super.cancel();

                if (isDone())
                    return false;

                runFut.cancel();

                threadFactory.interruptAllThreads();

                return onCancelled();
            }
        };

        runFut.listen(fut -> {
            try {
                resFut.onDone(fut.get());
            }
            catch (IgniteFutureCancelledCheckedException e) {
                resFut.onCancelled();
            }
            catch (Throwable e) {
                resFut.onDone(e);
            }
        });

        return resFut;
    }

    /**
     * Runs callable tasks each in separate threads.
     *
     * @param calls Callable tasks.
     * @param threadName Thread name.
     * @return Execution time in milliseconds.
     * @throws Exception If failed.
     */
    public static long runMultiThreaded(Iterable<Callable<?>> calls, String threadName) throws Exception {
        return runMultiThreaded(calls, new GridTestSafeThreadFactory(threadName));
    }

    /**
     * Runs callable tasks each in separate threads.
     *
     * @param calls Callable tasks.
     * @param threadFactory Thread factory.
     * @return Execution time in milliseconds.
     * @throws Exception If failed.
     */
    public static long runMultiThreaded(Iterable<Callable<?>> calls, GridTestSafeThreadFactory threadFactory)
        throws Exception {
        if (!busyLock.enterBusy())
            throw new IllegalStateException("Failed to start new threads (test is being stopped).");

        Collection<Thread> threads = new ArrayList<>();
        long time;

        try {
            for (Callable<?> call : calls)
                threads.add(threadFactory.newThread(call));

            time = System.currentTimeMillis();

            for (Thread t : threads)
                t.start();
        }
        finally {
            busyLock.leaveBusy();
        }

        // Wait threads finish their job.
        try {
            for (Thread t : threads)
                t.join();
        } catch (InterruptedException e) {
            for (Thread t : threads)
                t.interrupt();

            throw e;
        }

        time = System.currentTimeMillis() - time;

        // Validate errors happens
        threadFactory.checkError();

        return time;
    }

    /**
     * Runs runnable task asyncronously.
     *
     * @param task Runnable.
     * @return Future with task result.
     */
    public static IgniteInternalFuture runAsync(final Runnable task) {
        return runAsync(task,"async-runnable-runner");
    }

    /**
     * Runs runnable task asyncronously.
     *
     * @param task Runnable.
     * @return Future with task result.
     */
    public static IgniteInternalFuture runAsync(final RunnableX task) {
        return runAsync(task,"async-runnable-runner");
    }

    /**
     * Runs runnable task asyncronously.
     *
     * @param task Runnable.
     * @return Future with task result.
     */
    public static IgniteInternalFuture runAsync(final Runnable task, String threadName) {
        return runAsync(() -> {
            task.run();

            return null;
        }, threadName);
    }

    /**
     * Runs runnable task asyncronously.
     *
     * @param task Runnable.
     * @return Future with task result.
     */
    public static IgniteInternalFuture runAsync(final RunnableX task, String threadName) {
        return runAsync(() -> {
            task.run();

            return null;
        }, threadName);
    }

    /**
     * Runs callable task asyncronously.
     *
     * @param task Callable.
     * @return Future with task result.
     */
    public static <T> IgniteInternalFuture<T> runAsync(final Callable<T> task) {
        return runAsync(task, "async-callable-runner");
    }

    /**
     * Runs callable task asyncronously.
     *
     * @param task Callable.
     * @param threadName Thread name.
     * @return Future with task result.
     */
    public static <T> IgniteInternalFuture<T> runAsync(final Callable<T> task, String threadName) {
        if (!busyLock.enterBusy())
            throw new IllegalStateException("Failed to start new threads (test is being stopped).");

        try {
            final GridTestSafeThreadFactory thrFactory = new GridTestSafeThreadFactory(threadName);

            final GridFutureAdapter<T> fut = new GridFutureAdapter<T>() {
                @Override public boolean cancel() throws IgniteCheckedException {
                    super.cancel();

                    if (isDone())
                        return false;

                    thrFactory.interruptAllThreads();

                    try {
                        get();

                        return false;
                    }
                    catch (IgniteFutureCancelledCheckedException e) {
                        return true;
                    }
                    catch (IgniteCheckedException e) {
                        return false;
                    }
                }
            };

            thrFactory.newThread(() -> {
                try {
                    // Execute task.
                    T res = task.call();

                    fut.onDone(res);
                }
                catch (InterruptedException e) {
                    fut.onCancelled();
                }
                catch (Throwable e) {
                    fut.onDone(e);
                }
            }).start();

            return fut;
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Wait for all passed futures to complete even if they fail.
     *
     * @param futs Futures.
     * @throws AssertionError Suppresses underlying exceptions if some futures failed.
     */
    public static void waitForAllFutures(IgniteInternalFuture<?>... futs) {
        AssertionError err = null;

        for (IgniteInternalFuture<?> fut : futs) {
            try {
                fut.get();
            }
            catch (Throwable t) {
                if (err == null)
                    err = new AssertionError("One or several futures threw the exception.");

                err.addSuppressed(t);
            }
        }

        if (err != null)
            throw err;
    }

    /**
     * Interrupts and waits for termination of all the threads started
     * so far by current test.
     *
     * @param log Logger.
     */
    public static void stopThreads(IgniteLogger log) {
        busyLock.block();

        try {
            GridTestSafeThreadFactory.stopAllThreads(log);
        }
        finally {
            busyLock.unblock();
        }
    }

    /**
     * @return Ignite home.
     * @throws Exception If failed.
     */
    @SuppressWarnings({"ProhibitedExceptionThrown"})
    public static String getIgniteHome() throws Exception {
        String ggHome = System.getProperty("IGNITE_HOME");

        if (ggHome == null)
            ggHome = System.getenv("IGNITE_HOME");

        if (ggHome == null)
            throw new Exception("IGNITE_HOME parameter must be set either as system or environment variable.");

        File dir = new File(ggHome);

        if (!dir.exists())
            throw new Exception("Ignite home does not exist [ignite-home=" + dir.getAbsolutePath() + ']');

        if (!dir.isDirectory())
            throw new Exception("Ignite home is not a directory [ignite-home=" + dir.getAbsolutePath() + ']');

        return ggHome;
    }

    /**
     * @param <T> Type.
     * @param cls Class.
     * @param annCls Annotation class.
     * @return Annotation.
     */
    @Nullable public static <T extends Annotation> T getAnnotation(Class<?> cls, Class<T> annCls) {
        for (Class<?> cls0 = cls; cls0 != null; cls0 = cls0.getSuperclass()) {
            T ann = cls0.getAnnotation(annCls);

            if (ann != null)
                return ann;
        }

        return null;
    }

    /**
     * Initializes address.
     */
    static {
        InetAddress locHost = null;

        try {
            locHost = U.getLocalHost();
        }
        catch (IOException e) {
            assert false : "Unable to get local address. This leads to the same multicast addresses " +
                "in the local network.";
        }

        if (locHost != null) {
            int thirdByte = locHost.getAddress()[3];

            if (thirdByte < 0)
                thirdByte += 256;

            // To get different addresses for different machines.
            addr = new int[] {229, thirdByte, 1, 1};
        }
        else
            addr = new int[] {229, 1, 1, 1};
    }

    /**
     * @param path Path.
     * @param startFilter Start filter.
     * @param endFilter End filter.
     * @return List of JARs that corresponds to the filters.
     * @throws IOException If failed.
     */
    private static Collection<String> getFiles(String path, @Nullable final String startFilter,
        @Nullable final String endFilter) throws IOException {
        Collection<String> res = new ArrayList<>();

        File file = new File(path);

        assert file.isDirectory();

        File[] jars = file.listFiles(new FilenameFilter() {
            /**
             * @see FilenameFilter#accept(File, String)
             */
            @SuppressWarnings({"UnnecessaryJavaDocLink"})
            @Override public boolean accept(File dir, String name) {
                // Exclude spring.jar because it tries to load META-INF/spring-handlers.xml from
                // all available JARs and create instances of classes from there for example.
                // Exclude logging as it is used by spring and casted to Log interface.
                // Exclude log4j because of the design - 1 per VM.
                if (name.startsWith("spring") || name.startsWith("log4j") ||
                    name.startsWith("commons-logging") || name.startsWith("junit") ||
                    name.startsWith("ignite-tests"))
                    return false;

                boolean ret = true;

                if (startFilter != null)
                    ret = name.startsWith(startFilter);

                if (ret && endFilter != null)
                    ret = name.endsWith(endFilter);

                return ret;
            }
        });

        for (File jar : jars)
            res.add(jar.getCanonicalPath());

        return res;
    }

    /**
     * Silent stop grid.
     * Method doesn't throw any exception.
     *
     * @param ignite Grid to stop.
     * @param log Logger.
     */
    public static void close(Ignite ignite, IgniteLogger log) {
        if (ignite != null)
            try {
                G.stop(ignite.name(), false);
            }
            catch (Throwable e) {
                U.error(log, "Failed to stop grid: " + ignite.name(), e);
            }
    }

    /**
     * Silent stop grid.
     * Method doesn't throw any exception.
     *
     * @param igniteInstanceName Ignite instance name.
     * @param log Logger.
     */
    public static void stopGrid(String igniteInstanceName, IgniteLogger log) {
        try {
            G.stop(igniteInstanceName, false);
        }
        catch (Throwable e) {
            U.error(log, "Failed to stop grid: " + igniteInstanceName, e);
        }
    }

    /**
     * Gets file representing the path passed in. First the check is made if path is absolute.
     * If not, then the check is made if path is relative to ${IGNITE_HOME}. If both checks fail,
     * then {@code null} is returned, otherwise file representing path is returned.
     * <p>
     * See {@link #getIgniteHome()} for information on how {@code IGNITE_HOME} is retrieved.
     *
     * @param path Path to resolve.
     * @return Resolved path, or {@code null} if file cannot be resolved.
     * @see #getIgniteHome()
     */
    @Nullable public static File resolveIgnitePath(String path) {
        return resolvePath(null, path);
    }

    /**
     * @param igniteHome Optional ignite home path.
     * @param path Path to resolve.
     * @return Resolved path, or {@code null} if file cannot be resolved.
     */
    @Nullable private static File resolvePath(@Nullable String igniteHome, String path) {
        File file = new File(path).getAbsoluteFile();

        if (!file.exists()) {
            String home = igniteHome != null ? igniteHome : U.getIgniteHome();

            if (home == null)
                return null;

            file = new File(home, path);

            return file.exists() ? file : null;
        }

        return file;
    }

    /**
     * @param cache Cache.
     * @return Cache context.
     */
    public static <K, V> GridCacheContext<K, V> cacheContext(IgniteCache<K, V> cache) {
        return ((IgniteKernal)cache.unwrap(Ignite.class)).<K, V>internalCache(cache.getName()).context();
    }

    /**
     * @param cache Cache.
     * @return Near cache.
     */
    public static <K, V> GridNearCacheAdapter<K, V> near(IgniteCache<K, V> cache) {
        return cacheContext(cache).near();
    }

    /**
     * @param ig Ignite node to extract {@link IgniteWriteAheadLogManager} from.
     * @return Instance of {@link IgniteWriteAheadLogManager} associated with the given Ignite node.
     */
    public static IgniteWriteAheadLogManager wal(IgniteEx ig) {
        return ig.context().cache().context().wal();
    }

    /**
     * @param ig Ignite node to extract {@link IgniteCacheDatabaseSharedManager} from.
     * @@return Instance of {@link IgniteCacheDatabaseSharedManager} associated with the given Ignite node.
     */
    public static IgniteCacheDatabaseSharedManager database(IgniteEx ig) {
        return ig.context().cache().context().database();
    }

    /**
     * @param cache Cache.
     * @return DHT cache.
     */
    public static <K, V> GridDhtCacheAdapter<K, V> dht(IgniteCache<K, V> cache) {
        return near(cache).dht();
    }

    /**
     * @param cacheName Cache name.
     * @param backups Number of backups.
     * @param log Logger.
     * @throws Exception If failed.
     */
    @SuppressWarnings("BusyWait")
    public static <K, V> void waitTopologyUpdate(@Nullable String cacheName, int backups, IgniteLogger log)
        throws Exception {
        for (Ignite g : Ignition.allGrids()) {
            IgniteCache<K, V> cache = ((IgniteEx)g).cache(cacheName);

            GridDhtPartitionTopology top = dht(cache).topology();

            while (true) {
                boolean wait = false;

                for (int p = 0; p < g.affinity(cacheName).partitions(); p++) {
                    Collection<ClusterNode> nodes = top.nodes(p, AffinityTopologyVersion.NONE);

                    if (nodes.size() > backups + 1) {
                        LT.warn(log, "Partition map was not updated yet (will wait) [igniteInstanceName=" + g.name() +
                            ", p=" + p + ", nodes=" + F.nodeIds(nodes) + ']');

                        wait = true;

                        break;
                    }
                }

                if (wait)
                    Thread.sleep(20);
                else
                    break; // While.
            }
        }
    }

    /**
     * Convert runnable tasks with callable.
     *
     * @param run Runnable task to convert into callable one.
     * @param res Callable result.
     * @param <T> The result type of method <tt>call</tt>, always {@code null}.
     * @return Callable task around the specified runnable one.
     */
    public static <T> Callable<T> makeCallable(final Runnable run, @Nullable final T res) {
        return new Callable<T>() {
            @Override public T call() throws Exception {
                run.run();
                return res;
            }
        };
    }

    /**
     * Get object field value via reflection.
     *
     * @param obj Object or class to get field value from.
     * @param cls Class.
     * @param fieldName Field names to get value for.
     * @param <T> Expected field class.
     * @return Field value.
     * @throws IgniteException In case of error.
     */
    public static <T> T getFieldValue(Object obj, Class cls, String fieldName) throws IgniteException {
        assert obj != null;
        assert fieldName != null;

        try {
            return (T)findField(cls, obj, fieldName);
        }
        catch (NoSuchFieldException | IllegalAccessException e) {
            throw new IgniteException("Failed to get object field [obj=" + obj +
                ", fieldName=" + fieldName + ']', e);
        }
    }

    /**
     * Get object field value via reflection.
     *
     * @param obj Object or class to get field value from.
     * @param fieldNames Field names to get value for: obj->field1->field2->...->fieldN.
     * @param <T> Expected field class.
     * @return Field value.
     * @throws IgniteException In case of error.
     */
    public static <T> T getFieldValue(Object obj, String... fieldNames) throws IgniteException {
        assert obj != null;
        assert fieldNames != null;
        assert fieldNames.length >= 1;

        try {
            for (String fieldName : fieldNames) {
                Class<?> cls = obj instanceof Class ? (Class)obj : obj.getClass();

                try {
                    obj = findField(cls, obj, fieldName);
                }
                catch (NoSuchFieldException e) {
                    // Resolve inner class, if not an inner field.
                    Class<?> innerCls = getInnerClass(cls, fieldName);

                    if (innerCls == null)
                        throw new IgniteException("Failed to get object field [obj=" + obj +
                            ", fieldNames=" + Arrays.toString(fieldNames) + ']', e);

                    obj = innerCls;
                }
            }

            return (T)obj;
        }
        catch (IllegalAccessException e) {
            throw new IgniteException("Failed to get object field [obj=" + obj +
                ", fieldNames=" + Arrays.toString(fieldNames) + ']', e);
        }
    }

    /**
     * Get object field value via reflection(including superclass).
     *
     * @param obj Object or class to get field value from.
     * @param fieldNames Field names to get value for: obj->field1->field2->...->fieldN.
     * @param <T> Expected field class.
     * @return Field value.
     * @throws IgniteException In case of error.
     */
    public static <T> T getFieldValueHierarchy(Object obj, String... fieldNames) throws IgniteException {
        assert obj != null;
        assert fieldNames != null;
        assert fieldNames.length >= 1;

        try {
            for (String fieldName : fieldNames) {
                Class<?> cls = obj instanceof Class ? (Class)obj : obj.getClass();

                while (cls != null) {
                    try {
                        obj = findField(cls, obj, fieldName);

                        break;
                    }
                    catch (NoSuchFieldException e) {
                        cls = cls.getSuperclass();
                    }
                }
            }

            return (T)obj;
        }
        catch (IllegalAccessException e) {
            throw new IgniteException("Failed to get object field [obj=" + obj +
                ", fieldNames=" + Arrays.toString(fieldNames) + ']', e);
        }
    }

    /**
     * @param cls Class for searching.
     * @param obj Target object.
     * @param fieldName Field name for search.
     * @return Field from object if it was found.
     */
    private static Object findField(Class<?> cls, Object obj,
        String fieldName) throws NoSuchFieldException, IllegalAccessException {
        // Resolve inner field.
        Field field = cls.getDeclaredField(fieldName);

        boolean accessible = field.isAccessible();

        if (!accessible)
            field.setAccessible(true);

        return field.get(obj);
    }

    /**
     * Get inner class by its name from the enclosing class.
     *
     * @param parentCls Parent class to resolve inner class for.
     * @param innerClsName Name of the inner class.
     * @return Inner class.
     */
    @Nullable public static <T> Class<T> getInnerClass(Class<?> parentCls, String innerClsName) {
        for (Class<?> cls : parentCls.getDeclaredClasses())
            if (innerClsName.equals(cls.getSimpleName()))
                return (Class<T>)cls;

        return null;
    }

    /**
     * Set object field value via reflection.
     *
     * @param obj Object to set field value to.
     * @param fieldName Field name to set value for.
     * @param val New field value.
     * @throws IgniteException In case of error.
     */
    public static void setFieldValue(Object obj, String fieldName, Object val) throws IgniteException {
        assert obj != null;
        assert fieldName != null;

        try {
            Class<?> cls = obj instanceof Class ? (Class)obj : obj.getClass();

            Field field = cls.getDeclaredField(fieldName);

            boolean isFinal = (field.getModifiers() & Modifier.FINAL) != 0;

            boolean isStatic = (field.getModifiers() & Modifier.STATIC) != 0;

            /**
             * http://java.sun.com/docs/books/jls/third_edition/html/memory.html#17.5.3
             * If a final field is initialized to a compile-time constant in the field declaration,
             *   changes to the final field may not be observed.
             */
            if (isFinal && isStatic)
                throw new IgniteException("Modification of static final field through reflection.");

            boolean accessible = field.isAccessible();

            if (!accessible)
                field.setAccessible(true);

            field.set(obj, val);
        }
        catch (NoSuchFieldException | IllegalAccessException e) {
            throw new IgniteException("Failed to set object field [obj=" + obj + ", field=" + fieldName + ']', e);
        }
    }

    /**
     * Set object field value via reflection.
     *
     * @param obj Object to set field value to.
     * @param cls Class to get field from.
     * @param fieldName Field name to set value for.
     * @param val New field value.
     * @throws IgniteException In case of error.
     */
    public static void setFieldValue(Object obj, Class cls, String fieldName, Object val) throws IgniteException {
        assert fieldName != null;

        try {
            Field field = cls.getDeclaredField(fieldName);

            boolean accessible = field.isAccessible();

            if (!accessible)
                field.setAccessible(true);

            boolean isFinal = (field.getModifiers() & Modifier.FINAL) != 0;

            boolean isStatic = (field.getModifiers() & Modifier.STATIC) != 0;

            /**
             * http://java.sun.com/docs/books/jls/third_edition/html/memory.html#17.5.3
             * If a final field is initialized to a compile-time constant in the field declaration,
             *   changes to the final field may not be observed.
             */
            if (isFinal && isStatic)
                throw new IgniteException("Modification of static final field through reflection.");

            if (isFinal) {
                Field modifiersField = Field.class.getDeclaredField("modifiers");

                modifiersField.setAccessible(true);

                modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
            }

            field.set(obj, val);
        }
        catch (NoSuchFieldException | IllegalAccessException e) {
            throw new IgniteException("Failed to set object field [obj=" + obj + ", field=" + fieldName + ']', e);
        }
    }

    /**
     * Invoke method on an object.
     *
     * @param obj Object to call method on.
     * @param mtd Method to invoke.
     * @param params Parameters of the method.
     * @return Method invocation result.
     * @throws Exception If failed.
     */
    @Nullable public static <T> T invoke(Object obj, String mtd, Object... params) throws Exception {
        Class<?> cls = obj.getClass();

        do {
            // We cannot resolve method by parameter classes due to some of parameters can be null.
            // Search correct method among all methods collection.
            for (Method m : cls.getDeclaredMethods()) {
                // Filter methods by name.
                if (!m.getName().equals(mtd))
                    continue;

                if (!areCompatible(params, m.getParameterTypes()))
                    continue;

                try {
                    boolean accessible = m.isAccessible();

                    if (!accessible)
                        m.setAccessible(true);

                    return (T)m.invoke(obj, params);
                }
                catch (IllegalAccessException e) {
                    throw new RuntimeException("Failed to access method" +
                        " [obj=" + obj + ", mtd=" + mtd + ", params=" + Arrays.toString(params) + ']', e);
                }
                catch (InvocationTargetException e) {
                    Throwable cause = e.getCause();

                    if (cause instanceof Error)
                        throw (Error) cause;

                    if (cause instanceof Exception)
                        throw (Exception) cause;

                    throw new RuntimeException("Failed to invoke method)" +
                        " [obj=" + obj + ", mtd=" + mtd + ", params=" + Arrays.toString(params) + ']', e);
                }
            }

            cls = cls.getSuperclass();
        } while (cls != Object.class);

        throw new RuntimeException("Failed to find method" +
            " [obj=" + obj + ", mtd=" + mtd + ", params=" + Arrays.toString(params) + ']');
    }

    /**
     * Check objects and corresponding types are compatible.
     *
     * @param objs Objects array.
     * @param types Classes array.
     * @return Objects in array can be casted to corresponding types.
     */
    private static boolean areCompatible(Object[] objs, Class[] types) {
        if (objs.length != types.length)
            return false;

        for (int i = 0, size = objs.length; i < size; i++) {
            Object o = objs[i];

            if (o != null && !IgniteUtils.box(types[i]).isInstance(o))
                return false;
        }

        return true;
    }

    /**
     * Tries few times to perform some assertion. In the worst case
     * {@code assertion} closure will be executed {@code retries} + 1 times and
     * thread will spend approximately {@code retries} * {@code retryInterval} sleeping.
     *
     * @param log Log.
     * @param retries Number of retries.
     * @param retryInterval Interval between retries in milliseconds.
     * @param c Closure with assertion. All {@link AssertionError}s thrown
     *      from this closure will be ignored {@code retries} times.
     * @throws org.apache.ignite.internal.IgniteInterruptedCheckedException If interrupted.
     */
    @SuppressWarnings("ErrorNotRethrown")
    public static void retryAssert(@Nullable IgniteLogger log, int retries, long retryInterval, GridAbsClosure c)
        throws IgniteInterruptedCheckedException {
        for (int i = 0; i < retries; i++) {
            try {
                c.apply();

                return;
            }
            catch (AssertionError e) {
                U.warn(log, "Check failed (will retry in " + retryInterval + "ms).", e);

                U.sleep(retryInterval);
            }
        }

        // Apply the last time without guarding try.
        c.apply();
    }

    /**
     * Reads entire file into byte array.
     *
     * @param file File to read.
     * @return Content of file in byte array.
     * @throws IOException If failed.
     */
    public static byte[] readFile(File file) throws IOException {
        assert file.exists();
        assert file.length() < Integer.MAX_VALUE;

        byte[] bytes = new byte[(int) file.length()];

        try (FileInputStream fis = new FileInputStream(file)) {
            int readBytesCnt = fis.read(bytes);
            assert readBytesCnt == bytes.length;
        }

        return bytes;
    }

    /**
     * Clear file without deletion.
     *
     * @param path to file.
     */
    public static void clearFile(Path path) throws IOException {
        Files.newOutputStream(path).close();
    }

    /**
     * Reads resource into byte array.
     *
     * @param classLoader Classloader.
     * @param resourceName Resource name.
     * @return Content of resorce in byte array.
     * @throws IOException If failed.
     */
    public static byte[] readResource(ClassLoader classLoader, String resourceName) throws IOException {
        try (InputStream is = classLoader.getResourceAsStream(resourceName)) {
            assertNotNull("Resource is missing: " + resourceName, is);

            try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
                U.copy(is, baos);

                return baos.toByteArray();
            }
        }
    }

    /**
     * Sleeps and increments an integer.
     * <p>
     * Allows for loops like the following:
     * <pre>{@code
     *     for (int i = 0; i < 20 && !condition; i = sleepAndIncrement(200, i)) {
     *         ...
     *     }
     * }</pre>
     * for busy-waiting limited number of iterations.
     *
     * @param sleepDur Sleep duration in milliseconds.
     * @param i Integer to increment.
     * @return Incremented value.
     * @throws org.apache.ignite.internal.IgniteInterruptedCheckedException If sleep was interrupted.
     */
    public static int sleepAndIncrement(int sleepDur, int i) throws IgniteInterruptedCheckedException {
        U.sleep(sleepDur);

        return i + 1;
    }

    /**
     * Waits for condition, polling in busy wait loop.
     *
     * @param cond Condition to wait for.
     * @param timeout Max time to wait in milliseconds.
     * @param checkInterval Time interval between two consecutive condition checks.
     * @return {@code true} if condition was achieved, {@code false} otherwise.
     * @throws org.apache.ignite.internal.IgniteInterruptedCheckedException If interrupted.
     */
    public static boolean waitForCondition(GridAbsPredicate cond, long timeout, long checkInterval) throws IgniteInterruptedCheckedException {
        long curTime = U.currentTimeMillis();
        long endTime = curTime + timeout;

        if (endTime < 0)
            endTime = Long.MAX_VALUE;

        while (curTime < endTime) {
            if (cond.apply())
                return true;

            if (checkInterval > 0)
                U.sleep(checkInterval);

            curTime = U.currentTimeMillis();
        }

        return false;
    }

    /**
     * Waits for condition, polling in busy wait loop.
     *
     * @param cond Condition to wait for.
     * @param timeout Max time to wait in milliseconds.
     * @return {@code true} if condition was achieved, {@code false} otherwise.
     * @throws org.apache.ignite.internal.IgniteInterruptedCheckedException If interrupted.
     */
    public static boolean waitForCondition(GridAbsPredicate cond, long timeout) throws IgniteInterruptedCheckedException {
        return waitForCondition(cond, timeout, DFLT_BUSYWAIT_SLEEP_INTERVAL);
    }

    /**
     * Creates an SSL context from test key store with disabled trust manager.
     *
     * @return Initialized context.
     * @throws GeneralSecurityException In case if context could not be initialized.
     * @throws IOException If keystore cannot be accessed.
     */
    public static SSLContext sslContext() throws GeneralSecurityException, IOException {
        SSLContext ctx = SSLContext.getInstance(DFLT_SSL_PROTOCOL);

        char[] storePass = keyStorePassword().toCharArray();

        KeyManagerFactory keyMgrFactory = KeyManagerFactory.getInstance(DFLT_KEY_ALGORITHM);

        KeyStore keyStore = KeyStore.getInstance(DFLT_STORE_TYPE);

        try (InputStream stream = Files.newInputStream(U.resolveIgnitePath(getProperty("ssl.keystore.path")).toPath())) {
            keyStore.load(stream, storePass);
        }

        keyMgrFactory.init(keyStore, storePass);

        ctx.init(keyMgrFactory.getKeyManagers(),
            new TrustManager[]{GridSslBasicContextFactory.getDisabledTrustManager()}, null);

        return ctx;
    }

    /**
     * Creates test-purposed SSL context factory from test key store with disabled trust manager.
     *
     * @return SSL context factory used in test.
     */
    public static GridSslContextFactory sslContextFactory() {
        GridSslBasicContextFactory factory = new GridSslBasicContextFactory();

        factory.setKeyStoreFilePath(U.resolveIgnitePath(getProperty("ssl.keystore.path")).getAbsolutePath());
        factory.setKeyStorePassword(keyStorePassword().toCharArray());

        factory.setTrustManagers(GridSslBasicContextFactory.getDisabledTrustManager());

        return factory;
    }

    /**
     * Creates test-purposed SSL context factory from test key store with disabled trust manager.
     *
     * @return SSL context factory used in test.
     */
    public static Factory<SSLContext> sslFactory() {
        SslContextFactory factory = new SslContextFactory();

        factory.setKeyStoreFilePath(U.resolveIgnitePath(getProperty("ssl.keystore.path")).getAbsolutePath());
        factory.setKeyStorePassword(keyStorePassword().toCharArray());

        factory.setTrustManagers(SslContextFactory.getDisabledTrustManager());

        return factory;
    }

    /**
     * Creates test-purposed SSL context factory from specified key store and trust store.
     *
     * @param keyStore Key store name.
     * @param trustStore Trust store name.
     * @return SSL context factory used in test.
     */
    public static SslContextFactory sslTrustedFactory(String keyStore, String trustStore) {
        SslContextFactory factory = new SslContextFactory();

        if (keyStore != null) {
            factory.setKeyStoreFilePath(keyStorePath(keyStore));
            factory.setKeyStorePassword(keyStorePassword().toCharArray());
        }

        if (trustStore != null) {
            factory.setTrustStoreFilePath(keyStorePath(trustStore));
            factory.setTrustStorePassword(keyStorePassword().toCharArray());
        }

        return factory;
    }

    /**
     * Creates test-purposed SSL context factory from specified key store and trust store.
     *
     * @param keyStore Key store name.
     * @param trustStore Trust store name.
     * @return SSL context factory used in test.
     */
    public static GridSslBasicContextFactory gridSslTrustedFactory(String keyStore, String trustStore) {
        GridSslBasicContextFactory factory = new GridSslBasicContextFactory();

        if (keyStore != null) {
            factory.setKeyStoreFilePath(keyStorePath(keyStore));
            factory.setKeyStorePassword(keyStorePassword().toCharArray());
        }

        if (trustStore != null) {
            factory.setTrustStoreFilePath(keyStorePath(trustStore));
            factory.setTrustStorePassword(keyStorePassword().toCharArray());
        }

        return factory;
    }

    public static String keyStorePassword() {
        return getProperty("ssl.keystore.password");
    }

    @NotNull public static String keyStorePath(String keyStore) {
        return U.resolveIgnitePath(getProperty("ssl.keystore." + keyStore + ".path")).getAbsolutePath();
    }

    /**
     * @param o1 Object 1.
     * @param o2 Object 2.
     * @return Equals or not.
     */
    public static boolean deepEquals(@Nullable Object o1, @Nullable Object o2) {
        if (o1 == o2)
            return true;
        else if (o1 == null || o2 == null)
            return false;
        else if (o1.getClass() != o2.getClass())
            return false;
        else {
            Class<?> cls = o1.getClass();

            assert o2.getClass() == cls;

            for (Field f : cls.getDeclaredFields()) {
                f.setAccessible(true);

                Object v1;
                Object v2;

                try {
                    v1 = f.get(o1);
                    v2 = f.get(o2);
                }
                catch (IllegalAccessException e) {
                    throw new AssertionError(e);
                }

                if (!Objects.deepEquals(v1, v2))
                    return false;
            }

            return true;
        }
    }

    /**
     * Converts integer permission mode into set of {@link PosixFilePermission}.
     *
     * @param mode File mode.
     * @return Set of {@link PosixFilePermission}.
     */
    public static Set<PosixFilePermission> modeToPermissionSet(int mode) {
        Set<PosixFilePermission> res = EnumSet.noneOf(PosixFilePermission.class);

        if ((mode & 0400) > 0)
            res.add(PosixFilePermission.OWNER_READ);

        if ((mode & 0200) > 0)
            res.add(PosixFilePermission.OWNER_WRITE);

        if ((mode & 0100) > 0)
            res.add(PosixFilePermission.OWNER_EXECUTE);

        if ((mode & 040) > 0)
            res.add(PosixFilePermission.GROUP_READ);

        if ((mode & 020) > 0)
            res.add(PosixFilePermission.GROUP_WRITE);

        if ((mode & 010) > 0)
            res.add(PosixFilePermission.GROUP_EXECUTE);

        if ((mode & 04) > 0)
            res.add(PosixFilePermission.OTHERS_READ);

        if ((mode & 02) > 0)
            res.add(PosixFilePermission.OTHERS_WRITE);

        if ((mode & 01) > 0)
            res.add(PosixFilePermission.OTHERS_EXECUTE);

        return res;
    }

    /**
     * @param name Name.
     * @param run Run.
     */
    public static void benchmark(@Nullable String name, @NotNull Runnable run) {
        benchmark(name, 8000, 10000, run);
    }

    /**
     * @param name Name.
     * @param warmup Warmup.
     * @param executionTime Time.
     * @param run Run.
     */
    public static void benchmark(@Nullable String name, long warmup, long executionTime, @NotNull Runnable run) {
        final AtomicBoolean stop = new AtomicBoolean();

        class Stopper extends TimerTask {
            @Override public void run() {
                stop.set(true);
            }
        }

        new Timer(true).schedule(new Stopper(), warmup);

        while (!stop.get())
            run.run();

        stop.set(false);

        new Timer(true).schedule(new Stopper(), executionTime);

        long startTime = System.currentTimeMillis();

        int cnt = 0;

        do {
            run.run();

            cnt++;
        }
        while (!stop.get());

        double dur = (System.currentTimeMillis() - startTime) / 1000d;

        System.out.printf("%s:\n operations:%d, duration=%fs, op/s=%d, latency=%fms\n", name, cnt, dur,
            (long)(cnt / dur), dur / cnt);
    }

    /**
     * Prompt to execute garbage collector.
     * {@code System.gc();} is not guaranteed to garbage collection, this method try to fill memory to crowd out dead
     * objects.
     */
    public static void runGC() {
        System.gc();

        ReferenceQueue<byte[]> queue = new ReferenceQueue<>();

        Collection<SoftReference<byte[]>> refs = new ArrayList<>();

        while (true) {
            byte[] bytes = new byte[128 * 1024];

            refs.add(new SoftReference<>(bytes, queue));

            if (queue.poll() != null)
                break;
        }

        System.gc();
    }

    /**
     * @return Path to apache ignite.
     */
    public static String apacheIgniteTestPath() {
        return System.getProperty("IGNITE_TEST_PATH", U.getIgniteHome() + "/target/ignite");
    }

    /**
     * Deletes index.bin for all cach groups for given {@code igniteInstanceName}
     */
    public static void deleteIndexBin(String igniteInstanceName) throws IgniteCheckedException {
        File workDir = U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, false);

        for (File grp : new File(workDir, U.maskForFileName(igniteInstanceName)).listFiles()) {
            new File(grp, "index.bin").delete();
        }
    }

    /**
     * Removing the directory cache groups.
     * Deletes all directory satisfy the {@code cacheGrpFilter}.
     *
     * @param igniteInstanceName Ignite instance name.
     * @param cacheGrpFilter Filter cache groups.
     * @throws Exception If failed.
     */
    public static void deleteCacheGrpDir(String igniteInstanceName, FilenameFilter cacheGrpFilter) throws Exception {
        File workDir = U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, false);

        String nodeDirName = U.maskForFileName(igniteInstanceName);

        for (File cacheGrpDir : new File(workDir, nodeDirName).listFiles(cacheGrpFilter))
            U.delete(cacheGrpDir);
    }

    /**
     * Removes the last checkpoint end marker for the given node.
     *
     * @param ignite Ignite node.
     */
    public static void deleteLastCheckpointEndMarker(IgniteEx ignite) throws IOException {
        IgniteCacheDatabaseSharedManager dbSharedMgr = ignite.context().cache().context().database();

        Path cpDir = ((GridCacheDatabaseSharedManager)dbSharedMgr).checkpointDirectory().toPath();

        try (Stream<Path> files = Files.list(cpDir)) {
            Optional<Path> endMarker = files
                .map(path -> path.getFileName().toString())
                .filter(fileName -> fileName.endsWith("START.bin"))
                .max(comparingLong(fileName -> parseLong(fileName.split("-")[0])))
                .map(fileName -> fileName.replace("START.bin", "END.bin"))
                .map(cpDir::resolve);

            if (endMarker.isPresent())
                Files.delete(endMarker.get());
        }
    }

    /**
     * {@link Class#getSimpleName()} does not return outer class name prefix for inner classes, for example,
     * getSimpleName() returns "RegularDiscovery" instead of "GridDiscoveryManagerSelfTest$RegularDiscovery"
     * This method return correct simple name for inner classes.
     *
     * @param cls Class
     * @return Simple name with outer class prefix.
     */
    public static String fullSimpleName(@NotNull Class cls) {
        if (cls.getEnclosingClass() != null)
            return cls.getEnclosingClass().getSimpleName() + "." + cls.getSimpleName();
        else
            return cls.getSimpleName();
    }

    /**
     * Adds test class to the list only if it's not in {@code ignoredTests} set.
     *
     * @param suite List where to place the test class.
     * @param test Test.
     * @param ignoredTests Tests to ignore. If test contained in the collection it is not included in suite
     */
    public static void addTestIfNeeded(@NotNull final List<Class<?>> suite, @NotNull final Class<?> test,
        @Nullable final Collection<Class> ignoredTests) {
        if (ignoredTests != null && ignoredTests.contains(test))
            return;

        suite.add(test);
    }

    /**
     * Generate random alphabetical string.
     *
     * @param rnd Random object.
     * @param maxLen Maximal length of string
     * @return Random string object.
     */
    public static String randomString(Random rnd, int maxLen) {
        return randomString(rnd, 0, maxLen);
    }

    /**
     * Generate random alphabetical string.
     *
     * @param rnd Random object.
     * @param maxLen Maximal length of string
     * @param minLen Minimum length of string
     * @return Random string object.
     */
    public static String randomString(Random rnd, int minLen, int maxLen) {
        assert minLen >= 0 : "minLen >= 0";
        assert maxLen >= minLen : "maxLen >= minLen";

        int len = maxLen == minLen ? minLen : minLen + rnd.nextInt(maxLen - minLen);

        StringBuilder b = new StringBuilder(len);

        for (int i = 0; i < len; i++)
            b.append(ALPHABETH.charAt(rnd.nextInt(ALPHABETH.length())));

        return b.toString();
    }

    /**
     * @param node Node.
     * @param topVer Ready exchange version to wait for before trying to merge exchanges.
     */
    public static void mergeExchangeWaitVersion(Ignite node, long topVer) {
        ((IgniteEx)node).context().cache().context().exchange().mergeExchangesTestWaitVersion(
            new AffinityTopologyVersion(topVer, 0), null);
    }

    /**
     * @param node Node.
     * @param topVer Ready exchange version to wait for before trying to merge exchanges.
     */
    public static void mergeExchangeWaitVersion(Ignite node, long topVer, List mergedEvts) {
        ((IgniteEx)node).context().cache().context().exchange().mergeExchangesTestWaitVersion(
            new AffinityTopologyVersion(topVer, 0), mergedEvts);
    }

    /**
     * Checks that {@code state} is active.
     *
     * @param state Passed cluster state.
     * @see ClusterState#active(ClusterState)
     */
    public static void assertActive(ClusterState state) {
        assertTrue(state + " isn't active state", ClusterState.active(state));
    }

    /**
     * Checks that {@code state} isn't active.
     *
     * @param state Passed cluster state.
     * @see ClusterState#active(ClusterState)
     */
    public static void assertInactive(ClusterState state) {
        assertFalse(state + " isn't inactive state", ClusterState.active(state));
    }

    /** @return Cartesian product of collections. See {@link Lists#cartesianProduct(List)}. */
    public static Collection<Object[]> cartesianProduct(Collection<?>... c) {
        List<List<?>> lists = F.asList(c).stream().map(ArrayList::new).collect(Collectors.toList());

        return F.transform(Lists.cartesianProduct(lists), List::toArray);
    }

    /** Test parameters scale factor util. */
    private static class ScaleFactorUtil {
        /** Test speed scale factor property name. */
        private static final String TEST_SCALE_FACTOR_PROPERTY = "TEST_SCALE_FACTOR";

        /** Min test scale factor value. */
        private static final double MIN_TEST_SCALE_FACTOR_VALUE = 0.1;

        /** Max test scale factor value. */
        private static final double MAX_TEST_SCALE_FACTOR_VALUE = 1.0;

        /** Test speed scale factor. */
        private static final double TEST_SCALE_FACTOR_VALUE = readScaleFactor();

        /** */
        private static double readScaleFactor() {
            double scaleFactor = Double.parseDouble(System.getProperty(TEST_SCALE_FACTOR_PROPERTY, "1.0"));

            scaleFactor = Math.max(scaleFactor, MIN_TEST_SCALE_FACTOR_VALUE);
            scaleFactor = Math.min(scaleFactor, MAX_TEST_SCALE_FACTOR_VALUE);

            return scaleFactor;
        }

        /** */
        public static int apply(int val) {
            return (int) Math.round(TEST_SCALE_FACTOR_VALUE * val);
        }

        /** */
        public static int apply(int val, int lowerBound, int upperBound) {
            return applyUB(applyLB(val, lowerBound), upperBound);
        }

        /** Apply scale factor with lower bound */
        public static int applyLB(int val, int lowerBound) {
            return Math.max(apply(val), lowerBound);
        }

        /** Apply scale factor with upper bound */
        public static int applyUB(int val, int upperBound) {
            return Math.min(apply(val), upperBound);
        }
    }

    /**
     * @param node Node to connect to.
     * @param params Connection parameters.
     * @return Thin JDBC connection to specified node.
     */
    public static Connection connect(IgniteEx node, String params) throws SQLException {
        Collection<GridPortRecord> recs = node.context().ports().records();

        GridPortRecord cliLsnrRec = null;

        for (GridPortRecord rec : recs) {
            if (rec.clazz() == ClientListenerProcessor.class) {
                cliLsnrRec = rec;

                break;
            }
        }

        assertNotNull(cliLsnrRec);

        String connStr = "jdbc:ignite:thin://127.0.0.1:" + cliLsnrRec.port();

        if (!F.isEmpty(params))
            connStr += "/?" + params;

        return DriverManager.getConnection(connStr);
    }

    /**
     * Removes idle_verify log files created in tests.
     */
    public static void cleanIdleVerifyLogFiles() {
        File dir = new File(".");

        for (File f : dir.listFiles(n -> n.getName().startsWith(IdleVerifyResultV2.IDLE_VERIFY_FILE_PREFIX)))
            f.delete();
    }

    /**
     * @param grid Node.
     * @param grp Group name.
     * @param name Object name.
     * @param attr Attribute name.
     * @param val Attribute value.
     * @throws Exception On error.
     */
    public static void setJmxAttribute(IgniteEx grid, String grp, String name, String attr, Object val) throws Exception {
        grid.context().config().getMBeanServer().setAttribute(U.makeMBeanName(grid.name(), grp, name),
            new Attribute(attr, val));
    }

    /**
     * @param grid Node.
     * @param grp Group name.
     * @param name Object name.
     * @param attr Attribute name.
     * @return Attribute's value.
     * @throws Exception On error.
     */
    public static Object getJmxAttribute(IgniteEx grid, String grp, String name, String attr) throws Exception {
        return grid.context().config().getMBeanServer().getAttribute(U.makeMBeanName(grid.name(), grp, name), attr);
    }

    /**
     */
    public static class SqlTestFunctions {
        /** Sleep milliseconds. */
        public static volatile long sleepMs;

        /** Fail flag. */
        public static volatile boolean fail;

        /**
         * Do sleep {@code sleepMs} milliseconds
         *
         * @return amount of milliseconds to sleep
         */
        @QuerySqlFunction
        @SuppressWarnings("BusyWait")
        public static long sleep() {
            long end = System.currentTimeMillis() + sleepMs;

            long remainTime = sleepMs;

            do {
                try {
                    Thread.sleep(remainTime);
                }
                catch (InterruptedException ignored) {
                    // No-op
                }
            }
            while ((remainTime = end - System.currentTimeMillis()) > 0);

            return sleepMs;
        }

        /**
         * Delays execution for {@code duration} milliseconds.
         *
         * @param duration Duration.
         * @return amount of milliseconds to delay.
         */
        @QuerySqlFunction
        public static long delay(long duration) {
            try {
                Thread.sleep(duration);
            }
            catch (InterruptedException ignored) {
                // No-op
            }

            return duration;
        }

        /**
         * Function do fail in case of {@code fail} is true, return 0 otherwise.
         *
         * @return in case of {@code fail} is false return 0, fail otherwise.
         */
        @QuerySqlFunction
        public static int can_fail() {
            if (fail)
                throw new IllegalArgumentException();
            else
                return 0;
        }

        /**
         * Function do sleep {@code sleepMs} milliseconds and do fail in case of {@code fail} is true, return 0 otherwise.
         *
         * @return amount of milliseconds to sleep in case of {@code fail} is false, fail otherwise.
         */
        @QuerySqlFunction
        public static long sleep_and_can_fail() {
            long sleep = sleep();

            can_fail();

            return sleep;
        }
    }

    /**
     * Runnable that can throw exceptions.
     */
    @FunctionalInterface
    public interface RunnableX extends Runnable {
        /**
         * Runnable body.
         *
         * @throws Exception If failed.
         */
        void runx() throws Exception;

        /** {@inheritDoc} */
        @Override default void run() {
            try {
                runx();
            }
            catch (RuntimeException e) {
                throw e;
            }
            catch (Exception e) {
                throw new IgniteException(e);
            }
        }
    }

    /**
     * IgniteRunnable that can throw exceptions.
     */
    @FunctionalInterface
    public interface IgniteRunnableX extends IgniteRunnable {
        /**
         * Runnable body.
         *
         * @throws Exception If failed.
         */
        void runx() throws Exception;

        /** {@inheritDoc} */
        @Override default void run() {
            try {
                runx();
            }
            catch (RuntimeException e) {
                throw e;
            }
            catch (Exception e) {
                throw new IgniteException(e);
            }
        }
    }

    /**
     * @param runnableX Runnable with exception.
     */
    public static void suppressException(RunnableX runnableX) {
        runnableX.run();
    }

    /**
     * Repeats messages which were sent to the logger into the alternative consumer.
     * Useful for the intercepting of logs without breaking usual output to stdout.
     * (It is painful to see failed tests at TC without actual logs).
     *
     * @param logger j.u.l.Logger.
     * @param printer Alternative log consumer.
     */
    public static void echoLogOutput(@NotNull Logger logger, Consumer<String> printer) {
        logger.addHandler(
            new Handler() {
                /** {@inheritDoc} */
                @Override public void publish(LogRecord record) {
                    printer.accept(record.getMessage());
                    printer.accept(U.nl());
                }

                /** {@inheritDoc} */
                @Override public void flush() {
                    // No-op.
                }

                /** {@inheritDoc} */
                @Override public void close() {
                    // No-op.
                }
            }
        );
    }

    /**
     * Creates a string containing only copies of one character.
     *
     * @param cnt Number of characters in the returned string.
     * @param c Character to be copied.
     * @return String containing copies of characters appended one after another.
     */
    public static String nCopiesOfChar(int cnt, char c) {
        assertTrue("Must be greater than zero: " + cnt, cnt >= 0);

        SB sb = new SB(cnt);

        for (int i = 0; i < cnt; i++)
            sb.a(c);

        return sb.toString();
    }
}
