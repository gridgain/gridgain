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

package org.apache.ignite.testframework.junits;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Constructor;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import javax.management.MBeanServer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.SensitiveInfoTestLoggerProxy;
import org.apache.ignite.internal.binary.BinaryCachingMetadataHandler;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.resource.GridResourceProcessor;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.marshaller.MarshallerContextTestImpl;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.logger.GridTestLog4jLogger;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.COMMUNICATION_METRICS_GROUP_NAME;

/**
 * Test resources for injection.
 */
public class IgniteTestResources {
    /** Marshaller class name. */
    public static final String MARSH_CLASS_NAME = "test.marshaller.class";

    /** */
    private static final IgniteLogger rootLog = new GridTestLog4jLogger(false);

    /** */
    private final IgniteLogger log;

    /** Local host. */
    private final String locHost = localHost();

    /** */
    private final UUID nodeId = UUID.randomUUID();

    /** */
    private final MBeanServer jmx;

    /** */
    private final String home = U.getIgniteHome();

    /** */
    private ThreadPoolExecutor execSvc;

    /** */
    private IgniteConfiguration cfg;

    /** */
    private GridTestKernalContext ctx;

    /** */
    private GridResourceProcessor rsrcProc;

    /**
     * @return Default MBean server or {@code null} if {@code IGNITE_MBEANS_DISABLED} is configured.
     */
    @Nullable private static MBeanServer prepareMBeanServer() {
        return U.IGNITE_MBEANS_DISABLED ? null : ManagementFactory.getPlatformMBeanServer();
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public IgniteTestResources() throws IgniteCheckedException {
        if (SensitiveInfoTestLoggerProxy.TEST_SENSITIVE)
            log = new SensitiveInfoTestLoggerProxy(rootLog.getLogger(getClass()), null, null, null);
        else
            log = rootLog.getLogger(getClass());

        jmx = prepareMBeanServer();

        ctx = new GridTestKernalContext(log);

        rsrcProc = new GridResourceProcessor(ctx);
    }

    /**
     * @param cfg Ignite configuration
     */
    public IgniteTestResources(IgniteConfiguration cfg) throws IgniteCheckedException {
        this.cfg = cfg;
        log = rootLog.getLogger(getClass());
        jmx = prepareMBeanServer();
        ctx = new GridTestKernalContext(log, this.cfg);
        rsrcProc = new GridResourceProcessor(ctx);
    }

    /**
     * @param jmx JMX server.
     */
    public IgniteTestResources(MBeanServer jmx) throws IgniteCheckedException {
        assert jmx != null;

        this.jmx = jmx;
        log = rootLog.getLogger(getClass());
        ctx = new GridTestKernalContext(log);
        rsrcProc = new GridResourceProcessor(ctx);
    }

    /**
     * @param log Logger.
     */
    public IgniteTestResources(IgniteLogger log) throws IgniteCheckedException {
        assert log != null;

        this.log = log.getLogger(getClass());
        jmx = prepareMBeanServer();
        ctx = new GridTestKernalContext(log);
        rsrcProc = new GridResourceProcessor(ctx);
    }

    /**
     * @return Resource processor.
     */
    public GridResourceProcessor resources() {
        return rsrcProc;
    }

    /**
     * @return Local host.
     */
    @Nullable private String localHost() {
        try {
            return U.getLocalHost().getHostAddress();
        }
        catch (IOException e) {
            System.err.println("Failed to detect local host address.");

            e.printStackTrace();

            return null;
        }
    }

    /**
     * @param prestart Prestart flag.
     */
    public void startThreads(boolean prestart) {
        execSvc = new IgniteThreadPoolExecutor(nodeId.toString(), null, 40, 40, Long.MAX_VALUE,
            new LinkedBlockingQueue<Runnable>());

        // Improve concurrency for testing.
        if (prestart)
            execSvc.prestartAllCoreThreads();
    }

    /** */
    public void stopThreads() {
        if (execSvc != null) {
            U.shutdownNow(getClass(), execSvc, log);

            execSvc = null;
        }
    }

    /**
     * @param target Target.
     * @throws IgniteCheckedException If failed.
     */
    public void inject(Object target) throws IgniteCheckedException {
        assert target != null;
        assert getLogger() != null;

        rsrcProc.injectBasicResource(target, LoggerResource.class, getLogger().getLogger(target.getClass()));
        rsrcProc.injectBasicResource(target, IgniteInstanceResource.class,
            new IgniteMock(null, locHost, nodeId, getMarshaller(), jmx, home, cfg));

        if (target instanceof TcpCommunicationSpi) {
            TcpCommunicationSpi commSpi = (TcpCommunicationSpi)target;

            // I know it's shit, please suggest better solution.
            // The problem is that here we inject mock instance that is not IgniteEx as TcpCommunicationSpi expects.
            //
            // It would be cool to have MetricRegistry injected like
            // @MetricRegistryResource(COMMUNICATION_METRICS_GROUP_NAME)
            // private void injectMetricRegistry(MetricRegistry mreg) {
            //     // initialization of metrics.
            // }
            //
            // BUT it requires changing current injection framework a little bit. Parameterized annotations are
            // not supported.
            // Another option is to inject GridMetricManager, this is much easier to implement actually.
            GridTestUtils.setFieldValue(commSpi, "metricsLsnr", tcpCommunicationMetricsListener());
        }
    }

    /** */
    private Object tcpCommunicationMetricsListener() throws IgniteCheckedException {
        try {
            Class<?> metricsLsnrCls = Class.forName(
                "org.apache.ignite.spi.communication.tcp.TcpCommunicationMetricsListener"
            );

            MetricRegistry mreg = ctx.metric().registry(COMMUNICATION_METRICS_GROUP_NAME);

            @SuppressWarnings("JavaReflectionMemberAccess")
            Constructor<?> ctor = metricsLsnrCls.getDeclaredConstructor(MetricRegistry.class);

            ctor.setAccessible(true);

            return ctor.newInstance(mreg);
        }
        catch (Exception e) {
            throw new IgniteCheckedException(e);
        }
    }

    /**
     * @return Executor service.
     */
    public ExecutorService getExecutorService() {
        return execSvc;
    }

    /**
     * @return Ignite home.
     */
    public String getIgniteHome() {
        return home;
    }

    /**
     * @return MBean server.
     */
    public MBeanServer getMBeanServer() {
        return jmx;
    }

    /**
     * @param cls Class.
     * @return Logger for specified class.
     */
    public static IgniteLogger getLogger(Class<?> cls) {
        return rootLog.getLogger(cls);
    }

    /**
     * @return Logger.
     */
    public IgniteLogger getLogger() {
        return log;
    }

    /**
     * @return Node ID.
     */
    public UUID getNodeId() {
        return nodeId;
    }

    /**
     * @return Local host.
     */
    public String getLocalHost() {
        return locHost;
    }

    /**
     * @return Marshaller.
     * @throws IgniteCheckedException If failed.
     */
    public static synchronized Marshaller getMarshaller() throws IgniteCheckedException {
        String marshallerName = System.getProperty(MARSH_CLASS_NAME);

        Marshaller marsh;

        if (marshallerName == null)
            marsh = new BinaryMarshaller();
        else {
            try {
                Class<? extends Marshaller> cls = (Class<? extends Marshaller>)Class.forName(marshallerName);

                marsh = cls.newInstance();
            }
            catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
                throw new IgniteCheckedException("Failed to create test marshaller [marshaller=" +
                    marshallerName + ']', e);
            }
        }

        marsh.setContext(new MarshallerContextTestImpl());

        if (marsh instanceof BinaryMarshaller) {
            BinaryContext ctx =
                new BinaryContext(BinaryCachingMetadataHandler.create(), new IgniteConfiguration(), new NullLogger());

            IgniteUtils.invoke(BinaryMarshaller.class, marsh, "setBinaryContext", ctx, new IgniteConfiguration());
        }

        return marsh;
    }
}
