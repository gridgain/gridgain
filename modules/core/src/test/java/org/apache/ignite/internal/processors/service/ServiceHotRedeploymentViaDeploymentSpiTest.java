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

package org.apache.ignite.internal.processors.service;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;
import javax.tools.JavaCompiler;
import javax.tools.JavaCompiler.CompilationTask;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.StandardLocation;
import javax.tools.ToolProvider;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceDescriptor;
import org.apache.ignite.spi.deployment.DeploymentSpi;
import org.apache.ignite.spi.deployment.local.LocalDeploymentSpi;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_EVENT_DRIVEN_SERVICE_PROCESSOR_ENABLED;

/**
 * Tests services hot redeployment via {@link DeploymentSpi}.
 *
 * <b>NOTE:</b> to run test via Maven Surefire Plugin, the property "forkCount' should be set great than '0' or profile
 * 'surefire-fork-count-1' enabled.
 */
@WithSystemProperty(key = IGNITE_EVENT_DRIVEN_SERVICE_PROCESSOR_ENABLED, value = "true")
public class ServiceHotRedeploymentViaDeploymentSpiTest extends GridCommonAbstractTest {
    /** */
    private static final String SERVICE_NAME = "test-service";

    /** */
    private Path srcTmpDir;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDeploymentSpi(new LocalDeploymentSpi());

        return cfg;
    }

    /** */
    @BeforeClass
    public static void check() {
        Assume.assumeTrue(isEventDrivenServiceProcessorEnabled());
    }

    /** */
    @Before
    public void prepare() throws IOException {
        srcTmpDir = Files.createTempDirectory(getClass().getSimpleName());
    }

    /** */
    @After
    public void cleanup() {
        U.delete(srcTmpDir);
    }

    /** */
    @Test
    public void testServiceDeploymentViaDeploymentSpi() throws Exception {
        URLClassLoader clsLdr = prepareClassLoader(1);
        Class<?> cls = clsLdr.loadClass("MyRenewServiceImpl");

        MyRenewService srvc = (MyRenewService)cls.newInstance();

        assertEquals(1, srvc.version());

        try {
            Ignite ignite = startGrid(0);

            ignite.configuration().getDeploymentSpi().register(clsLdr, cls);

            ignite.services().deployClusterSingleton(SERVICE_NAME, srvc);

            Class<?> srvcCls = serviceClass(ignite, SERVICE_NAME);

            assertSame(cls, srvcCls);

            assertEquals(1, ignite.services().serviceProxy(SERVICE_NAME, MyRenewService.class, false)
                .version());

            Ignite ignite1 = startGrid(1);

            ignite1.configuration().getDeploymentSpi().register(clsLdr, cls);

            srvcCls = serviceClass(ignite1, SERVICE_NAME);

            assertSame(cls, srvcCls);

            assertEquals(1, ignite1.services().serviceProxy(SERVICE_NAME, MyRenewService.class, false)
                .version());
        }
        finally {
            stopAllGrids();
        }
    }

    /** */
    @Test
    public void testServiceHotRedeploymentNode() throws Exception {
        serviceHotRedeploymentTest(
            ignite -> ignite.services().serviceProxy(SERVICE_NAME, MyRenewService.class, false).version());
    }

    /** */
    @Test
    public void testServiceHotRedeploymentThinClient() throws Exception {
        serviceHotRedeploymentTest(ignite -> {
            try (IgniteClient client = Ignition.startClient(new ClientConfiguration().setAddresses("127.0.0.1:10800"))) {
                return client.services().serviceProxy(SERVICE_NAME, MyRenewService.class).version();
            }
            catch (Exception e) {
                throw new IgniteException(e);
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    private void serviceHotRedeploymentTest(ToIntFunction<Ignite> srvcFunc) throws Exception {
        URLClassLoader clsLdr = prepareClassLoader(1);
        Class<?> cls = clsLdr.loadClass("MyRenewServiceImpl");

        MyRenewService srvc = (MyRenewService)cls.newInstance();

        assertEquals(1, srvc.version());

        try {
            Ignite ignite = startGrid(0);

            final DeploymentSpi depSpi = ignite.configuration().getDeploymentSpi();

            depSpi.register(clsLdr, srvc.getClass());

            ignite.services().deployClusterSingleton(SERVICE_NAME, srvc);

            Class<?> srvcCls = serviceClass(ignite, SERVICE_NAME);

            assertSame(cls, srvcCls);

            assertEquals(1, srvcFunc.applyAsInt(ignite));

            ignite.services().cancel(SERVICE_NAME);
            depSpi.unregister(srvc.getClass().getName());

            clsLdr.close();

            clsLdr = prepareClassLoader(2);
            depSpi.register(clsLdr, srvc.getClass());

            ignite.services().deployClusterSingleton(SERVICE_NAME, srvc);

            assertNotSame(srvcCls, serviceClass(ignite, SERVICE_NAME));

            assertEquals(2, srvcFunc.applyAsInt(ignite));
        }
        finally {
            stopAllGrids();
        }
    }

    /** */
    private Class<?> serviceClass(Ignite ignite, String srvcName) {
        for (ServiceDescriptor desc : ignite.services().serviceDescriptors()) {
            if (srvcName.equals(desc.name()))
                return desc.serviceClass();
        }

        return null;
    }

    /** */
    public interface MyRenewService extends Service {
        /**
         * @return Service's version.
         */
        public int version();
    }

    /**
     * @param ver Version of generated class.
     * @return Prepared classloader.
     * @throws Exception In case of an error.
     */
    private URLClassLoader prepareClassLoader(int ver) throws Exception {
        String source = "import org.apache.ignite.internal.processors.service.ServiceHotRedeploymentViaDeploymentSpiTest;\n" +
            "import org.apache.ignite.services.ServiceContext;\n" +
            "public class MyRenewServiceImpl implements ServiceHotRedeploymentViaDeploymentSpiTest.MyRenewService {\n" +
            "    @Override public int version() {\n" +
            "        return " + ver + ";\n" +
            "    }\n" +
            "    @Override public void cancel(ServiceContext ctx) {}\n" +
            "    @Override public void init(ServiceContext ctx) throws Exception {}\n" +
            "    @Override public void execute(ServiceContext ctx) throws Exception {}\n" +
            "}";

        Files.createDirectories(srcTmpDir); // To avoid possible NoSuchFileException on some OS.

        File srcFile = new File(srcTmpDir.toFile(), "MyRenewServiceImpl.java");

        Path srcFilePath = Files.write(srcFile.toPath(), source.getBytes(StandardCharsets.UTF_8));

        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();

        StringWriter compilerLogger = new StringWriter();

        String classPath = System.getProperty("java.class.path");

        String separator = System.getProperty("path.separator");

        String surefireClassPath = System.getProperty("surefire.test.class.path");

        if (surefireClassPath != null)
            classPath += separator + surefireClassPath;

        List<File> cpList = Arrays.stream(classPath.split(separator)).map(File::new).collect(Collectors.toList());

        StandardJavaFileManager sjfm = compiler.getStandardFileManager(null, null, null);

        sjfm.setLocation(StandardLocation.CLASS_PATH, cpList);

        Iterable<? extends JavaFileObject> fileObjects = sjfm.getJavaFileObjects(srcFilePath.toFile());

        CompilationTask task = compiler.getTask(compilerLogger, sjfm, null, null, null, fileObjects);

        boolean result = task.call();

        String err = compilerLogger.toString();

        if (!result)
            throw new IgniteException("Failed to compile test classes: " + err + ", cp: " + classPath);

        try {
            sjfm.close();
        }
        catch (IOException e) {
            fail("Failed to close java filemanager: " + e.getMessage());
        }

        assertTrue("Failed to remove source file.", srcFile.delete());

        // Use context classloader as a parent for URLClassLoader (crucial for running this test via maven)
        ClassLoader loader = Thread.currentThread().getContextClassLoader();

        return new URLClassLoader(new URL[] {srcTmpDir.toUri().toURL()}, loader);
    }
}