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

package org.apache.ignite.testsuites;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import javassist.CannotCompileException;
import javassist.ClassClassPath;
import javassist.ClassPool;
import javassist.CodeConverter;
import javassist.CtClass;
import javassist.CtMethod;
import javassist.CtNewMethod;
import javassist.NotFoundException;
import javassist.bytecode.AnnotationsAttribute;
import javassist.bytecode.ClassFile;
import javassist.bytecode.ConstPool;
import javassist.bytecode.annotation.Annotation;
import javax.cache.CacheException;
import org.apache.ignite.DataRegionMetrics;
import org.apache.ignite.DataStorageMetrics;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicLong;
import org.apache.ignite.IgniteAtomicReference;
import org.apache.ignite.IgniteAtomicSequence;
import org.apache.ignite.IgniteAtomicStamped;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteCountDownLatch;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteEvents;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLock;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteMessaging;
import org.apache.ignite.IgniteQueue;
import org.apache.ignite.IgniteScheduler;
import org.apache.ignite.IgniteSemaphore;
import org.apache.ignite.IgniteServices;
import org.apache.ignite.IgniteSet;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.Ignition;
import org.apache.ignite.MemoryMetrics;
import org.apache.ignite.PersistenceMetrics;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.configuration.AtomicConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.CollectionConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.examples.ml.util.MLExamplesCommonArgs;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.plugin.IgnitePlugin;
import org.apache.ignite.plugin.PluginNotFoundException;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.model.InitializationError;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_OVERRIDE_MCAST_GRP;

/**
 * Examples test suite.
 * <p>
 * Contains only ML Grid Ignite examples tests.</p>
 */
@RunWith(IgniteExamplesMLTestSuite.DynamicSuite.class)
public class IgniteExamplesMLTestSuite {
    /** Base package to create test classes in. */
    private static final String basePkgForTests = "org.apache.ignite.examples.ml";

    /** Test class name pattern. */
    private static final String clsNamePtrn = ".*Example$";

    /** Ignite signleton for examples test suite. */
    private static IgniteTestMLProxy ignite;

    /** */
    @BeforeClass
    public static void init() {
        System.setProperty(IGNITE_OVERRIDE_MCAST_GRP,
            GridTestUtils.getNextMulticastGroup(IgniteExamplesMLTestSuite.class));
    }

    /** */
    @AfterClass
    public static void tearDown() throws Exception {
        if (ignite != null)
            ignite.delegate.close();
    }

    /** */
    public static Ignite getTestIgnite(String someString) {
        if (ignite == null) {
            try {
                ignite = new IgniteTestMLProxy(IgnitionEx.start("examples/config/example-ignite-ml.xml"));
            }
            catch (IgniteCheckedException e) {
                throw new RuntimeException(e);
            }
        }

        return ignite;
    }

    /**
     * Creates list of test classes for Ignite ML examples.
     *
     * @return Created list.
     * @throws IOException, ClassNotFoundException If failed.
     */
    public static Class<?>[] suite() throws IOException, ClassNotFoundException {
        return getClasses(basePkgForTests)
            .stream()
            .map(IgniteExamplesMLTestSuite::makeTestClass)
            .collect(Collectors.toList())
            .toArray(new Class[] {null});
    }

    /**
     * Creates test class for given example.
     *
     * @param exampleCls Class of the example to be tested.
     * @return Test class.
     */
    private static Class<?> makeTestClass(Class<?> exampleCls) {
        ClassPool cp = ClassPool.getDefault();

        cp.insertClassPath(new ClassClassPath(IgniteExamplesMLTestSuite.class));

        CtClass cl = cp.makeClass(basePkgForTests + "." + exampleCls.getSimpleName() + "SelfTest");

        try {
            CtMethod mtd = CtNewMethod.make("public void testExample() { "
                + exampleCls.getCanonicalName()
                + ".main("
                + MLExamplesCommonArgs.class.getName()
                + ".EMPTY_ARGS_ML); }", cl);

            // Create and add annotation.
            ClassFile ccFile = cl.getClassFile();
            ConstPool constpool = ccFile.getConstPool();

            AnnotationsAttribute attr = new AnnotationsAttribute(constpool, AnnotationsAttribute.visibleTag);
            Annotation annot = new Annotation("org.junit.Test", constpool);

            attr.addAnnotation(annot);
            mtd.getMethodInfo().addAttribute(attr);

            cl.addMethod(mtd);

            return cl.toClass();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Scans all classes accessible from the context class loader which belong to the given package and subpackages.
     *
     * @param pkgName The base package.
     * @return The classes.
     * @throws ClassNotFoundException If some classes not found.
     * @throws IOException If some resources unavailable.
     */
    private static List<Class> getClasses(String pkgName) throws ClassNotFoundException, IOException {
        String path = pkgName.replace('.', '/');

        Enumeration<URL> resources = Thread.currentThread()
            .getContextClassLoader()
            .getResources(path);

        List<File> dirs = new ArrayList<>();
        while (resources.hasMoreElements())
            dirs.add(new File(resources.nextElement().getFile()));


        List<Class> classes = new ArrayList<>();
        for (File directory : dirs) {
            // Replace Ignition.Start call in Tutorial.
            List<Class> tutorialClassess = findClassesAndReplaceIgniteStartCall(directory, pkgName, ".*Step_\\d+.*");
            A.notEmpty(tutorialClassess, "tutorialClassess.size() != 0");
            classes.addAll(findClassesAndReplaceIgniteStartCall(directory, pkgName, clsNamePtrn));
        }

        return classes;
    }

    /**
     * Recursive method used to find all classes in a given directory and sub-dirs.
     *
     * @param dir The base directory.
     * @param pkgName The package name for classes found inside the base directory.
     * @param clsNamePtrn Class name pattern.
     * @return The classes.
     * @throws ClassNotFoundException If class not found.
     */
    private static List<Class> findClassesAndReplaceIgniteStartCall(File dir, String pkgName, String clsNamePtrn) throws ClassNotFoundException {
        List<Class> classes = new ArrayList<>();

        if (!dir.exists())
            return classes;

        File[] files = dir.listFiles();
        if (files != null)
            for (File file : files) {
                if (file.isDirectory())
                    classes.addAll(findClassesAndReplaceIgniteStartCall(file, pkgName + "." + file.getName(), clsNamePtrn));
                else if (file.getName().endsWith(".class")) {
                    String clsName = pkgName + '.' + file.getName().substring(0, file.getName().length() - 6);

                    if (clsName.matches(clsNamePtrn))
                        classes.add(replaceIgniteStartCall(clsName));
                }
            }

        return classes;
    }

    /** */
    private static Class replaceIgniteStartCall(String clsName) {
        try {
            ClassPool cp = ClassPool.getDefault();
            CtClass ignClass = cp.get(Ignition.class.getName());
            CtClass suiteClass = cp.get(IgniteExamplesMLTestSuite.class.getName());
            CtMethod getTestIgniteMethod = suiteClass.getDeclaredMethod("getTestIgnite");

            CtMethod startM = null;
            for (CtMethod m : ignClass.getDeclaredMethods("start")) {
                CtClass[] params = m.getParameterTypes();
                if (params.length == 1 && params[0].getName().equals(String.class.getName())) {
                    startM = m;
                    break;
                }
            }

            CodeConverter converter = new CodeConverter();
            converter.redirectMethodCall(startM, getTestIgniteMethod);
            cp.get(clsName).instrument(converter);
            return cp.get(clsName).toClass();
        }
        catch (NotFoundException | CannotCompileException e) {
            throw new RuntimeException(e);
        }
    }

    /** */
    public static class DynamicSuite extends Suite {

        /** */
        public DynamicSuite(Class<?> cls) throws InitializationError, IOException, ClassNotFoundException {
            super(cls, suite());
        }
    }

    /** */
    private static class IgniteTestMLProxy implements Ignite {
        /** */
        private final Ignite delegate;

        /** */
        public IgniteTestMLProxy(Ignite delegate) {
            this.delegate = delegate;
        }

        /** */
        @Override public String name() {
            return delegate.name();
        }

        /** */
        @Override public IgniteLogger log() {
            return delegate.log();
        }

        /** */
        @Override public IgniteConfiguration configuration() {
            return delegate.configuration();
        }

        /** */
        @Override public IgniteCluster cluster() {
            return delegate.cluster();
        }

        /** */
        @Override public IgniteCompute compute() {
            return delegate.compute();
        }

        /** */
        @Override public IgniteCompute compute(ClusterGroup grp) {
            return delegate.compute(grp);
        }

        /** */
        @Override public IgniteMessaging message() {
            return delegate.message();
        }

        /** */
        @Override public IgniteMessaging message(ClusterGroup grp) {
            return delegate.message(grp);
        }

        /** */
        @Override public IgniteEvents events() {
            return delegate.events();
        }

        /** */
        @Override public IgniteEvents events(ClusterGroup grp) {
            return delegate.events(grp);
        }

        /** */
        @Override public IgniteServices services() {
            return delegate.services();
        }

        /** */
        @Override public IgniteServices services(ClusterGroup grp) {
            return delegate.services(grp);
        }

        /** */
        @Override public ExecutorService executorService() {
            return delegate.executorService();
        }

        /** */
        @Override public ExecutorService executorService(ClusterGroup grp) {
            return delegate.executorService(grp);
        }

        /** */
        @Override public IgniteProductVersion version() {
            return delegate.version();
        }

        /** */
        @Override public IgniteScheduler scheduler() {
            return delegate.scheduler();
        }

        /** */
        @Override public <K, V> IgniteCache<K, V> createCache(
            CacheConfiguration<K, V> cacheCfg) throws CacheException {
            return delegate.createCache(cacheCfg);
        }

        /** */
        @Override public Collection<IgniteCache> createCaches(
            Collection<CacheConfiguration> cacheCfgs) throws CacheException {
            return delegate.createCaches(cacheCfgs);
        }

        /** */
        @Override public <K, V> IgniteCache<K, V> createCache(String cacheName) throws CacheException {
            return delegate.createCache(cacheName);
        }

        /** */
        @Override public <K, V> IgniteCache<K, V> getOrCreateCache(
            CacheConfiguration<K, V> cacheCfg) throws CacheException {
            return delegate.getOrCreateCache(cacheCfg);
        }

        /** */
        @Override public <K, V> IgniteCache<K, V> getOrCreateCache(String cacheName) throws CacheException {
            return delegate.getOrCreateCache(cacheName);
        }

        /** */
        @Override public Collection<IgniteCache> getOrCreateCaches(
            Collection<CacheConfiguration> cacheCfgs) throws CacheException {
            return delegate.getOrCreateCaches(cacheCfgs);
        }

        /** */
        @Override public <K, V> void addCacheConfiguration(CacheConfiguration<K, V> cacheCfg) throws CacheException {
            delegate.addCacheConfiguration(cacheCfg);
        }

        /** */
        @Override public <K, V> IgniteCache<K, V> createCache(
            CacheConfiguration<K, V> cacheCfg,
            NearCacheConfiguration<K, V> nearCfg) throws CacheException {
            return delegate.createCache(cacheCfg, nearCfg);
        }

        /** */
        @Override public <K, V> IgniteCache<K, V> getOrCreateCache(
            CacheConfiguration<K, V> cacheCfg,
            NearCacheConfiguration<K, V> nearCfg) throws CacheException {
            return delegate.getOrCreateCache(cacheCfg, nearCfg);
        }

        /** */
        @Override public <K, V> IgniteCache<K, V> createNearCache(String cacheName,
            NearCacheConfiguration<K, V> nearCfg) throws CacheException {
            return delegate.createNearCache(cacheName, nearCfg);
        }

        /** */
        @Override public <K, V> IgniteCache<K, V> getOrCreateNearCache(String cacheName,
            NearCacheConfiguration<K, V> nearCfg) throws CacheException {
            return delegate.getOrCreateNearCache(cacheName, nearCfg);
        }

        /** */
        @Override public void destroyCache(String cacheName) throws CacheException {
            delegate.destroyCache(cacheName);
        }

        /** */
        @Override public void destroyCaches(Collection<String> cacheNames) throws CacheException {
            delegate.destroyCaches(cacheNames);
        }

        /** */
        @Override public <K, V> IgniteCache<K, V> cache(String name) throws CacheException {
            return delegate.cache(name);
        }

        /** */
        @Override public Collection<String> cacheNames() {
            return delegate.cacheNames();
        }

        /** */
        @Override public IgniteTransactions transactions() {
            return delegate.transactions();
        }

        /** */
        @Override public <K, V> IgniteDataStreamer<K, V> dataStreamer(String cacheName) throws IllegalStateException {
            return delegate.dataStreamer(cacheName);
        }

        /** */
        @Override
        public IgniteAtomicSequence atomicSequence(String name, long initVal, boolean create) throws IgniteException {
            return delegate.atomicSequence(name, initVal, create);
        }

        /** */
        @Override public IgniteAtomicSequence atomicSequence(String name,
            AtomicConfiguration cfg, long initVal, boolean create) throws IgniteException {
            return delegate.atomicSequence(name, cfg, initVal, create);
        }

        /** */
        @Override public IgniteAtomicLong atomicLong(String name, long initVal, boolean create) throws IgniteException {
            return delegate.atomicLong(name, initVal, create);
        }

        /** */
        @Override public IgniteAtomicLong atomicLong(String name,
            AtomicConfiguration cfg, long initVal, boolean create) throws IgniteException {
            return delegate.atomicLong(name, cfg, initVal, create);
        }

        /** */
        @Override public <T> IgniteAtomicReference<T> atomicReference(String name, T initVal,
            boolean create) throws IgniteException {
            return delegate.atomicReference(name, initVal, create);
        }

        /** */
        @Override public <T> IgniteAtomicReference<T> atomicReference(String name,
            AtomicConfiguration cfg, T initVal, boolean create) throws IgniteException {
            return delegate.atomicReference(name, cfg, initVal, create);
        }

        /** */
        @Override public <T, S> IgniteAtomicStamped<T, S> atomicStamped(String name, T initVal, S initStamp,
            boolean create) throws IgniteException {
            return delegate.atomicStamped(name, initVal, initStamp, create);
        }

        /** */
        @Override public <T, S> IgniteAtomicStamped<T, S> atomicStamped(String name,
            AtomicConfiguration cfg, T initVal, S initStamp, boolean create) throws IgniteException {
            return delegate.atomicStamped(name, cfg, initVal, initStamp, create);
        }

        /** */
        @Override public IgniteCountDownLatch countDownLatch(String name, int cnt, boolean autoDel,
            boolean create) throws IgniteException {
            return delegate.countDownLatch(name, cnt, autoDel, create);
        }

        /** */
        @Override public IgniteSemaphore semaphore(String name, int cnt, boolean failoverSafe,
            boolean create) throws IgniteException {
            return delegate.semaphore(name, cnt, failoverSafe, create);
        }

        /** */
        @Override public IgniteLock reentrantLock(String name, boolean failoverSafe, boolean fair,
            boolean create) throws IgniteException {
            return delegate.reentrantLock(name, failoverSafe, fair, create);
        }

        /** */
        @Override public <T> IgniteQueue<T> queue(String name, int cap,
            CollectionConfiguration cfg) throws IgniteException {
            return delegate.queue(name, cap, cfg);
        }

        /** */
        @Override public <T> IgniteSet<T> set(String name, CollectionConfiguration cfg) throws IgniteException {
            return delegate.set(name, cfg);
        }

        /** */
        @Override public <T extends IgnitePlugin> T plugin(String name) throws PluginNotFoundException {
            return delegate.plugin(name);
        }

        /** */
        @Override public IgniteBinary binary() {
            return delegate.binary();
        }

        /** */
        @Override public void close() throws IgniteException {
            // No op.
        }

        /** */
        @Override public <K> Affinity<K> affinity(String cacheName) {
            return delegate.affinity(cacheName);
        }

        /** */
        @Override @Deprecated public boolean active() {
            return delegate.active();
        }

        /** */
        @Override @Deprecated public void active(boolean active) {
            delegate.active(active);
        }

        /** */
        @Override public void resetLostPartitions(Collection<String> cacheNames) {
            delegate.resetLostPartitions(cacheNames);
        }

        /** */
        @Override @Deprecated public Collection<MemoryMetrics> memoryMetrics() {
            return delegate.memoryMetrics();
        }

        /** */
        @Override @Deprecated public MemoryMetrics memoryMetrics(String memPlcName) {
            return delegate.memoryMetrics(memPlcName);
        }

        /** */
        @Override @Deprecated public PersistenceMetrics persistentStoreMetrics() {
            return delegate.persistentStoreMetrics();
        }

        /** */
        @Override public Collection<DataRegionMetrics> dataRegionMetrics() {
            return delegate.dataRegionMetrics();
        }

        /** */
        @Override public DataRegionMetrics dataRegionMetrics(String memPlcName) {
            return delegate.dataRegionMetrics(memPlcName);
        }

        /** */
        @Override public DataStorageMetrics dataStorageMetrics() {
            return delegate.dataStorageMetrics();
        }
    }
}
