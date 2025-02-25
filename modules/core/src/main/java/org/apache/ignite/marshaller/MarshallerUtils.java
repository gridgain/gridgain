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

package org.apache.ignite.marshaller;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URL;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Objects;
import java.util.function.Consumer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.ClassSet;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.plugin.PluginProvider;
import org.apache.ignite.tools.marshaller.IgniteObjectInputFilter;
import org.jetbrains.annotations.Nullable;
import sun.misc.ObjectInputFilter;

import static org.apache.ignite.internal.util.IgniteUtils.jdkVersion;
import static org.apache.ignite.internal.util.IgniteUtils.majorJavaVersion;

/**
 * Utility marshaller methods.
 */
public class MarshallerUtils {
    /** Jdk class names file. */
    public static final String JDK_CLS_NAMES_FILE = "META-INF/classnames-jdk.properties";

    /** Class names file. */
    public static final String CLS_NAMES_FILE = "META-INF/classnames.properties";

    /** Job sender node version. */
    private static final ThreadLocal<IgniteProductVersion> JOB_SND_NODE_VER = new ThreadLocal<>();

    /** Job sender node version. */
    private static final ThreadLocal<IgniteProductVersion> JOB_RCV_NODE_VER = new ThreadLocal<>();

    /** */
    private static final Object MUX = new Object();

    /**
     * Set node name to marshaller context if possible.
     *
     * @param marsh Marshaller instance.
     * @param nodeName Node name.
     */
    public static void setNodeName(Marshaller marsh, @Nullable String nodeName) {
        if (marsh instanceof AbstractNodeNameAwareMarshaller)
            ((AbstractNodeNameAwareMarshaller)marsh).nodeName(nodeName);
    }

    /**
     * Create JDK marshaller with provided node name.
     *
     * @param nodeName Node name.
     * @return JDK marshaller.
     */
    public static JdkMarshaller jdkMarshaller(@Nullable String nodeName) {
        JdkMarshaller marsh = new JdkMarshaller();

        setNodeName(marsh, nodeName);

        return marsh;
    }

    /**
     * Private constructor.
     */
    private MarshallerUtils() {
        // No-op.
    }

    /**
     * Sets thread local job sender node version.
     *
     * @param ver Thread local job sender node version.
     */
    public static void jobSenderVersion(IgniteProductVersion ver) {
        JOB_SND_NODE_VER.set(ver);
    }

    /**
     * Returns thread local job sender node version.
     *
     * @return Thread local job sender node version.
     */
    public static IgniteProductVersion jobSenderVersion() {
        return JOB_SND_NODE_VER.get();
    }

    /**
     * Sets thread local job receiver node version.
     *
     * @param ver Thread local job receiver node version.
     */
    public static void jobReceiverVersion(IgniteProductVersion ver) {
        JOB_RCV_NODE_VER.set(ver);
    }

    /**
     * Returns thread local job receiver node version.
     *
     * @return Thread local job receiver node version.
     */
    public static IgniteProductVersion jobReceiverVersion() {
        return JOB_RCV_NODE_VER.get();
    }

    /**
     * Returns class name filter for marshaller.
     *
     * @return Class name filter for marshaller.
     */
/*    public static IgnitePredicate<String> classNameFilter(ClassLoader clsLdr) throws IgniteCheckedException {
        ClassSet whiteList = classWhiteList(clsLdr);
        ClassSet blackList = classBlackList(clsLdr);

        return new IgnitePredicate<String>() {
            @Override public boolean apply(String s) {
                // Allows all primitive arrays and checks arrays' type.
                if ((blackList != null || whiteList != null) && s.charAt(0) == '[') {
                    if (s.charAt(1) == 'L' && s.length() > 2)
                        s = s.substring(2, s.length() - 1);
                    else
                        return true;
                }

                return (blackList == null || !blackList.contains(s)) && (whiteList == null || whiteList.contains(s));
            }
        };
    }*/

    public static IgniteMarshallerClassFilter classNameFilter(ClassLoader clsLdr) throws IgniteCheckedException {
        return new IgniteMarshallerClassFilter(classWhiteList(clsLdr), classBlackList(clsLdr));
    }

    /**
     * @param clsFilter Ignite marshaller class filter to which class validation will be delegated.
     * @throws IgniteCheckedException if autoconfiguration failed.
     */
    public static void autoconfigureObjectInputFilter(IgniteMarshallerClassFilter clsFilter) throws IgniteCheckedException {
/*        if (!IgniteSystemProperties.getBoolean(IGNITE_ENABLE_OBJECT_INPUT_FILTER_AUTOCONFIGURATION, true))
            return;*/

        int javaVer = majorJavaVersion(jdkVersion());

        String packageName;

        if (javaVer < 9) {
            packageName = "sun.misc.";
        } else {
            packageName = "java.io.";
        }

        Method getMtd;
        Method setMtd;

        Object objFilter;

        try {
            Class<?> clazz1 = Class.forName(packageName + "ObjectInputFilter");
            Class<?> clazz2 = Class.forName(packageName + "ObjectInputFilter$Config");

            getMtd = clazz2.getMethod("getSerialFilter");
            setMtd = clazz2.getMethod("setSerialFilter", clazz1);
        }
        catch (ClassNotFoundException | NoSuchMethodException e) {
            throw new RuntimeException("No ObjectInputFilter.Config.getSerialFilter method found", e);
        }

        try {
            objFilter = getMtd.invoke(null);
        }
        catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException("Failed to invoke direct buffer cleaner", e);
        }

        synchronized (MUX) {
            if (objFilter == null) {
                try {
                    setMtd.invoke(null, new IgniteObjectInputFilter(clsFilter::apply));
                } catch (IllegalAccessException | InvocationTargetException e) {
                    throw new RuntimeException(e);
                }
            }
            else if (objFilter instanceof IgniteObjectInputFilter) {
                IgniteObjectInputFilter igniteObjFilter = (IgniteObjectInputFilter)objFilter;

                if (!Objects.equals(igniteObjFilter.classFilter(), clsFilter)) {
                    throw new IgniteCheckedException("Failed to autoconfigure Ignite Object Input Filter for the current JVM" +
                            " because it was already set by another Ignite instance which is running in the same JVM and is" +
                            " configured with a different Marshaller Black or White lists.");
                }
            }
            else {
                throw new IgniteCheckedException("Failed to autoconfigure Ignite Object Input Filter for the current JVM as" +
                        " it was already set via `jdk.serialFilter` JVM system property or programmatically. You can disable" +
                        " Object Input Stream Filter autoconfiguration by setting `IGNITE_ENABLE_OBJECT_INPUT_FILTER_AUTOCONFIGURATION`" +
                        " system property to `false`. Note that in this case you must configure Java Serialization" +
                        " Filtering manually to filter out classes defined by the `IGNITE_MARSHALLER_BLACKLIST` system property" +
                        " [objectInputFilterClass=" + objFilter.getClass().getName() + ']');
            }
        }
    }

    /**
     * @param clsLdr Class loader.
     * @return White list of classes.
     */
    private static ClassSet classWhiteList(ClassLoader clsLdr) throws IgniteCheckedException {
        ClassSet clsSet = null;

        String fileName = IgniteSystemProperties.getString(IgniteSystemProperties.IGNITE_MARSHALLER_WHITELIST);

        if (fileName != null) {
            clsSet = new ClassSet();

            addClassNames(JDK_CLS_NAMES_FILE, clsSet, clsLdr);
            addClassNames(CLS_NAMES_FILE, clsSet, clsLdr);
            addClassNames(fileName, clsSet, clsLdr);
        }

        return clsSet;
    }

    /**
     * @param clsLdr Class loader.
     * @return Black list of classes.
     */
    private static ClassSet classBlackList(ClassLoader clsLdr) throws IgniteCheckedException {
        ClassSet clsSet = null;

        String blackListFileName = IgniteSystemProperties.getString(IgniteSystemProperties.IGNITE_MARSHALLER_BLACKLIST);

        if (blackListFileName != null)
            addClassNames(blackListFileName, clsSet = new ClassSet(), clsLdr);

        return clsSet;
    }

    /**
     * Reads class names from resource referred by given system property name and returns set of classes.
     *
     * @param fileName File name containing list of classes.
     * @param clsSet Class set for update.
     * @param clsLdr Class loader.
     */
    private static void addClassNames(
        String fileName,
        ClassSet clsSet,
        ClassLoader clsLdr
    ) throws IgniteCheckedException {
        InputStream is = clsLdr.getResourceAsStream(fileName);

        if (is == null) {
            try {
                is = new FileInputStream(new File(fileName));
            }
            catch (FileNotFoundException e) {
                throw new IgniteCheckedException("File " + fileName + " not found.");
            }
        }

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {
            String line;

            for (int i = 1; (line = reader.readLine()) != null; i++) {
                String s = line.trim();

                if (!s.isEmpty() && s.charAt(0) != '#' && s.charAt(0) != '[') {
                    try {
                        clsSet.add(s);
                    }
                    catch (IllegalArgumentException e) {
                        throw new IgniteCheckedException("Exception occurred while reading list of classes" +
                            "[path=" + fileName + ", row=" + i + ", line=" + s + ']', e);
                    }
                }
            }
        }
        catch (IOException e) {
            throw new IgniteCheckedException("Exception occurred while reading and creating list of classes " +
                "[path=" + fileName + ']', e);
        }
    }

    /**
     * Find all system class names (for JDK or Ignite classes) and process them with a given consumer.
     *
     * @param ldr Class loader.
     * @param plugins Plugins.
     * @param proc Class processor (class name consumer).
     */
    public static void processSystemClasses(ClassLoader ldr, @Nullable Collection<PluginProvider> plugins,
        Consumer<String> proc) throws IOException {
        Enumeration<URL> urls = ldr.getResources(CLS_NAMES_FILE);

        boolean foundClsNames = false;

        while (urls.hasMoreElements()) {
            processResource(urls.nextElement(), proc);

            foundClsNames = true;
        }

        if (!foundClsNames)
            throw new IgniteException("Failed to load class names properties file packaged with ignite binaries " +
                "[file=" + CLS_NAMES_FILE + ", ldr=" + ldr + ']');

        URL jdkClsNames = ldr.getResource(JDK_CLS_NAMES_FILE);

        if (jdkClsNames == null)
            throw new IgniteException("Failed to load class names properties file packaged with ignite binaries " +
                "[file=" + JDK_CLS_NAMES_FILE + ", ldr=" + ldr + ']');

        processResource(jdkClsNames, proc);

        if (plugins != null && !plugins.isEmpty()) {
            for (PluginProvider plugin : plugins) {
                Enumeration<URL> pluginUrls = ldr.getResources("META-INF/" + plugin.name().toLowerCase()
                    + ".classnames.properties");

                while (pluginUrls.hasMoreElements())
                    processResource(pluginUrls.nextElement(), proc);
            }
        }
    }

    /**
     * Process resource containing class names.
     *
     * @param url Resource URL.
     * @param proc Class processor (class name consumer).
     * @throws IOException In case of error.
     */
    private static void processResource(URL url, Consumer<String> proc) throws IOException {
        try (BufferedReader rdr = new BufferedReader(new InputStreamReader(url.openStream()))) {
            String line;

            while ((line = rdr.readLine()) != null) {
                if (line.isEmpty() || line.startsWith("#"))
                    continue;

                proc.accept(line.trim());
            }
        }
    }
}
