/*
 * Copyright 2024 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.util;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.Externalizable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.Reader;
import java.io.Serializable;
import java.io.StringWriter;
import java.io.UTFDataFormatException;
import java.io.Writer;
import java.lang.annotation.Annotation;
import java.lang.management.CompilationMXBean;
import java.lang.management.LockInfo;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MonitorInfo;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.math.MathContext;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLConnection;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileLock;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.nio.file.DirectoryStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.AccessController;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.KeyManagementException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PrivilegedAction;
import java.security.ProtectionDomain;
import java.security.cert.X509Certificate;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Random;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.function.Consumer;
import java.util.jar.JarFile;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.zip.Deflater;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;
import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.management.DynamicMBean;
import javax.management.JMException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.naming.Context;
import javax.naming.NamingException;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteClientDisconnectedException;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteDeploymentException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteIllegalStateException;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.binary.BinaryRawWriter;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterGroupEmptyException;
import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.compute.ComputeTaskCancelledException;
import org.apache.ignite.compute.ComputeTaskName;
import org.apache.ignite.compute.ComputeTaskTimeoutException;
import org.apache.ignite.configuration.AddressResolver;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteClientDisconnectedCheckedException;
import org.apache.ignite.internal.IgniteDeploymentCheckedException;
import org.apache.ignite.internal.IgniteFutureCancelledCheckedException;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.binary.GridBinaryMarshaller;
import org.apache.ignite.internal.cluster.ClusterGroupEmptyCheckedException;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.compute.ComputeTaskCancelledCheckedException;
import org.apache.ignite.internal.compute.ComputeTaskTimeoutCheckedException;
import org.apache.ignite.internal.events.DiscoveryCustomEvent;
import org.apache.ignite.internal.managers.communication.GridIoManager;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.managers.deployment.GridDeployment;
import org.apache.ignite.internal.managers.deployment.GridDeploymentInfo;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.mxbean.IgniteStandardMXBean;
import org.apache.ignite.internal.processors.cache.CacheClassLoaderMarker;
import org.apache.ignite.internal.processors.cache.GridCacheAttributes;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.IgnitePeerToPeerClassLoadingException;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cluster.BaselineTopology;
import org.apache.ignite.internal.transactions.IgniteTxAlreadyCompletedCheckedException;
import org.apache.ignite.internal.transactions.IgniteTxDuplicateKeyCheckedException;
import org.apache.ignite.internal.transactions.IgniteTxHeuristicCheckedException;
import org.apache.ignite.internal.transactions.IgniteTxOptimisticCheckedException;
import org.apache.ignite.internal.transactions.IgniteTxRollbackCheckedException;
import org.apache.ignite.internal.transactions.IgniteTxSerializationCheckedException;
import org.apache.ignite.internal.transactions.IgniteTxTimeoutCheckedException;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.future.IgniteFinishedFutureImpl;
import org.apache.ignite.internal.util.future.IgniteFutureImpl;
import org.apache.ignite.internal.util.io.GridFilenameUtils;
import org.apache.ignite.internal.util.ipc.shmem.IpcSharedMemoryNativeLoader;
import org.apache.ignite.internal.util.lang.GridClosureException;
import org.apache.ignite.internal.util.lang.GridPeerDeployAware;
import org.apache.ignite.internal.util.lang.GridTuple;
import org.apache.ignite.internal.util.lang.IgniteThrowableFunction;
import org.apache.ignite.internal.util.tostring.GridToStringBuilder;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.P1;
import org.apache.ignite.internal.util.typedef.T1;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.internal.util.typedef.T4;
import org.apache.ignite.internal.util.typedef.T5;
import org.apache.ignite.internal.util.typedef.T6;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteFutureCancelledException;
import org.apache.ignite.lang.IgniteFutureTimeoutException;
import org.apache.ignite.lang.IgniteOutClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.lifecycle.LifecycleAware;
import org.apache.ignite.logger.LoggerNodeIdAndApplicationAware;
import org.apache.ignite.logger.LoggerNodeIdAware;
import org.apache.ignite.logger.java.JavaLogger;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.PluginProvider;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.apache.ignite.spi.IgniteSpi;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.DiscoverySpiOrderSupport;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.thread.IgniteThreadFactory;
import org.apache.ignite.transactions.TransactionAlreadyCompletedException;
import org.apache.ignite.transactions.TransactionDeadlockException;
import org.apache.ignite.transactions.TransactionDuplicateKeyException;
import org.apache.ignite.transactions.TransactionHeuristicException;
import org.apache.ignite.transactions.TransactionOptimisticException;
import org.apache.ignite.transactions.TransactionRollbackException;
import org.apache.ignite.transactions.TransactionSerializationException;
import org.apache.ignite.transactions.TransactionTimeoutException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;
import sun.misc.Unsafe;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME;
import static java.util.Objects.isNull;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_DISABLE_HOSTNAME_VERIFIER;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_HOME;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_IGNORE_LOCAL_HOST_NAME;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_LOCAL_HOST;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_MBEAN_APPEND_CLASS_LOADER_ID;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_MBEAN_APPEND_JVM_ID;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_NO_DISCO_ORDER;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_SSH_HOST;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_SSH_USER_NAME;
import static org.apache.ignite.IgniteSystemProperties.getBoolean;
import static org.apache.ignite.IgniteSystemProperties.getString;
import static org.apache.ignite.events.EventType.EVTS_ALL;
import static org.apache.ignite.events.EventType.EVTS_ALL_MINUS_METRIC_UPDATE;
import static org.apache.ignite.events.EventType.EVT_NODE_METRICS_UPDATED;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_BUILD_DATE;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_BUILD_VER;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_CACHE;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_DATA_REGIONS_OFFHEAP_SIZE;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_DATA_REGIONS_TOTAL_ALLOCATED_SIZE;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_HOST_RAM_SIZE;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_JVM_PID;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_MACS;
import static org.apache.ignite.internal.util.GridUnsafe.objectFieldOffset;
import static org.apache.ignite.internal.util.GridUnsafe.putObjectVolatile;
import static org.apache.ignite.internal.util.GridUnsafe.staticFieldBase;
import static org.apache.ignite.internal.util.GridUnsafe.staticFieldOffset;

/**
 * Collection of utility methods used throughout the system.
 */
@SuppressWarnings({"UnusedReturnValue"})
public abstract class IgniteUtils {
    /** Logger. */
    private static final Logger log = Logger.getLogger(IgniteUtils.class.getName());

    /** */
    public static final long KB = 1024L;

    /** */
    public static final long MB = 1024L * 1024;

    /** */
    public static final long GB = 1024L * 1024 * 1024;

    /**
     * String limit in bytes for {@link DataOutput#writeUTF} and
     * {@link DataInput#readUTF()}, that use "Modified UTF-8".
     */
    public static final int UTF_BYTE_LIMIT = 65_535;

    /** Maximum number of character bytes for {@link DataOutput#writeUTF}. */
    public static final int MAX_UTF_BYTES = 3;

    /** Minimum checkpointing page buffer size (may be adjusted by Ignite). */
    public static final Long DFLT_MIN_CHECKPOINTING_PAGE_BUFFER_SIZE = GB / 4;

    /** Default minimum checkpointing page buffer size (may be adjusted by Ignite). */
    public static final Long DFLT_MAX_CHECKPOINTING_PAGE_BUFFER_SIZE = 2 * GB;

    /** @see IgniteSystemProperties#IGNITE_MBEAN_APPEND_CLASS_LOADER_ID */
    public static final boolean DFLT_MBEAN_APPEND_CLASS_LOADER_ID = true;

    /** Fallback local address. */
    public static final String LOCALHOST = "localhost";

    /** {@code True} if {@code unsafe} should be used for array copy. */
    private static final boolean UNSAFE_BYTE_ARR_CP = unsafeByteArrayCopyAvailable();

    /** Sun-specific JDK constructor factory for objects that don't have empty constructor. */
    private static final Method CTOR_FACTORY;

    /** Sun JDK reflection factory. */
    private static final Object SUN_REFLECT_FACTORY;

    /** Public {@code java.lang.Object} no-argument constructor. */
    private static final Constructor OBJECT_CTOR;

    /** All grid event names. */
    private static final Map<Integer, String> GRID_EVT_NAMES = new HashMap<>();

    /** All grid events. */
    private static final int[] GRID_EVTS;

    /** Empty byte array. */
    public static final byte[] EMPTY_BYTES = new byte[0];

    /** Empty integers array. */
    public static final int[] EMPTY_INTS = new int[0];

    /** Empty  longs. */
    public static final long[] EMPTY_LONGS = new long[0];

    /** Empty  longs. */
    public static final Field[] EMPTY_FIELDS = new Field[0];

    /** Empty string array. */
    public static final String[] EMPTY_STRINGS = new String[0];

    /** System line separator. */
    private static final String NL = System.getProperty("line.separator");

    /** Default user version. */
    public static final String DFLT_USER_VERSION = "0";

    /** Cache for {@link GridPeerDeployAware} fields to speed up reflection. */
    private static final ConcurrentMap<String, IgniteBiTuple<Class<?>, Collection<Field>>> p2pFields =
        new ConcurrentHashMap<>();

    /** Secure socket protocol to use. */
    private static final String HTTPS_PROTOCOL = "TLS";

    /** Default working directory name. */
    private static final String DEFAULT_WORK_DIR = "work";

    /** Thread dump message. */
    public static final String THREAD_DUMP_MSG = "Thread dump at ";

    /** Alphanumeric with underscore regexp pattern. */
    private static final Pattern ALPHANUMERIC_UNDERSCORE_PATTERN = Pattern.compile("^[a-zA-Z_0-9]+$");

    /** Ignite package. */
    public static final String IGNITE_PKG = "org.apache.ignite.";

    /**
     * To avoid tree corruption, at least two items should fit into one page of H2 index tree.
     * So maximum payload size equals: P = (PS - H - 3L) / 2 - X , where P - Payload size, PS - page size, H - page
     * header size, L - size of the child link, X - overhead per item. Calculated for pageSize = 1KB with
     * KeystoreEncryptionSpi and MVCC enabled.
     */
    public static final int MAX_INLINE_SIZE = 427;

    /** Project home directory. */
    private static volatile GridTuple<String> ggHome;

    /** OS JDK string. */
    private static final String osJdkStr;

    /** OS string. */
    private static final String osStr;

    /** JDK string. */
    private static String jdkStr;

    /** Indicates whether current OS is Windows 95. */
    private static boolean win95;

    /** Indicates whether current OS is Windows 98. */
    private static boolean win98;

    /** Indicates whether current OS is Windows NT. */
    private static boolean winNt;

    /** Indicates whether current OS is Windows Vista. */
    private static boolean winVista;

    /** Indicates whether current OS is Windows 7. */
    private static boolean win7;

    /** Indicates whether current OS is Windows 8. */
    private static boolean win8;

    /** Indicates whether current OS is Windows 8.1. */
    private static boolean win81;

    /** Indicates whether current OS is some version of Windows. */
    private static boolean unknownWin;

    /** Indicates whether current OS is Windows 2000. */
    private static boolean win2k;

    /** Indicates whether current OS is Windows XP. */
    private static boolean winXp;

    /** Indicates whether current OS is Windows Server 2003. */
    private static boolean win2003;

    /** Indicates whether current OS is Windows Server 2008. */
    private static boolean win2008;

    /** Indicates whether current OS is UNIX flavor. */
    private static boolean unix;

    /** Indicates whether current OS is Solaris. */
    private static boolean solaris;

    /** Indicates whether current OS is Linux flavor. */
    private static boolean linux;

    /** Indicates whether current OS is NetWare. */
    private static boolean netware;

    /** Indicates whether current OS is Mac OS. */
    private static boolean mac;

    /** Indicates whether current OS is of RedHat family. */
    private static final boolean redHat;

    /** Indicates whether current CPU architecture is Sun Sparc. */
    private static boolean sparc;

    /** Indicates whether current CPU architecture is Intel X86. */
    private static boolean x86;

    /** Indicates whether current CPU architecture is AMD64. */
    private static boolean amd64;

    /** Name of the underlying OS. */
    private static String osName;

    /** Version of the underlying OS. */
    private static String osVer;

    /** CPU architecture of the underlying OS. */
    private static String osArch;

    /** Name of the Java Runtime. */
    private static String javaRtName;

    /** Name of the Java Runtime version. */
    private static String javaRtVer;

    /** Name of the JDK vendor. */
    private static String jdkVendor;

    /** Name of the JDK. */
    private static String jdkName;

    /** Version of the JDK. */
    private static String jdkVer;

    /** Name of JVM specification. */
    private static String jvmSpecName;

    /** Version of JVM implementation. */
    private static String jvmImplVer;

    /** Vendor's name of JVM implementation. */
    private static String jvmImplVendor;

    /** Name of the JVM implementation. */
    private static String jvmImplName;

    /** Will be set to {@code true} if detected a 32-bit JVM. */
    private static final boolean jvm32Bit;

    /** JMX domain as 'xxx.apache.ignite'. */
    public static final String JMX_DOMAIN = IgniteUtils.class.getName().substring(0, IgniteUtils.class.getName().
        indexOf('.', IgniteUtils.class.getName().indexOf('.') + 1));

    /** Network packet header. */
    public static final byte[] IGNITE_HEADER = intToBytes(0x00004747);

    /** Default buffer size = 4K. */
    private static final int BUF_SIZE = 4096;

    /** Byte bit-mask. */
    private static final int MASK = 0xf;

    /** Long date format pattern for log messages. */
    public static final DateTimeFormatter LONG_DATE_FMT =
        DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm:ss").withZone(ZoneId.systemDefault());

    /**
     * Short date format pattern for log messages in "quiet" mode.
     * Only time is included since we don't expect "quiet" mode to be used
     * for longer runs.
     */
    public static final DateTimeFormatter SHORT_DATE_FMT =
        DateTimeFormatter.ofPattern("HH:mm:ss").withZone(ZoneId.systemDefault());

    /** Debug date format. */
    public static final DateTimeFormatter DEBUG_DATE_FMT =
        DateTimeFormatter.ofPattern("HH:mm:ss,SSS").withZone(ZoneId.systemDefault());

    /** Date format for thread dumps. */
    private static final DateTimeFormatter THREAD_DUMP_FMT =
        DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss z").withZone(ZoneId.systemDefault());

    /** ISO-8601 compliant date-time format. */
    public static final DateTimeFormatter ISO_DATE_FMT = ISO_LOCAL_DATE_TIME.withZone(ZoneId.systemDefault());

    /** Cached local host address to make sure that every time the same local host is returned. */
    private static InetAddress locHost;

    /** Supplier of network interfaces. Could be used for tests purposes, must not be changed in production code. */
    public static InterfaceSupplier INTERFACE_SUPPLIER = NetworkInterface::getNetworkInterfaces;

    /** */
    static volatile long curTimeMillis = System.currentTimeMillis();

    /** Primitive class map. */
    private static final Map<String, Class<?>> primitiveMap = new HashMap<>(16, .5f);

    /** Boxed class map. */
    private static final Map<Class<?>, Class<?>> boxedClsMap = new HashMap<>(16, .5f);

    /** Class loader used to load Ignite. */
    private static final ClassLoader gridClassLoader = IgniteUtils.class.getClassLoader();

    /** MAC OS invalid argument socket error message. */
    public static final String MAC_INVALID_ARG_MSG = "On MAC OS you may have too many file descriptors open " +
        "(simple restart usually solves the issue)";

    /** Ignite Logging Directory. */
    public static final String IGNITE_LOG_DIR = System.getenv(IgniteSystemProperties.IGNITE_LOG_DIR);

    /** Ignite Work Directory. */
    public static final String IGNITE_WORK_DIR = System.getenv(IgniteSystemProperties.IGNITE_WORK_DIR);

    /** Random is used to get random server node to authentication from client node. */
    private static final Random RND = new Random(System.currentTimeMillis());

    /** Clock timer. */
    private static Thread timer;

    /** Grid counter. */
    static int gridCnt;

    /** Mutex. */
    static final Object mux = new Object();

    /** Exception converters. */
    private static final Map<Class<? extends IgniteCheckedException>, C1<IgniteCheckedException, IgniteException>>
        exceptionConverters;

    /** */
    private static volatile IgniteBiTuple<Collection<String>, Collection<String>> cachedLocalAddr;

    /** */
    private static volatile IgniteBiTuple<Collection<String>, Collection<String>> cachedLocalAddrAllHostNames;

    /** */
    private static final ConcurrentMap<ClassLoader, ConcurrentMap<String, Class>> classCache =
        new ConcurrentHashMap<>();

    /** */
    private static volatile Boolean hasShmem;

    /** Object.hashCode() */
    private static Method hashCodeMtd;

    /** Object.equals(...) */
    private static Method equalsMtd;

    /** Object.toString() */
    private static Method toStringMtd;

    /** Empty local Ignite name. */
    public static final String LOC_IGNITE_NAME_EMPTY = new String();

    /** Local Ignite name thread local. */
    private static final ThreadLocal<String> LOC_IGNITE_NAME = new ThreadLocal<String>() {
        @Override protected String initialValue() {
            return LOC_IGNITE_NAME_EMPTY;
        }
    };

    /** Ignite MBeans disabled flag. */
    public static boolean IGNITE_MBEANS_DISABLED =
        IgniteSystemProperties.getBoolean(IgniteSystemProperties.IGNITE_MBEANS_DISABLED);

    /** Ignite test features enabled flag. */
    public static boolean IGNITE_TEST_FEATURES_ENABLED =
        IgniteSystemProperties.getBoolean(IgniteSystemProperties.IGNITE_TEST_FEATURES_ENABLED);

    /** */
    private static final boolean assertionsEnabled;

    /** Empty URL array. */
    private static final URL[] EMPTY_URL_ARR = new URL[0];

    /** Builtin class loader class.
     *
     * Note: needs for compatibility with Java 9.
     */
    private static final Class bltClsLdrCls = defaultClassLoaderClass();

    /** Url class loader field.
     *
     * Note: needs for compatibility with Java 9.
     */
    private static final Field urlClsLdrField = urlClassLoaderField();

    /** Dev only logging disabled. */
    private static final boolean devOnlyLogDisabled =
        IgniteSystemProperties.getBoolean(IgniteSystemProperties.IGNITE_DEV_ONLY_LOGGING_DISABLED);

    /** JDK9: jdk.internal.loader.URLClassPath. */
    private static Class clsURLClassPath;

    /** JDK9: URLClassPath#getURLs. */
    private static Method mthdURLClassPathGetUrls;

    /** Byte count prefixes. */
    private static final String BYTE_CNT_PREFIXES = " KMGTPE";

    /*
     * Initializes enterprise check.
     */
    static {
        boolean assertionsEnabled0 = true;

        try {
            assert false;

            assertionsEnabled0 = false;
        }
        catch (AssertionError ignored) {
            assertionsEnabled0 = true;
        }
        finally {
            assertionsEnabled = assertionsEnabled0;
        }

        redHat = Files.exists(Paths.get("/etc/redhat-release")); // RedHat family OS (Fedora, CentOS, RedHat)

        String osName = System.getProperty("os.name");

        String osLow = osName.toLowerCase();

        // OS type detection.
        if (osLow.contains("win")) {
            if (osLow.contains("95"))
                win95 = true;
            else if (osLow.contains("98"))
                win98 = true;
            else if (osLow.contains("nt"))
                winNt = true;
            else if (osLow.contains("2000"))
                win2k = true;
            else if (osLow.contains("vista"))
                winVista = true;
            else if (osLow.contains("xp"))
                winXp = true;
            else if (osLow.contains("2003"))
                win2003 = true;
            else if (osLow.contains("2008"))
                win2008 = true;
            else if (osLow.contains("7"))
                win7 = true;
            else if (osLow.contains("8.1"))
                win81 = true;
            else if (osLow.contains("8"))
                win8 = true;
            else
                unknownWin = true;
        }
        else if (osLow.contains("netware"))
            netware = true;
        else if (osLow.contains("mac os"))
            mac = true;
        else {
            // UNIXs flavors tokens.
            for (CharSequence os : new String[] {"ix", "inux", "olaris", "un", "ux", "sco", "bsd", "att"})
                if (osLow.contains(os)) {
                    unix = true;

                    break;
                }

            // UNIX name detection.
            if (osLow.contains("olaris") || osLow.contains("sunos"))
                solaris = true;
            else if (osLow.contains("inux"))
                linux = true;
        }

        String osArch = System.getProperty("os.arch");

        String archStr = osArch.toLowerCase();

        // OS architecture detection.
        if (archStr.contains("x86"))
            x86 = true;
        else if (archStr.contains("sparc"))
            sparc = true;
        else if (archStr.contains("amd64"))
            amd64 = true;

        String javaRtName = System.getProperty("java.runtime.name");
        String javaRtVer = System.getProperty("java.runtime.version");
        String jdkVendor = System.getProperty("java.specification.vendor");
        String jdkName = System.getProperty("java.specification.name");
        String jdkVer = System.getProperty("java.specification.version");
        String osVer = System.getProperty("os.version");
        String jvmSpecName = System.getProperty("java.vm.specification.name");
        String jvmImplVer = System.getProperty("java.vm.version");
        String jvmImplVendor = System.getProperty("java.vm.vendor");
        String jvmImplName = System.getProperty("java.vm.name");

        // Best effort to detect a 32-bit JVM.
        String jvmArchDataModel = System.getProperty("sun.arch.data.model");

        String jdkStr = javaRtName + ' ' + javaRtVer + ' ' + jvmImplVendor + ' ' + jvmImplName + ' ' +
            jvmImplVer;

        osStr = osName + ' ' + osVer + ' ' + osArch;
        osJdkStr = osLow + ", " + jdkStr;

        // Copy auto variables to static ones.
        IgniteUtils.osName = osName;
        IgniteUtils.jdkName = jdkName;
        IgniteUtils.jdkVendor = jdkVendor;
        IgniteUtils.jdkVer = jdkVer;
        IgniteUtils.jdkStr = jdkStr;
        IgniteUtils.osVer = osVer;
        IgniteUtils.osArch = osArch;
        IgniteUtils.jvmSpecName = jvmSpecName;
        IgniteUtils.jvmImplVer = jvmImplVer;
        IgniteUtils.jvmImplVendor = jvmImplVendor;
        IgniteUtils.jvmImplName = jvmImplName;
        IgniteUtils.javaRtName = javaRtName;
        IgniteUtils.javaRtVer = javaRtVer;

        jvm32Bit = "32".equals(jvmArchDataModel);

        primitiveMap.put("byte", byte.class);
        primitiveMap.put("short", short.class);
        primitiveMap.put("int", int.class);
        primitiveMap.put("long", long.class);
        primitiveMap.put("float", float.class);
        primitiveMap.put("double", double.class);
        primitiveMap.put("char", char.class);
        primitiveMap.put("boolean", boolean.class);
        primitiveMap.put("void", void.class);

        boxedClsMap.put(byte.class, Byte.class);
        boxedClsMap.put(short.class, Short.class);
        boxedClsMap.put(int.class, Integer.class);
        boxedClsMap.put(long.class, Long.class);
        boxedClsMap.put(float.class, Float.class);
        boxedClsMap.put(double.class, Double.class);
        boxedClsMap.put(char.class, Character.class);
        boxedClsMap.put(boolean.class, Boolean.class);
        boxedClsMap.put(void.class, Void.class);

        try {
            OBJECT_CTOR = Object.class.getConstructor();
        }
        catch (NoSuchMethodException e) {
            throw withCause(new AssertionError("Object class does not have empty constructor (is JDK corrupted?)."), e);
        }

        // Constructor factory.
        Method ctorFac = null;
        Object refFac = null;

        try {
            Class<?> refFactoryCls = Class.forName("sun.reflect.ReflectionFactory");

            refFac = refFactoryCls.getMethod("getReflectionFactory").invoke(null);

            ctorFac = refFac.getClass().getMethod("newConstructorForSerialization", Class.class,
                Constructor.class);
        }
        catch (NoSuchMethodException | ClassNotFoundException | IllegalAccessException | InvocationTargetException ignored) {
            // No-op.
        }

        CTOR_FACTORY = ctorFac;
        SUN_REFLECT_FACTORY = refFac;

        // Disable hostname SSL verification for development and testing with self-signed certificates.
        if (Boolean.parseBoolean(System.getProperty(IGNITE_DISABLE_HOSTNAME_VERIFIER))) {
            HttpsURLConnection.setDefaultHostnameVerifier(new HostnameVerifier() {
                @Override public boolean verify(String hostname, SSLSession sslSes) {
                    return true;
                }
            });
        }

        // Event names initialization.
        Class<?>[] evtHolderClasses = new Class[] {EventType.class, DiscoveryCustomEvent.class};

        for (Class<?> cls : evtHolderClasses) {
            for (Field field : cls.getFields()) {
                if (Modifier.isStatic(field.getModifiers()) && field.getType().equals(int.class)) {
                    if (field.getName().startsWith("EVT_")) {
                        try {
                            int type = field.getInt(null);

                            String prev = GRID_EVT_NAMES.put(type, field.getName().substring("EVT_".length()));

                            // Check for duplicate event types.
                            assert prev == null : "Duplicate event [type=" + type + ", name1=" + prev +
                                ", name2=" + field.getName() + ']';
                        }
                        catch (IllegalAccessException e) {
                            throw new IgniteException(e);
                        }
                    }
                }
            }
        }

        // Event array initialization.
        GRID_EVTS = toIntArray(GRID_EVT_NAMES.keySet());

        // Sort for fast event lookup.
        Arrays.sort(GRID_EVTS);

        // We need to re-initialize EVTS_ALL and EVTS_ALL_MINUS_METRIC_UPDATE
        // because they may have been initialized to null before GRID_EVTS were initialized.
        if (EVTS_ALL == null || EVTS_ALL_MINUS_METRIC_UPDATE == null) {
            try {
                Field f1 = EventType.class.getDeclaredField("EVTS_ALL");
                Field f2 = EventType.class.getDeclaredField("EVTS_ALL_MINUS_METRIC_UPDATE");

                assert f1 != null;
                assert f2 != null;

                // We use unsafe operations to update static fields on interface because
                // they are treated as static final and cannot be updated via standard reflection.
                putObjectVolatile(staticFieldBase(f1), staticFieldOffset(f1), gridEvents());
                putObjectVolatile(staticFieldBase(f2), staticFieldOffset(f2), gridEvents(EVT_NODE_METRICS_UPDATED));

                assert EVTS_ALL != null;
                assert EVTS_ALL.length == GRID_EVTS.length;

                assert EVTS_ALL_MINUS_METRIC_UPDATE != null;
                assert EVTS_ALL_MINUS_METRIC_UPDATE.length == GRID_EVTS.length - 1;

                // Validate correctness.
                for (int type : GRID_EVTS) {
                    assert containsIntArray(EVTS_ALL, type);

                    if (type != EVT_NODE_METRICS_UPDATED)
                        assert containsIntArray(EVTS_ALL_MINUS_METRIC_UPDATE, type);
                }

                assert !containsIntArray(EVTS_ALL_MINUS_METRIC_UPDATE, EVT_NODE_METRICS_UPDATED);
            }
            catch (NoSuchFieldException e) {
                throw new IgniteException(e);
            }
        }

        exceptionConverters = Collections.unmodifiableMap(exceptionConverters());

        // Set the http.strictPostRedirect property to prevent redirected POST from being mapped to a GET.
        System.setProperty("http.strictPostRedirect", "true");

        for (Method mtd : Object.class.getMethods()) {
            if ("hashCode".equals(mtd.getName()))
                hashCodeMtd = mtd;
            else if ("equals".equals(mtd.getName()))
                equalsMtd = mtd;
            else if ("toString".equals(mtd.getName()))
                toStringMtd = mtd;
        }

        try {
            clsURLClassPath = Class.forName("jdk.internal.loader.URLClassPath");
            mthdURLClassPathGetUrls = clsURLClassPath.getMethod("getURLs");
        }
        catch (ReflectiveOperationException e) {
            clsURLClassPath = null;
            mthdURLClassPathGetUrls = null;
        }
    }

    /**
     * Gets IgniteClosure for an IgniteCheckedException class.
     *
     * @param clazz Class.
     * @return The IgniteClosure mapped to this exception class, or null if none.
     */
    public static C1<IgniteCheckedException, IgniteException> getExceptionConverter(Class<? extends IgniteCheckedException> clazz) {
        return exceptionConverters.get(clazz);
    }

    /**
     * Gets map with converters to convert internal checked exceptions to public API unchecked exceptions.
     *
     * @return Exception converters.
     */
    private static Map<Class<? extends IgniteCheckedException>, C1<IgniteCheckedException, IgniteException>>
        exceptionConverters() {
        Map<Class<? extends IgniteCheckedException>, C1<IgniteCheckedException, IgniteException>> m = new HashMap<>();

        m.put(IgniteInterruptedCheckedException.class, new C1<IgniteCheckedException, IgniteException>() {
            @Override public IgniteException apply(IgniteCheckedException e) {
                return new IgniteInterruptedException(e.getMessage(), (InterruptedException)e.getCause());
            }
        });

        m.put(IgniteFutureCancelledCheckedException.class, new C1<IgniteCheckedException, IgniteException>() {
            @Override public IgniteException apply(IgniteCheckedException e) {
                return new IgniteFutureCancelledException(e.getMessage(), e);
            }
        });

        m.put(IgniteFutureTimeoutCheckedException.class, new C1<IgniteCheckedException, IgniteException>() {
            @Override public IgniteException apply(IgniteCheckedException e) {
                return new IgniteFutureTimeoutException(e.getMessage(), e);
            }
        });

        m.put(ClusterGroupEmptyCheckedException.class, new C1<IgniteCheckedException, IgniteException>() {
            @Override public IgniteException apply(IgniteCheckedException e) {
                return new ClusterGroupEmptyException(e.getMessage(), e);
            }
        });

        m.put(ClusterTopologyCheckedException.class, new C1<IgniteCheckedException, IgniteException>() {
            @Override public IgniteException apply(IgniteCheckedException e) {
                ClusterTopologyException topEx = new ClusterTopologyException(e.getMessage(), e);

                ClusterTopologyCheckedException checked = (ClusterTopologyCheckedException)e;

                if (checked.retryReadyFuture() != null)
                    topEx.retryReadyFuture(new IgniteFutureImpl<>(checked.retryReadyFuture()));

                return topEx;
            }
        });

        m.put(IgniteDeploymentCheckedException.class, new C1<IgniteCheckedException, IgniteException>() {
            @Override public IgniteException apply(IgniteCheckedException e) {
                return new IgniteDeploymentException(e.getMessage(), e);
            }
        });

        m.put(ComputeTaskTimeoutCheckedException.class, new C1<IgniteCheckedException, IgniteException>() {
            @Override public IgniteException apply(IgniteCheckedException e) {
                return new ComputeTaskTimeoutException(e.getMessage(), e);
            }
        });

        m.put(ComputeTaskCancelledCheckedException.class, new C1<IgniteCheckedException, IgniteException>() {
            @Override public IgniteException apply(IgniteCheckedException e) {
                return new ComputeTaskCancelledException(e.getMessage(), e);
            }
        });

        m.put(IgniteTxRollbackCheckedException.class, new C1<IgniteCheckedException, IgniteException>() {
            @Override public IgniteException apply(IgniteCheckedException e) {
                return new TransactionRollbackException(e.getMessage(), e);
            }
        });

        m.put(IgniteTxHeuristicCheckedException.class, new C1<IgniteCheckedException, IgniteException>() {
            @Override public IgniteException apply(IgniteCheckedException e) {
                return new TransactionHeuristicException(e.getMessage(), e);
            }
        });

        m.put(IgniteTxTimeoutCheckedException.class, new C1<IgniteCheckedException, IgniteException>() {
            @Override public IgniteException apply(IgniteCheckedException e) {
                if (e.getCause() instanceof TransactionDeadlockException)
                    return new TransactionTimeoutException(e.getMessage(), e.getCause());

                return new TransactionTimeoutException(e.getMessage(), e);
            }
        });

        m.put(IgniteTxOptimisticCheckedException.class, new C1<IgniteCheckedException, IgniteException>() {
            @Override public IgniteException apply(IgniteCheckedException e) {
                return new TransactionOptimisticException(e.getMessage(), e);
            }
        });

        m.put(IgniteClientDisconnectedCheckedException.class, new C1<IgniteCheckedException, IgniteException>() {
            @Override public IgniteException apply(IgniteCheckedException e) {
                return new IgniteClientDisconnectedException(
                    ((IgniteClientDisconnectedCheckedException)e).reconnectFuture(),
                    e.getMessage(),
                    e);
            }
        });

        m.put(IgniteTxSerializationCheckedException.class, new C1<IgniteCheckedException, IgniteException>() {
            @Override public IgniteException apply(IgniteCheckedException e) {
                return new TransactionSerializationException(e.getMessage(), e);
            }
        });

        m.put(IgniteTxDuplicateKeyCheckedException.class, new C1<IgniteCheckedException, IgniteException>() {
            @Override public IgniteException apply(IgniteCheckedException e) {
                return new TransactionDuplicateKeyException(e.getMessage(), e);
            }
        });

        m.put(IgniteTxAlreadyCompletedCheckedException.class, new C1<IgniteCheckedException, IgniteException>() {
            @Override public IgniteException apply(IgniteCheckedException e) {
                return new TransactionAlreadyCompletedException(e.getMessage(), e);
            }
        });

        return m;
    }

    /**
     * Gets all plugin providers.
     *
     * @return Plugins.
     */
    public static List<PluginProvider> allPluginProviders() {
        return AccessController.doPrivileged(new PrivilegedAction<List<PluginProvider>>() {
            @Override public List<PluginProvider> run() {
                List<PluginProvider> providers = new ArrayList<>();

                ServiceLoader<PluginProvider> ldr = ServiceLoader.load(PluginProvider.class);

                for (PluginProvider provider : ldr)
                    providers.add(provider);

                return providers;
            }
        });
    }

    /**
     * Converts exception, but unlike {@link #convertException(IgniteCheckedException)}
     * does not wrap passed in exception if none suitable converter found.
     *
     * @param e Ignite checked exception.
     * @return Ignite runtime exception.
     */
    public static Exception convertExceptionNoWrap(IgniteCheckedException e) {
        C1<IgniteCheckedException, IgniteException> converter = exceptionConverters.get(e.getClass());

        if (converter != null)
            return converter.apply(e);

        if (e.getCause() instanceof IgniteException)
            return (Exception)e.getCause();

        return e;
    }

    /**
     * @param e Ignite checked exception.
     * @return Ignite runtime exception.
     */
    public static IgniteException convertException(IgniteCheckedException e) {
        IgniteClientDisconnectedException e0 = e.getCause(IgniteClientDisconnectedException.class);

        if (e0 != null) {
            assert e0.reconnectFuture() != null : e0;

            throw e0;
        }

        IgniteClientDisconnectedCheckedException disconnectedErr =
            e.getCause(IgniteClientDisconnectedCheckedException.class);

        if (disconnectedErr != null) {
            assert disconnectedErr.reconnectFuture() != null : disconnectedErr;

            e = disconnectedErr;
        }

        C1<IgniteCheckedException, IgniteException> converter = exceptionConverters.get(e.getClass());

        if (converter != null)
            return converter.apply(e);

        if (e.getCause() instanceof IgniteException)
            return (IgniteException)e.getCause();

        return new IgniteException(e.getMessage(), e);
    }

    /**
     * @return System time approximated by 10 ms.
     */
    public static long currentTimeMillis() {
        return curTimeMillis;
    }

    /**
     * @return Value of {@link System#nanoTime()} in microseconds.
     */
    public static long microTime() {
        return System.nanoTime() / 1000;
    }

    /**
     * Convert milliseconds time interval to nanoseconds.
     *
     * @param millis Original time interval.
     * @return Calculated time interval.
     */
    public static long millisToNanos(long millis) {
        return TimeUnit.MILLISECONDS.toNanos(millis);
    }

    /**
     * Convert nanoseconds time interval to milliseconds.
     *
     * @param nanos Original time interval.
     * @return Calculated time interval.
     */
    public static long nanosToMillis(long nanos) {
        return TimeUnit.NANOSECONDS.toMillis(nanos);
    }

    /**
     * Returns number of milliseconds passed after the given nanos timestamp.
     *
     * @param nanos Nanos timestamp.
     * @return Number of milliseconds passed after the given nanos timestamp.
     * @see System#nanoTime()
     */
    public static long millisSinceNanos(long nanos) {
        return nanosToMillis(System.nanoTime() - nanos);
    }

    /**
     * Gets nearest power of 2 larger or equal than v.
     *
     * @param v Value.
     * @return Nearest power of 2.
     */
    public static int ceilPow2(int v) {
        int i = v - 1;

        return Integer.highestOneBit(i) << 1 - (i >>> 30 ^ v >> 31);
    }

    /**
     * @param i Value.
     * @return {@code true} If the given value is power of 2 (0 is not power of 2).
     */
    public static boolean isPow2(int i) {
        return i > 0 && (i & (i - 1)) == 0;
    }

    /**
     * Return SUN specific constructor factory.
     *
     * @return SUN specific constructor factory.
     */
    @Nullable public static Method ctorFactory() {
        return CTOR_FACTORY;
    }

    /**
     * @return Empty constructor for object class.
     */
    public static Constructor objectConstructor() {
        return OBJECT_CTOR;
    }

    /**
     * SUN JDK specific reflection factory for objects without public constructor.
     *
     * @return Reflection factory for objects without public constructor.
     */
    @Nullable public static Object sunReflectionFactory() {
        return SUN_REFLECT_FACTORY;
    }

    /**
     * Gets name for given grid event type.
     *
     * @param type Event type.
     * @return Event name.
     */
    public static String gridEventName(int type) {
        String name = GRID_EVT_NAMES.get(type);

        return name != null ? name : Integer.toString(type);
    }

    /**
     * Gets all event types.
     *
     * @param excl Optional exclude events.
     * @return All events minus excluded ones.
     */
    public static int[] gridEvents(final int... excl) {
        if (F.isEmpty(excl))
            return GRID_EVTS;

        List<Integer> evts = toIntList(GRID_EVTS, new P1<Integer>() {
            @Override public boolean apply(Integer i) {
                return !containsIntArray(excl, i);
            }
        });

        return toIntArray(evts);
    }

    /**
     * @param discoSpi Discovery SPI.
     * @return {@code True} if ordering is supported.
     */
    public static boolean discoOrdered(DiscoverySpi discoSpi) {
        DiscoverySpiOrderSupport ann = U.getAnnotation(discoSpi.getClass(), DiscoverySpiOrderSupport.class);

        return ann != null && ann.value();
    }

    /**
     * @return Checks if disco ordering should be enforced.
     */
    public static boolean relaxDiscoveryOrdered() {
        return "true".equalsIgnoreCase(System.getProperty(IGNITE_NO_DISCO_ORDER));
    }

    /**
     * This method should be used for adding quick debug statements in code
     * while debugging. Calls to this method should never be committed to master.
     *
     * @param msg Message to debug.
     * @deprecated Calls to this method should never be committed to master.
     */
    @Deprecated
    public static void debug(Object msg) {
        X.error(debugPrefix() + msg);
    }

    /**
     * This method should be used for adding quick debug statements in code
     * while debugging. Calls to this method should never be committed to master.
     *
     * @param msg Message to debug.
     * @deprecated Calls to this method should never be committed to master.
     */
    @Deprecated
    public static void debugx(String msg) {
        X.printerrln(debugPrefix() + msg);
    }

    /**
     * This method should be used for adding quick debug statements in code
     * while debugging. Calls to this method should never be committed to master.
     *
     * @param log Logger.
     * @param msg Message to debug.
     *
     * @deprecated Calls to this method should never be committed to master.
     */
    @Deprecated
    public static void debug(IgniteLogger log, String msg) {
        log.info(msg);
    }

    /**
     * Prints stack trace of the current thread to {@code System.out}.
     *
     * @deprecated Calls to this method should never be committed to master.
     */
    @Deprecated
    public static void dumpStack() {
        dumpStack("Dumping stack.");
    }

    /**
     * Prints stack trace of the current thread to {@code System.out}.
     *
     * @param msg Message to print with the stack.
     *
     * @deprecated Calls to this method should never be committed to master.
     */
    @Deprecated
    public static void dumpStack(String msg) {
        new Exception(debugPrefix() + msg).printStackTrace(System.err);
    }

    /**
     * @param log Logger.
     * @param msg Message.
     */
    public static void dumpStack(@Nullable IgniteLogger log, String msg) {
        U.error(log, "Dumping stack.", new Exception(msg));
    }

    /**
     * Prints stack trace of the current thread to provided output stream.
     *
     * @param msg Message to print with the stack.
     * @param out Output to dump stack to.
     *
     * @deprecated Calls to this method should never be committed to master.
     */
    @Deprecated
    public static void dumpStack(String msg, PrintStream out) {
        new Exception(msg).printStackTrace(out);
    }

    /**
     * Prints stack trace of the current thread to provided logger.
     *
     * @param log Logger.
     * @param msg Message to print with the stack.
     *
     * @deprecated Calls to this method should never be committed to master.
     */
    @Deprecated
    public static void debugStack(IgniteLogger log, String msg) {
        log.error(msg, new Exception(debugPrefix() + msg));
    }

    /**
     * @return Common prefix for debug messages.
     */
    private static String debugPrefix() {
        return '<' + DEBUG_DATE_FMT.format(Instant.now()) + "><DEBUG><" +
            Thread.currentThread().getName() + '>' + ' ';
    }

    /**
     * Prints heap usage.
     */
    public static void debugHeapUsage() {
        System.gc();

        Runtime runtime = Runtime.getRuntime();

        X.println('<' + DEBUG_DATE_FMT.format(Instant.now()) + "><DEBUG><" +
            Thread.currentThread().getName() + "> Heap stats [free=" + runtime.freeMemory() / (1024 * 1024) +
            "M, total=" + runtime.totalMemory() / (1024 * 1024) + "M]");
    }

    /**
     * Gets heap size in GB rounded to specified precision.
     *
     * @param node Node.
     * @param precision Precision.
     * @return Heap size in GB.
     */
    public static double heapSize(ClusterNode node, int precision) {
        return heapSize(Collections.singleton(node), precision);
    }

    /**
     * Gets total heap size in GB rounded to specified precision.
     *
     * @param nodes Nodes.
     * @param precision Precision.
     * @return Total heap size in GB.
     */
    public static double heapSize(Iterable<ClusterNode> nodes, int precision) {
        // In bytes.
        double heap = 0.0;

        for (ClusterNode n : nodesPerJvm(nodes)) {
            ClusterMetrics m = n.metrics();

            heap += Math.max(m.getHeapMemoryInitialized(), m.getHeapMemoryMaximum());
        }

        return roundedHeapSize(heap, precision);
    }

    /**
     * Gets total offheap size in GB rounded to specified precision.
     *
     * @param nodes Nodes.
     * @param precision Precision.
     * @return Total offheap size in GB.
     */
    public static double offheapSize(Iterable<ClusterNode> nodes, int precision) {
        // In bytes.
        double totalOffheap = 0.0;

        for (ClusterNode n : nodesPerJvm(nodes)) {
            Long val = n.<Long>attribute(ATTR_DATA_REGIONS_OFFHEAP_SIZE);

            if (val != null)
                totalOffheap += val;
        }

        return roundedHeapSize(totalOffheap, precision);
    }

    /**
     * Gets total allocated persistence size in GB rounded to specified precision.
     *
     * @param nodes Nodes.
     * @param precision Precision.
     * @return Total allocated PDS size in GB.
     */
    public static double allocatedPDSSize(Iterable<ClusterNode> nodes, int precision) {
        double totalAllocatedPDS = .0;

        for (ClusterNode n : nodes) {
            if (n.isClient())
                continue;

            Long val = n.<Long>attribute(ATTR_DATA_REGIONS_TOTAL_ALLOCATED_SIZE);

            if (val != null)
                totalAllocatedPDS += val;
        }

        return roundedHeapSize(totalAllocatedPDS, precision);
    }

    public static double hostRamSize(Iterable<ClusterNode> nodes, int precision) {
        double totalHostRamSize = .0;

        for (Collection<ClusterNode> nodesPerHost : neighborhood(nodes).values()) {
            ClusterNode first = F.first(nodesPerHost);
            if (first != null) {
                Long val = first.<Long>attribute(ATTR_HOST_RAM_SIZE);

                if (val != null)
                    totalHostRamSize += val;
            }
        }

        return roundedHeapSize(totalHostRamSize, precision);
    }

    /**
     * Returns one representative node for each JVM.
     *
     * @param nodes Nodes.
     * @return Collection which contains only one representative node for each JVM.
     */
    private static Iterable<ClusterNode> nodesPerJvm(Iterable<ClusterNode> nodes) {
        Map<String, ClusterNode> grpMap = new HashMap<>();

        // Group by mac addresses and pid.
        for (ClusterNode node : nodes) {
            String grpId = node.attribute(ATTR_MACS) + "|" + node.attribute(ATTR_JVM_PID);

            if (!grpMap.containsKey(grpId))
                grpMap.put(grpId, node);
        }

        return grpMap.values();
    }

    /**
     * Returns current JVM maxMemory in the same format as {@link #heapSize(ClusterNode, int)}.
     *
     * @param precision Precision.
     * @return Maximum memory size in GB.
     */
    public static double heapSize(int precision) {
        return roundedHeapSize(Runtime.getRuntime().maxMemory(), precision);
    }

    /**
     * Rounded heap size in gigabytes.
     *
     * @param heap Heap.
     * @param precision Precision.
     * @return Rounded heap size.
     */
    private static double roundedHeapSize(double heap, int precision) {
        double rounded = new BigDecimal(heap / (1024 * 1024 * 1024d)).round(new MathContext(precision)).doubleValue();

        return rounded < 0.1 ? 0.1 : rounded;
    }

    /**
     * Performs thread dump and prints all available info to the given log with WARN logging level.
     *
     * @param log Logger.
     */
    public static void dumpThreads(@Nullable IgniteLogger log) {
        dumpThreads(log, false);
    }

    /**
     * Performs thread dump and prints all available info to the given log
     * with WARN or ERROR logging level depending on {@code isErrorLevel} parameter.
     *
     * @param log Logger.
     * @param isErrorLevel {@code true} if thread dump must be printed with ERROR logging level,
     *      {@code false} if thread dump must be printed with WARN logging level.
     */
    public static void dumpThreads(@Nullable IgniteLogger log, boolean isErrorLevel) {
        ThreadMXBean mxBean = ManagementFactory.getThreadMXBean();

        final Set<Long> deadlockedThreadsIds = getDeadlockedThreadIds(mxBean);

        if (deadlockedThreadsIds.isEmpty())
            logMessage(log, "No deadlocked threads detected.", isErrorLevel);
        else
            logMessage(log, "Deadlocked threads detected (see thread dump below) " +
                "[deadlockedThreadsCnt=" + deadlockedThreadsIds.size() + ']', isErrorLevel);

        ThreadInfo[] threadInfos =
            mxBean.dumpAllThreads(mxBean.isObjectMonitorUsageSupported(), mxBean.isSynchronizerUsageSupported());

        GridStringBuilder sb = new GridStringBuilder(THREAD_DUMP_MSG)
            .a(THREAD_DUMP_FMT.format(Instant.ofEpochMilli(U.currentTimeMillis()))).a(NL);

        for (ThreadInfo info : threadInfos) {
            printThreadInfo(info, sb, deadlockedThreadsIds);

            sb.a(NL);

            if (info.getLockedSynchronizers() != null && info.getLockedSynchronizers().length > 0) {
                printSynchronizersInfo(info.getLockedSynchronizers(), sb);

                sb.a(NL);
            }
        }

        sb.a(NL);

        logMessage(log, sb.toString(), isErrorLevel);
    }

    /**
     * @param log Logger.
     * @param msg Message.
     * @param isErrorLevel {@code true} if message must be printed with ERROR logging level,
     *      {@code false} if message must be printed with WARN logging level.
     */
    private static void logMessage(@Nullable IgniteLogger log, String msg, boolean isErrorLevel) {
        if (isErrorLevel)
            error(log, msg);
        else
            warn(log, msg);
    }

    /**
     * Get deadlocks from the thread bean.
     * @param mxBean the bean
     * @return the set of deadlocked threads (may be empty Set, but never null).
     */
    private static Set<Long> getDeadlockedThreadIds(ThreadMXBean mxBean) {
        final long[] deadlockedIds = mxBean.findDeadlockedThreads();

        final Set<Long> deadlockedThreadsIds;

        if (!F.isEmpty(deadlockedIds)) {
            Set<Long> set = new HashSet<>();

            for (long id : deadlockedIds)
                set.add(id);

            deadlockedThreadsIds = Collections.unmodifiableSet(set);
        }
        else
            deadlockedThreadsIds = Collections.emptySet();

        return deadlockedThreadsIds;
    }

    /**
     * @param threadId Thread ID.
     * @param sb Builder.
     */
    public static void printStackTrace(long threadId, GridStringBuilder sb) {
        ThreadMXBean mxBean = ManagementFactory.getThreadMXBean();

        ThreadInfo threadInfo = mxBean.getThreadInfo(threadId, Integer.MAX_VALUE);

        if (threadInfo != null)
            printThreadInfo(threadInfo, sb, Collections.<Long>emptySet());
        else
            sb.a("The thread of the given id is not alive or does not exist [id=").a(threadId).a("]").a(NL);
    }

    /**
     * @return Stacktrace of current thread as {@link String}.
     */
    public static String stackTrace() {
        GridStringBuilder sb = new GridStringBuilder();
        long threadId = Thread.currentThread().getId();

        printStackTrace(threadId, sb);

        return sb.toString();
    }

    /**
     * @return {@code true} if there is java level deadlock.
     */
    public static boolean deadlockPresent() {
        ThreadMXBean mxBean = ManagementFactory.getThreadMXBean();

        return !F.isEmpty(mxBean.findDeadlockedThreads());
    }

    /**
     * Prints single thread info to a buffer.
     *
     * @param threadInfo Thread info.
     * @param sb Buffer.
     */
    private static void printThreadInfo(ThreadInfo threadInfo, GridStringBuilder sb, Set<Long> deadlockedIdSet) {
        final long id = threadInfo.getThreadId();

        if (deadlockedIdSet.contains(id))
            sb.a("##### DEADLOCKED ");

        sb.a("Thread [name=\"").a(threadInfo.getThreadName())
            .a("\", id=").a(threadInfo.getThreadId())
            .a(", state=").a(threadInfo.getThreadState())
            .a(", blockCnt=").a(threadInfo.getBlockedCount())
            .a(", waitCnt=").a(threadInfo.getWaitedCount()).a("]").a(NL);

        LockInfo lockInfo = threadInfo.getLockInfo();

        if (lockInfo != null) {
            sb.a("    Lock [object=").a(lockInfo)
                .a(", ownerName=").a(threadInfo.getLockOwnerName())
                .a(", ownerId=").a(threadInfo.getLockOwnerId()).a("]").a(NL);
        }

        printStackTraceElements(sb, threadInfo.getStackTrace(), threadInfo.getLockedMonitors());
    }

    /**
     * Print stack trace elements into a string builder. Optionally writes acquired locks.
     *
     * @param sb Output string builder.
     * @param elements Stack trace elements.
     * @param monitors Monitors.
     */
    public static void printStackTraceElements(GridStringBuilder sb, StackTraceElement[] elements, MonitorInfo[] monitors) {
        for (int i = 0; i < elements.length; i++) {
            StackTraceElement e = elements[i];

            sb.a("        at ").a(e.toString());

            if (monitors != null) {
                for (MonitorInfo monitor : monitors) {
                    if (monitor.getLockedStackDepth() == i)
                        sb.a(NL).a("        - locked ").a(monitor);
                }
            }

            sb.a(NL);
        }
    }

    /**
     * Prints Synchronizers info to a buffer.
     *
     * @param syncs Synchronizers info.
     * @param sb Buffer.
     */
    private static void printSynchronizersInfo(LockInfo[] syncs, GridStringBuilder sb) {
        sb.a("    Locked synchronizers:");

        for (LockInfo info : syncs)
            sb.a(NL).a("        ").a(info);
    }

    /**
     * Gets empty constructor for class even if the class does not have empty constructor
     * declared. This method is guaranteed to work with SUN JDK and other JDKs still need
     * to be tested.
     *
     * @param cls Class to get empty constructor for.
     * @return Empty constructor if one could be found or {@code null} otherwise.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public static Constructor<?> forceEmptyConstructor(Class<?> cls) throws IgniteCheckedException {
        Constructor<?> ctor = null;

        try {
            return cls.getDeclaredConstructor();
        }
        catch (Exception ignore) {
            Method ctorFac = U.ctorFactory();
            Object sunRefFac = U.sunReflectionFactory();

            if (ctorFac != null && sunRefFac != null)
                try {
                    ctor = (Constructor)ctorFac.invoke(sunRefFac, cls, U.objectConstructor());
                }
                catch (IllegalAccessException | InvocationTargetException e) {
                    throw new IgniteCheckedException("Failed to get object constructor for class: " + cls, e);
                }
        }

        return ctor;
    }

    /**
     * Gets class for the given name if it can be loaded or default given class.
     *
     * @param cls Class.
     * @param dflt Default class to return.
     * @return Class or default given class if it can't be found.
     */
    @Nullable public static Class<?> classForName(@Nullable String cls, @Nullable Class<?> dflt) {
        return classForName(cls, dflt, false);
    }

    /**
     * Gets class for the given name if it can be loaded or default given class.
     *
     * @param cls Class.
     * @param dflt Default class to return.
     * @param includePrimitiveTypes Whether class resolution should include primitive types
     *                              (i.e. "int" will resolve to int.class if flag is set)
     * @return Class or default given class if it can't be found.
     */
    @Nullable public static Class<?> classForName(
        @Nullable String cls,
        @Nullable Class<?> dflt,
        boolean includePrimitiveTypes
    ) {
        Class<?> clazz;
        if (cls == null)
            clazz = dflt;
        else if (!includePrimitiveTypes || cls.length() > 7 || (clazz = primitiveMap.get(cls)) == null) {
            try {
                clazz = Class.forName(cls);
            }
            catch (ClassNotFoundException ignore) {
                clazz = dflt;
            }
        }
        return clazz;
    }

    /**
     * Creates new instance of a class only if it has an empty constructor (can be non-public).
     *
     * @param cls Class name.
     * @return Instance.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public static <T> T newInstance(String cls) throws IgniteCheckedException {
        Class<?> cls0;

        try {
            cls0 = Class.forName(cls);
        }
        catch (Exception e) {
            throw new IgniteCheckedException(e);
        }

        return (T)newInstance(cls0);
    }

    /**
     * Creates new instance of a class only if it has an empty constructor (can be non-public).
     *
     * @param cls Class to instantiate.
     * @return New instance of the class or {@code null} if empty constructor could not be assigned.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public static <T> T newInstance(Class<T> cls) throws IgniteCheckedException {
        boolean set = false;

        Constructor<T> ctor = null;

        try {
            ctor = cls.getDeclaredConstructor();

            if (ctor == null)
                return null;

            if (!ctor.isAccessible()) {
                ctor.setAccessible(true);

                set = true;
            }

            return ctor.newInstance();
        }
        catch (NoSuchMethodException e) {
            throw new IgniteCheckedException("Failed to find empty constructor for class: " + cls, e);
        }
        catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
            throw new IgniteCheckedException("Failed to create new instance for class: " + cls, e);
        }
        finally {
            if (ctor != null && set)
                ctor.setAccessible(false);
        }
    }

    /**
     * Check whether class is in classpath.
     *
     * @return {@code True} if in classpath.
     */
    public static boolean inClassPath(String clsName) {
        try {
            Class.forName(clsName);

            return true;
        }
        catch (ClassNotFoundException ignore) {
            return false;
        }
    }

    /**
     * Creates new instance of a class even if it does not have public constructor.
     *
     * @param cls Class to instantiate.
     * @return New instance of the class or {@code null} if empty constructor could not be assigned.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public static <T> T forceNewInstance(Class<?> cls) throws IgniteCheckedException {
        Constructor ctor = forceEmptyConstructor(cls);

        if (ctor == null)
            return null;

        boolean set = false;

        try {

            if (!ctor.isAccessible()) {
                ctor.setAccessible(true);

                set = true;
            }

            return (T)ctor.newInstance();
        }
        catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
            throw new IgniteCheckedException("Failed to create new instance for class: " + cls, e);
        }
        finally {
            if (set)
                ctor.setAccessible(false);
        }
    }

    /**
     * Pretty-formatting for minutes.
     *
     * @param mins Minutes to format.
     * @return Formatted presentation of minutes.
     */
    public static String formatMins(long mins) {
        assert mins >= 0;

        if (mins == 0)
            return "< 1 min";

        SB sb = new SB();

        long dd = mins / 1440; // 1440 mins = 60 mins * 24 hours

        if (dd > 0)
            sb.a(dd).a(dd == 1 ? " day " : " days ");

        mins %= 1440;

        long hh = mins / 60;

        if (hh > 0)
            sb.a(hh).a(hh == 1 ? " hour " : " hours ");

        mins %= 60;

        if (mins > 0)
            sb.a(mins).a(mins == 1 ? " min " : " mins ");

        return sb.toString().trim();
    }

    /**
     * Gets 8-character substring of UUID (for terse logging).
     *
     * @param id Input ID.
     * @return 8-character ID substring.
     */
    public static String id8(UUID id) {
        return id.toString().substring(0, 8);
    }

    /**
     * Gets 8-character substring of {@link org.apache.ignite.lang.IgniteUuid} (for terse logging).
     * The ID8 will be constructed as follows:
     * <ul>
     * <li>Take first 4 digits for global ID, i.e. {@link IgniteUuid#globalId()}.</li>
     * <li>Take last 4 digits for local ID, i.e. {@link IgniteUuid#localId()}.</li>
     * </ul>
     *
     * @param id Input ID.
     * @return 8-character representation of {@link IgniteUuid}.
     */
    public static String id8(IgniteUuid id) {
        String s = id.toString();

        return s.substring(0, 4) + s.substring(s.length() - 4);
    }

    /**
     *
     * @param len Number of characters to fill in.
     * @param ch Character to fill with.
     * @return String.
     */
    public static String filler(int len, char ch) {
        char[] a = new char[len];

        Arrays.fill(a, ch);

        return new String(a);
    }

    /**
     * Writes array to output stream.
     *
     * @param out Output stream.
     * @param arr Array to write.
     * @param <T> Array type.
     * @throws IOException If failed.
     */
    public static <T> void writeArray(ObjectOutput out, T[] arr) throws IOException {
        int len = arr == null ? 0 : arr.length;

        out.writeInt(len);

        if (arr != null && arr.length > 0)
            for (T t : arr)
                out.writeObject(t);
    }

    /**
     * Reads array from input stream.
     *
     * @param in Input stream.
     * @return Deserialized array.
     * @throws IOException If failed.
     * @throws ClassNotFoundException If class not found.
     */
    @Nullable public static Object[] readArray(ObjectInput in) throws IOException, ClassNotFoundException {
        int len = in.readInt();

        Object[] arr = null;

        if (len > 0) {
            arr = new Object[len];

            for (int i = 0; i < len; i++)
                arr[i] = in.readObject();
        }

        return arr;
    }

    /**
     * Reads array from input stream.
     *
     * @param in Input stream.
     * @return Deserialized array.
     * @throws IOException If failed.
     * @throws ClassNotFoundException If class not found.
     */
    @Nullable public static Class<?>[] readClassArray(ObjectInput in) throws IOException, ClassNotFoundException {
        int len = in.readInt();

        Class<?>[] arr = null;

        if (len > 0) {
            arr = new Class<?>[len];

            for (int i = 0; i < len; i++)
                arr[i] = (Class<?>)in.readObject();
        }

        return arr;
    }

    /**
     *
     * @param out Output.
     * @param col Set to write.
     * @throws IOException If write failed.
     */
    public static void writeCollection(ObjectOutput out, Collection<?> col) throws IOException {
        if (col != null) {
            out.writeInt(col.size());

            for (Object o : col)
                out.writeObject(o);
        }
        else
            out.writeInt(-1);
    }

    /**
     *
     * @param out Output.
     * @param col Set to write.
     * @throws IOException If write failed.
     */
    public static void writeIntCollection(DataOutput out, Collection<Integer> col) throws IOException {
        if (col != null) {
            out.writeInt(col.size());

            for (Integer i : col)
                out.writeInt(i);
        }
        else
            out.writeInt(-1);
    }

    /**
     * @param in Input.
     * @return Deserialized set.
     * @throws IOException If deserialization failed.
     * @throws ClassNotFoundException If deserialized class could not be found.
     */
    @Nullable public static <E> Collection<E> readCollection(ObjectInput in)
        throws IOException, ClassNotFoundException {
        return readList(in);
    }

    /**
     * @param in Input.
     * @return Deserialized set.
     * @throws IOException If deserialization failed.
     */
    @Nullable public static Collection<Integer> readIntCollection(DataInput in) throws IOException {
        int size = in.readInt();

        // Check null flag.
        if (size == -1)
            return null;

        Collection<Integer> col = new ArrayList<>(size);

        for (int i = 0; i < size; i++)
            col.add(in.readInt());

        return col;
    }

    /**
     *
     * @param m Map to copy.
     * @param <K> Key type.
     * @param <V> Value type
     * @return Copied map.
     */
    public static <K, V> Map<K, V> copyMap(Map<K, V> m) {
        return new HashMap<>(m);
    }

    /**
     *
     * @param m Map to seal.
     * @param <K> Key type.
     * @param <V> Value type
     * @return Sealed map.
     */
    public static <K, V> Map<K, V> sealMap(Map<K, V> m) {
        assert m != null;

        return Collections.unmodifiableMap(new HashMap<>(m));
    }

    /**
     * Seal collection.
     *
     * @param c Collection to seal.
     * @param <E> Entry type
     * @return Sealed collection.
     */
    public static <E> Set<E> sealSet(Collection<E> c) {
        return Collections.unmodifiableSet(new HashSet<>(c));
    }

    /**
     * Seal collection.
     *
     * @param c Collection to seal.
     * @param <E> Entry type
     * @return Sealed collection.
     */
    public static <E> List<E> sealList(Collection<E> c) {
        return Collections.unmodifiableList(new ArrayList<>(c));
    }

    /**
     * Convert array to seal list.
     *
     * @param a Array for convert to seal list.
     * @param <E> Entry type
     * @return Sealed collection.
     */
    public static <E> List<E> sealList(E... a) {
        return Collections.unmodifiableList(Arrays.asList(a));
    }

    /**
     * Gets display name of the network interface this IP address belongs to.
     *
     * @param addr IP address for which to find network interface name.
     * @return Network interface name or {@code null} if can't be found.
     */
    @Nullable public static String getNetworkInterfaceName(String addr) {
        assert addr != null;

        try {
            InetAddress inetAddr = InetAddress.getByName(addr);

            for (NetworkInterface itf : asIterable(INTERFACE_SUPPLIER.getInterfaces()))
                for (InetAddress itfAddr : asIterable(itf.getInetAddresses()))
                    if (itfAddr.equals(inetAddr))
                        return itf.getDisplayName();
        }
        catch (UnknownHostException ignore) {
            return null;
        }
        catch (SocketException ignore) {
            return null;
        }

        return null;
    }

    /**
     * Tries to resolve host by name, returning local host if input is empty.
     * This method reflects how {@link org.apache.ignite.configuration.IgniteConfiguration#getLocalHost()} should
     * be handled in most places.
     *
     * @param hostName Hostname or {@code null} if local host should be returned.
     * @return Address of given host or of localhost.
     * @throws IOException If attempt to get local host failed.
     */
    public static InetAddress resolveLocalHost(@Nullable String hostName) throws IOException {
        return F.isEmpty(hostName) ?
            // Should default to InetAddress#anyLocalAddress which is package-private.
            new InetSocketAddress(0).getAddress() :
            InetAddress.getByName(hostName);
    }

    /**
     * Determines whether current local host is different from previously cached.
     *
     * @return {@code true} or {@code false} depending on whether or not local host
     *      has changed from the cached value.
     * @throws IOException If attempt to get local host failed.
     */
    public static synchronized boolean isLocalHostChanged() throws IOException {
        InetAddress locHost0 = locHost;

        return locHost0 != null && !resetLocalHost().equals(locHost0);
    }

    /**
     * @param addrs Addresses.
     * @return List of reachable addresses.
     */
    public static List<InetAddress> filterReachable(Collection<InetAddress> addrs) {
        if (addrs.isEmpty())
            return Collections.emptyList();

        final int reachTimeout = 2000;

        if (addrs.size() == 1) {
            InetAddress addr = F.first(addrs);

            if (reachable(addr, reachTimeout))
                return Collections.singletonList(addr);

            return Collections.emptyList();
        }

        final List<InetAddress> res = new ArrayList<>(addrs.size());

        Collection<Future<?>> futs = new ArrayList<>(addrs.size());

        ExecutorService executor = Executors.newFixedThreadPool(Math.min(10, addrs.size()),
            new IgniteThreadFactory("utils", "reachable"));

        try {
            for (final InetAddress addr : addrs) {
                futs.add(executor.submit(new Runnable() {
                    @Override public void run() {
                        if (reachable(addr, reachTimeout)) {
                            synchronized (res) {
                                res.add(addr);
                            }
                        }
                    }
                }));
            }

            for (Future<?> fut : futs) {
                try {
                    fut.get();
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();

                    throw new IgniteException("Thread has been interrupted.", e);
                }
                catch (ExecutionException e) {
                    throw new IgniteException(e);
                }
            }
        }
        finally {
            executor.shutdown();
        }

        return res;
    }

    /**
     * Returns host names consistent with {@link #resolveLocalHost(String)}. So when it returns
     * a common address this method returns single host name, and when a wildcard address passed
     * this method tries to collect addresses of all available interfaces.
     *
     * @param locAddr Local address to resolve.
     * @return Resolved available addresses of given local address.
     * @throws IOException If failed.
     */
    public static IgniteBiTuple<Collection<String>, Collection<String>> resolveLocalAddresses(InetAddress locAddr)
        throws IOException {
        return resolveLocalAddresses(locAddr, false);
    }

    /**
     * Returns host names consistent with {@link #resolveLocalHost(String)}. So when it returns
     * a common address this method returns single host name, and when a wildcard address passed
     * this method tries to collect addresses of all available interfaces.
     *
     * @param locAddr Local address to resolve.
     * @param allHostNames If {@code true} then include host names for all addresses.
     * @return Resolved available addresses and host names of given local address.
     * @throws IOException If failed.
     */
    public static IgniteBiTuple<Collection<String>, Collection<String>> resolveLocalAddresses(
        InetAddress locAddr,
        boolean allHostNames
    ) throws IOException {
        return resolveLocalAddresses(locAddr, allHostNames, null);
    }

    /**
     * Returns host names consistent with {@link #resolveLocalHost(String)}. So when it returns
     * a common address this method returns single host name, and when a wildcard address passed
     * this method tries to collect addresses of all available interfaces.
     *
     * @param locAddr Local address to resolve.
     * @param allHostNames If {@code true} then include host names for all addresses.
     * @param filter Optinal filter that allows to filter network interfaces when {@code locAddr} is wildcard.
     * @return Resolved available addresses and host names of given local address.
     * @throws IOException If failed.
     */
    public static IgniteBiTuple<Collection<String>, Collection<String>> resolveLocalAddresses(
        InetAddress locAddr,
        boolean allHostNames,
        @Nullable IgnitePredicate<InetAddress> filter
    ) throws IOException {
        assert locAddr != null;

        Collection<String> addrs = new ArrayList<>();
        Collection<String> hostNames = new ArrayList<>();

        if (locAddr.isAnyLocalAddress()) {
            IgniteBiTuple<Collection<String>, Collection<String>> res =
                allHostNames ? cachedLocalAddrAllHostNames : cachedLocalAddr;

            if (res == null) {
                List<InetAddress> locAddrs = new ArrayList<>();

                for (NetworkInterface itf : asIterable(INTERFACE_SUPPLIER.getInterfaces())) {
                    for (InetAddress addr : asIterable(itf.getInetAddresses())) {
                        if (!addr.isLinkLocalAddress())
                            locAddrs.add(addr);
                    }
                }

                /* On z/OS NetworkInterface::getNetworkInterfaces returns external interfaces
                    and IPv6 loopback but not IPv4 loopback.
                    Trying to additionally resolve "localhost" to find the IPv4 loopback address. */
                try {
                    InetAddress localHost = InetAddress.getByName(LOCALHOST);

                    if (!locAddrs.contains(localHost))
                        locAddrs.add(localHost);
                }
                catch (Exception ex) {
                    // Ignore.
                }

                locAddrs = filterReachable(locAddrs);

                for (InetAddress addr : locAddrs) {
                    if (filter == null || filter.apply(addr))
                        addresses(addr, addrs, hostNames, allHostNames);
                }

                if (F.isEmpty(addrs))
                    return F.t(Collections.emptyList(), Collections.emptyList());

                res = F.t(addrs, hostNames);

                if (allHostNames)
                    cachedLocalAddrAllHostNames = res;
                else
                    cachedLocalAddr = res;
            }

            return res;
        }

        addresses(locAddr, addrs, hostNames, allHostNames);

        return F.t(addrs, hostNames);
    }

    /**
     * @param addr Address.
     * @param addrs Addresses.
     * @param allHostNames If {@code true} then include host names for all addresses.
     * @param hostNames Host names.
     */
    private static void addresses(InetAddress addr, Collection<String> addrs, Collection<String> hostNames,
        boolean allHostNames) {
        String ipAddr = addr.getHostAddress();

        addrs.add(ipAddr);

        boolean ignoreLocalHostName = getBoolean(IGNITE_IGNORE_LOCAL_HOST_NAME, true);

        String userDefinedLocalHost = getString(IGNITE_LOCAL_HOST);

        // If IGNITE_LOCAL_HOST is defined and IGNITE_IGNORE_LOCAL_HOST_NAME is not false, then ignore local address's hostname
        if (!F.isEmpty(userDefinedLocalHost) && ignoreLocalHostName)
            return;

        String hostName = addr.getHostName();

        if (allHostNames)
            hostNames.add(hostName);
        else if (!F.isEmpty(hostName) && !addr.isLoopbackAddress())
            hostNames.add(hostName);
    }

    /**
     * Gets local host. Implementation will first attempt to get a non-loopback
     * address. If that fails, then loopback address will be returned.
     * <p>
     * Note that this method is synchronized to make sure that local host
     * initialization happens only once.
     *
     * @return Address representing local host.
     * @throws IOException If attempt to get local host failed.
     */
    public static synchronized InetAddress getLocalHost() throws IOException {
        if (locHost == null)
            // Cache it.
            resetLocalHost();

        return locHost;
    }

    /**
     * @return Local host.
     * @throws IOException If attempt to get local host failed.
     */
    private static synchronized InetAddress resetLocalHost() throws IOException {
        locHost = null;

        String sysLocHost = IgniteSystemProperties.getString(IGNITE_LOCAL_HOST);

        if (sysLocHost != null)
            sysLocHost = sysLocHost.trim();

        if (!F.isEmpty(sysLocHost))
            locHost = InetAddress.getByName(sysLocHost);
        else {
            List<NetworkInterface> itfs = new ArrayList<>();

            for (NetworkInterface itf : asIterable(INTERFACE_SUPPLIER.getInterfaces()))
                itfs.add(itf);

            Collections.sort(itfs, new Comparator<NetworkInterface>() {
                @Override public int compare(NetworkInterface itf1, NetworkInterface itf2) {
                    boolean itf1ForceOrdering = itf1.getName().charAt(0) < 'e';
                    boolean itf2ForceOrdering = itf2.getName().charAt(0) < 'e';

                    // Interfaces whose name start with letter before 'e' should go last.
                    // This helps avoiding interfaces like "docker0" to be recognized as local host.
                    if (itf1ForceOrdering != itf2ForceOrdering)
                        return itf1ForceOrdering ? 1 : -1;

                    return itf1.getName().compareTo(itf2.getName());
                }
            });

            // It should not take longer than 2 seconds to reach
            // local address on any network.
            int reachTimeout = 2000;

            for (NetworkInterface itf : itfs) {
                boolean found = false;

                for (InetAddress addr : asIterable(itf.getInetAddresses())) {
                    if (!addr.isLoopbackAddress() && !addr.isLinkLocalAddress() && reachable(itf, addr, reachTimeout)) {
                        locHost = addr;

                        found = true;

                        break;
                    }
                }

                if (found)
                    break;
            }
        }

        if (locHost == null)
            locHost = InetAddress.getLocalHost();

        return locHost;
    }

    /**
     * Checks if address can be reached using three argument InetAddress.isReachable() version.
     *
     * @param itf Network interface to use for test.
     * @param addr Address to check.
     * @param reachTimeout Timeout for the check.
     * @return {@code True} if address is reachable.
     */
    public static boolean reachable(NetworkInterface itf, InetAddress addr, int reachTimeout) {
        try {
            return addr.isReachable(itf, 0, reachTimeout);
        }
        catch (IOException ignore) {
            return false;
        }
    }

    /**
     * Checks if address can be reached using one argument InetAddress.isReachable() version.
     *
     * @param addr Address to check.
     * @param reachTimeout Timeout for the check.
     * @return {@code True} if address is reachable.
     */
    public static boolean reachable(InetAddress addr, int reachTimeout) {
        try {
            return addr.isReachable(reachTimeout);
        }
        catch (IOException ignore) {
            return false;
        }
    }

    /**
     * @param loc Local node.
     * @param rmt Remote node.
     * @return Whether given nodes have the same macs.
     */
    public static boolean sameMacs(ClusterNode loc, ClusterNode rmt) {
        assert loc != null;
        assert rmt != null;

        String locMacs = loc.attribute(IgniteNodeAttributes.ATTR_MACS);
        String rmtMacs = rmt.attribute(IgniteNodeAttributes.ATTR_MACS);

        return locMacs != null && locMacs.equals(rmtMacs);
    }

    /**
     * Gets a list of all local non-loopback IPs known to this JVM.
     * Note that this will include both IPv4 and IPv6 addresses (even if one "resolves"
     * into another). Loopbacks will be skipped.
     *
     * @return List of all known local IPs (empty list if no addresses available).
     */
    public static synchronized Collection<String> allLocalIps() {
        List<String> ips = new ArrayList<>(4);

        try {
            Enumeration<NetworkInterface> itfs = INTERFACE_SUPPLIER.getInterfaces();

            if (itfs != null) {
                for (NetworkInterface itf : asIterable(itfs)) {
                    if (!itf.isLoopback()) {
                        Enumeration<InetAddress> addrs = itf.getInetAddresses();

                        for (InetAddress addr : asIterable(addrs)) {
                            String hostAddr = addr.getHostAddress();

                            if (!addr.isLoopbackAddress() && !ips.contains(hostAddr))
                                ips.add(hostAddr);
                        }
                    }
                }
            }
        }
        catch (SocketException ignore) {
            return Collections.emptyList();
        }

        Collections.sort(ips);

        return ips;
    }

    /**
     * Checks if the address is local.
     *
     * @param addr Address for check.
     * @return true if address is local, otherwise false
     */
    public static boolean isLocalAddress(InetAddress addr) {
        // Check if the address is a valid special local or loop back
        if (addr.isAnyLocalAddress() || addr.isLoopbackAddress())
            return true;

        // Check if the address is defined on any interface
        try {
            return NetworkInterface.getByInetAddress(addr) != null;
        }
        catch (SocketException e) {
            return false;
        }
    }

    /**
     * Gets a list of all local enabled MACs known to this JVM. It
     * is using hardware address of the network interface that is not guaranteed to be
     * MAC addresses (but in most cases it is).
     * <p>
     * Note that if network interface is disabled - its MAC won't be included. All
     * local network interfaces are probed including loopbacks. Virtual interfaces
     * (sub-interfaces) are skipped.
     * <p>
     * Note that on linux getHardwareAddress() can return null from time to time
     * if NetworkInterface.getHardwareAddress() method is called from many threads.
     *
     * @return List of all known enabled local MACs or empty list
     *      if no MACs could be found.
     */
    public static synchronized Collection<String> allLocalMACs() {
        List<String> macs = new ArrayList<>(3);

        try {
            Enumeration<NetworkInterface> itfs = INTERFACE_SUPPLIER.getInterfaces();

            if (itfs != null) {
                for (NetworkInterface itf : asIterable(itfs)) {
                    byte[] hwAddr = itf.getHardwareAddress();

                    // Loopback produces empty MAC.
                    if (hwAddr != null && hwAddr.length > 0) {
                        String mac = byteArray2HexString(hwAddr);

                        if (!macs.contains(mac))
                            macs.add(mac);
                    }
                }
            }
        }
        catch (SocketException ignore) {
            return Collections.emptyList();
        }

        Collections.sort(macs);

        return macs;
    }

    /**
     * Sort addresses: IPv4 & real addresses first.
     *
     * @param addrs Addresses to sort.
     * @return Sorted list.
     */
    public static Collection<String> sortAddresses(Collection<String> addrs) {
        if (F.isEmpty(addrs))
            return Collections.emptyList();

        int sz = addrs.size();

        List<SortableAddress> sorted = new ArrayList<>(sz);

        for (String addr : addrs)
            sorted.add(new SortableAddress(addr));

        Collections.sort(sorted);

        Collection<String> res = new ArrayList<>(sz);

        for (SortableAddress sa : sorted)
            res.add(sa.address());

        return res;
    }

    /**
     * Downloads resource by URL.
     *
     * @param url URL to download.
     * @param file File where downloaded resource should be stored.
     * @return File where downloaded resource should be stored.
     * @throws IOException If error occurred.
     */
    public static File downloadUrl(URL url, File file) throws IOException {
        assert url != null;
        assert file != null;

        InputStream in = null;
        OutputStream out = null;

        try {
            URLConnection conn = url.openConnection();

            if (conn instanceof HttpsURLConnection) {
                HttpsURLConnection https = (HttpsURLConnection)conn;

                https.setHostnameVerifier(new DeploymentHostnameVerifier());

                SSLContext ctx = SSLContext.getInstance(HTTPS_PROTOCOL);

                ctx.init(null, getTrustManagers(), null);

                // Initialize socket factory.
                https.setSSLSocketFactory(ctx.getSocketFactory());
            }

            in = conn.getInputStream();

            if (in == null)
                throw new IOException("Failed to open connection: " + url.toString());

            out = new BufferedOutputStream(new FileOutputStream(file));

            copy(in, out);
        }
        catch (NoSuchAlgorithmException | KeyManagementException e) {
            throw new IOException("Failed to open HTTPs connection [url=" + url.toString() + ", msg=" + e + ']', e);
        }
        finally {
            close(in, null);
            close(out, null);
        }

        return file;
    }

    /**
     * Construct array with one trust manager which don't reject input certificates.
     *
     * @return Array with one X509TrustManager implementation of trust manager.
     */
    private static TrustManager[] getTrustManagers() {
        return new TrustManager[] {
            new X509TrustManager() {
                @Nullable @Override public X509Certificate[] getAcceptedIssuers() {
                    return null;
                }

                @Override public void checkClientTrusted(X509Certificate[] certs, String authType) {
                    /* No-op. */
                }

                @Override public void checkServerTrusted(X509Certificate[] certs, String authType) {
                    /* No-op. */
                }
            }
        };
    }

    /**
     * Replace password in URI string with a single '*' character.
     * <p>
     * Parses given URI by applying &quot;.*://(.*:.*)@.*&quot;
     * regular expression pattern and than if URI matches it
     * replaces password strings between '/' and '@' with '*'.
     *
     * @param uri URI which password should be replaced.
     * @return Converted URI string
     */
    @Nullable public static String hidePassword(@Nullable String uri) {
        if (uri == null)
            return null;

        if (Pattern.matches(".*://(.*:.*)@.*", uri)) {
            int userInfoLastIdx = uri.indexOf('@');

            assert userInfoLastIdx != -1;

            String str = uri.substring(0, userInfoLastIdx);

            int userInfoStartIdx = str.lastIndexOf('/');

            str = str.substring(userInfoStartIdx + 1);

            String[] params = str.split(";");

            StringBuilder builder = new StringBuilder();

            for (int i = 0; i < params.length; i++) {
                int idx;

                if ((idx = params[i].indexOf(':')) != -1)
                    params[i] = params[i].substring(0, idx + 1) + '*';

                builder.append(params[i]);

                if (i != params.length - 1)
                    builder.append(';');
            }

            return new StringBuilder(uri).replace(userInfoStartIdx + 1, userInfoLastIdx,
                builder.toString()).toString();
        }

        return uri;
    }

    /**
     * @return Class loader used to load Ignite itself.
     */
    public static ClassLoader gridClassLoader() {
        return gridClassLoader;
    }

    /**
     * @return ClassLoader at IgniteConfiguration in case it is not null or
     * ClassLoader used to start Ignite.
     */
    public static ClassLoader resolveClassLoader(IgniteConfiguration cfg) {
        return resolveClassLoader(null, cfg);
    }

    /**
     * @return ClassLoader passed as param in case it is not null or
     * ClassLoader at IgniteConfiguration in case it is not null or
     * ClassLoader used to start Ignite.
     */
    public static ClassLoader resolveClassLoader(ClassLoader ldr, IgniteConfiguration cfg) {
        assert cfg != null;

        return (ldr != null && ldr != gridClassLoader) ?
            ldr :
            cfg.getClassLoader() != null ?
                cfg.getClassLoader() :
                gridClassLoader;
    }

    /**
     * @param parent Parent to find.
     * @param ldr Loader to check.
     * @return {@code True} if parent found.
     */
    public static boolean hasParent(@Nullable ClassLoader parent, ClassLoader ldr) {
        if (parent != null) {
            for (; ldr != null; ldr = ldr.getParent()) {
                if (ldr.equals(parent))
                    return true;
            }

            return false;
        }

        return true;
    }

    /**
     * Writes collection of byte arrays to data output.
     *
     * @param out Output to write to.
     * @param bytes Collection with byte arrays.
     * @throws java.io.IOException If write failed.
     */
    public static void writeBytesCollection(DataOutput out, Collection<byte[]> bytes) throws IOException {
        if (bytes != null) {
            out.writeInt(bytes.size());

            for (byte[] b : bytes)
                writeByteArray(out, b);
        }
        else
            out.writeInt(-1);
    }

    /**
     * Reads collection of byte arrays from data input.
     *
     * @param in Data input to read from.
     * @return List of byte arrays.
     * @throws java.io.IOException If read failed.
     */
    public static List<byte[]> readBytesList(DataInput in) throws IOException {
        int size = in.readInt();

        if (size < 0)
            return null;

        List<byte[]> res = new ArrayList<>(size);

        for (int i = 0; i < size; i++)
            res.add(readByteArray(in));

        return res;
    }

    /**
     * Writes byte array to output stream accounting for <tt>null</tt> values.
     *
     * @param out Output stream to write to.
     * @param arr Array to write, possibly <tt>null</tt>.
     * @throws java.io.IOException If write failed.
     */
    public static void writeByteArray(DataOutput out, @Nullable byte[] arr) throws IOException {
        if (arr == null)
            out.writeInt(-1);
        else {
            out.writeInt(arr.length);

            out.write(arr);
        }
    }

    /**
     * Writes byte array to output stream accounting for <tt>null</tt> values.
     *
     * @param out Output stream to write to.
     * @param arr Array to write, possibly <tt>null</tt>.
     * @throws java.io.IOException If write failed.
     */
    public static void writeByteArray(DataOutput out, @Nullable byte[] arr, int maxLen) throws IOException {
        if (arr == null)
            out.writeInt(-1);
        else {
            int len = Math.min(arr.length, maxLen);

            out.writeInt(len);

            out.write(arr, 0, len);
        }
    }

    /**
     * Reads byte array from input stream accounting for <tt>null</tt> values.
     *
     * @param in Stream to read from.
     * @return Read byte array, possibly <tt>null</tt>.
     * @throws java.io.IOException If read failed.
     */
    @Nullable public static byte[] readByteArray(DataInput in) throws IOException {
        int len = in.readInt();

        if (len == -1)
            return null; // Value "-1" indicates null.

        byte[] res = new byte[len];

        in.readFully(res);

        return res;
    }

    /**
     * Reads byte array from given buffers (changing buffer positions).
     *
     * @param bufs Byte buffers.
     * @return Byte array.
     */
    public static byte[] readByteArray(ByteBuffer... bufs) {
        assert !F.isEmpty(bufs);

        int size = 0;

        for (ByteBuffer buf : bufs)
            size += buf.remaining();

        byte[] res = new byte[size];

        int off = 0;

        for (ByteBuffer buf : bufs) {
            int len = buf.remaining();

            if (len != 0) {
                buf.get(res, off, len);

                off += len;
            }
        }

        assert off == res.length;

        return res;
    }

    /**
     * // FIXME: added for DR dataCenterIds, review if it is needed after GG-6879.
     *
     * @param out Output.
     * @param col Set to write.
     * @throws java.io.IOException If write failed.
     */
    public static void writeByteCollection(DataOutput out, Collection<Byte> col) throws IOException {
        if (col != null) {
            out.writeInt(col.size());

            for (Byte i : col)
                out.writeByte(i);
        }
        else
            out.writeInt(-1);
    }

    /**
     * // FIXME: added for DR dataCenterIds, review if it is needed after GG-6879.
     *
     * @param in Input.
     * @return Deserialized list.
     * @throws java.io.IOException If deserialization failed.
     */
    @Nullable public static List<Byte> readByteList(DataInput in) throws IOException {
        int size = in.readInt();

        // Check null flag.
        if (size == -1)
            return null;

        List<Byte> col = new ArrayList<>(size);

        for (int i = 0; i < size; i++)
            col.add(in.readByte());

        return col;
    }

    /**
     * Join byte arrays into single one.
     *
     * @param bufs list of byte arrays to concatenate.
     * @return Concatenated byte's array.
     */
    public static byte[] join(byte[]... bufs) {
        int size = 0;
        for (byte[] buf : bufs) {
            size += buf.length;
        }

        byte[] res = new byte[size];
        int position = 0;
        for (byte[] buf : bufs) {
            arrayCopy(buf, 0, res, position, buf.length);
            position += buf.length;
        }

        return res;
    }

    /**
     * Converts byte array to formatted string. If calling:
     * <pre name="code" class="java">
     * ...
     * byte[] data = {10, 20, 30, 40, 50, 60, 70, 80, 90};
     *
     * U.byteArray2String(data, "0x%02X", ",0x%02X")
     * ...
     * </pre>
     * the result will be:
     * <pre name="code" class="java">
     * ...
     * 0x0A, 0x14, 0x1E, 0x28, 0x32, 0x3C, 0x46, 0x50, 0x5A
     * ...
     * </pre>
     *
     * @param arr Array of byte.
     * @param hdrFmt C-style string format for the first element.
     * @param bodyFmt C-style string format for second and following elements, if any.
     * @return String with converted bytes.
     */
    public static String byteArray2String(byte[] arr, String hdrFmt, String bodyFmt) {
        assert arr != null;
        assert hdrFmt != null;
        assert bodyFmt != null;

        SB sb = new SB();

        sb.a('{');

        boolean first = true;

        for (byte b : arr)
            if (first) {
                sb.a(String.format(hdrFmt, b));

                first = false;
            }
            else
                sb.a(String.format(bodyFmt, b));

        sb.a('}');

        return sb.toString();
    }

    /**
     * Convert string with hex values to byte array.
     *
     * @param hex Hexadecimal string to convert.
     * @return array of bytes defined as hex in string.
     * @throws IllegalArgumentException If input character differs from certain hex characters.
     */
    public static byte[] hexString2ByteArray(String hex) throws IllegalArgumentException {
        // If Hex string has odd character length.
        if (hex.length() % 2 != 0)
            hex = '0' + hex;

        char[] chars = hex.toCharArray();

        byte[] bytes = new byte[chars.length / 2];

        int byteCnt = 0;

        for (int i = 0; i < chars.length; i += 2) {
            int newByte = 0;

            newByte |= hexCharToByte(chars[i]);

            newByte <<= 4;

            newByte |= hexCharToByte(chars[i + 1]);

            bytes[byteCnt] = (byte)newByte;

            byteCnt++;
        }

        return bytes;
    }

    /**
     * Gets a hex string representation of the given long value.
     *
     * @param val Value to convert to string.
     * @return Hex string.
     */
    public static String hexLong(long val) {
        return new SB().appendHex(val).toString();
    }

    /**
     * Gets a hex string representation of the given long value.
     *
     * @param val Value to convert to string.
     * @return Hex string.
     */
    public static String hexInt(int val) {
        return new SB().appendHex(val).toString();
    }

    /**
     * Return byte value for certain character.
     *
     * @param ch Character
     * @return Byte value.
     * @throws IllegalArgumentException If input character differ from certain hex characters.
     */
    private static byte hexCharToByte(char ch) throws IllegalArgumentException {
        switch (ch) {
            case '0':
            case '1':
            case '2':
            case '3':
            case '4':
            case '5':
            case '6':
            case '7':
            case '8':
            case '9':
                return (byte)(ch - '0');

            case 'a':
            case 'A':
                return 0xa;

            case 'b':
            case 'B':
                return 0xb;

            case 'c':
            case 'C':
                return 0xc;

            case 'd':
            case 'D':
                return 0xd;

            case 'e':
            case 'E':
                return 0xe;

            case 'f':
            case 'F':
                return 0xf;

            default:
                throw new IllegalArgumentException("Hex decoding wrong input character [character=" + ch + ']');
        }
    }

    /**
     * Converts primitive double to byte array.
     *
     * @param d Double to convert.
     * @return Byte array.
     */
    public static byte[] doubleToBytes(double d) {
        return longToBytes(Double.doubleToLongBits(d));
    }

    /**
     * Converts primitive {@code double} type to byte array and stores
     * it in the specified byte array.
     *
     * @param d Double to convert.
     * @param bytes Array of bytes.
     * @param off Offset.
     * @return New offset.
     */
    public static int doubleToBytes(double d, byte[] bytes, int off) {
        return longToBytes(Double.doubleToLongBits(d), bytes, off);
    }

    /**
     * Converts primitive float to byte array.
     *
     * @param f Float to convert.
     * @return Array of bytes.
     */
    public static byte[] floatToBytes(float f) {
        return intToBytes(Float.floatToIntBits(f));
    }

    /**
     * Converts primitive float to byte array.
     *
     * @param f Float to convert.
     * @param bytes Array of bytes.
     * @param off Offset.
     * @return New offset.
     */
    public static int floatToBytes(float f, byte[] bytes, int off) {
        return intToBytes(Float.floatToIntBits(f), bytes, off);
    }

    /**
     * Converts primitive {@code long} type to byte array.
     *
     * @param l Long value.
     * @return Array of bytes.
     */
    public static byte[] longToBytes(long l) {
        return GridClientByteUtils.longToBytes(l);
    }

    /**
     * Converts primitive {@code long} type to byte array and stores it in specified
     * byte array.
     *
     * @param l Long value.
     * @param bytes Array of bytes.
     * @param off Offset in {@code bytes} array.
     * @return Number of bytes overwritten in {@code bytes} array.
     */
    public static int longToBytes(long l, byte[] bytes, int off) {
        return off + GridClientByteUtils.longToBytes(l, bytes, off);
    }

    /**
     * Converts primitive {@code int} type to byte array.
     *
     * @param i Integer value.
     * @return Array of bytes.
     */
    public static byte[] intToBytes(int i) {
        return GridClientByteUtils.intToBytes(i);
    }

    /**
     * Converts primitive {@code int} type to byte array and stores it in specified
     * byte array.
     *
     * @param i Integer value.
     * @param bytes Array of bytes.
     * @param off Offset in {@code bytes} array.
     * @return Number of bytes overwritten in {@code bytes} array.
     */
    public static int intToBytes(int i, byte[] bytes, int off) {
        return off + GridClientByteUtils.intToBytes(i, bytes, off);
    }

    /**
     * Converts primitive {@code short} type to byte array.
     *
     * @param s Short value.
     * @return Array of bytes.
     */
    public static byte[] shortToBytes(short s) {
        return GridClientByteUtils.shortToBytes(s);
    }

    /**
     * Converts primitive {@code short} type to byte array and stores it in specified
     * byte array.
     *
     * @param s Short value.
     * @param bytes Array of bytes.
     * @param off Offset in {@code bytes} array.
     * @return Number of bytes overwritten in {@code bytes} array.
     */
    public static int shortToBytes(short s, byte[] bytes, int off) {
        return off + GridClientByteUtils.shortToBytes(s, bytes, off);
    }

    /**
     * Encodes {@link java.util.UUID} into a sequence of bytes using the {@link java.nio.ByteBuffer},
     * storing the result into a new byte array.
     *
     * @param uuid Unique identifier.
     * @param arr Byte array to fill with result.
     * @param off Offset in {@code arr}.
     * @return Number of bytes overwritten in {@code bytes} array.
     */
    public static int uuidToBytes(@Nullable UUID uuid, byte[] arr, int off) {
        return off + GridClientByteUtils.uuidToBytes(uuid, arr, off);
    }

    /**
     * Converts an UUID to byte array.
     *
     * @param uuid UUID value.
     * @return Encoded into byte array {@link java.util.UUID}.
     */
    public static byte[] uuidToBytes(@Nullable UUID uuid) {
        return GridClientByteUtils.uuidToBytes(uuid);
    }

    /**
     * Constructs {@code short} from byte array.
     *
     * @param bytes Array of bytes.
     * @param off Offset in {@code bytes} array.
     * @return Short value.
     */
    public static short bytesToShort(byte[] bytes, int off) {
        assert bytes != null;

        int bytesCnt = Short.SIZE >> 3;

        if (off + bytesCnt > bytes.length)
            // Just use the remainder.
            bytesCnt = bytes.length - off;

        short res = 0;

        for (int i = 0; i < bytesCnt; i++) {
            int shift = bytesCnt - i - 1 << 3;

            res |= (0xffL & bytes[off++]) << shift;
        }

        return res;
    }

    /**
     * Constructs {@code int} from byte array.
     *
     * @param bytes Array of bytes.
     * @param off Offset in {@code bytes} array.
     * @return Integer value.
     */
    public static int bytesToInt(byte[] bytes, int off) {
        assert bytes != null;

        int bytesCnt = Integer.SIZE >> 3;

        if (off + bytesCnt > bytes.length)
            // Just use the remainder.
            bytesCnt = bytes.length - off;

        int res = 0;

        for (int i = 0; i < bytesCnt; i++) {
            int shift = bytesCnt - i - 1 << 3;

            res |= (0xffL & bytes[off++]) << shift;
        }

        return res;
    }

    /**
     * Constructs {@code long} from byte array.
     *
     * @param bytes Array of bytes.
     * @param off Offset in {@code bytes} array.
     * @return Long value.
     */
    public static long bytesToLong(byte[] bytes, int off) {
        assert bytes != null;

        int bytesCnt = Long.SIZE >> 3;

        if (off + bytesCnt > bytes.length)
            bytesCnt = bytes.length - off;

        long res = 0;

        for (int i = 0; i < bytesCnt; i++) {
            int shift = bytesCnt - i - 1 << 3;

            res |= (0xffL & bytes[off++]) << shift;
        }

        return res;
    }

    /**
     * Reads an {@link java.util.UUID} form byte array.
     * If given array contains all 0s then {@code null} will be returned.
     *
     * @param bytes array of bytes.
     * @param off Offset in {@code bytes} array.
     * @return UUID value or {@code null}.
     */
    public static UUID bytesToUuid(byte[] bytes, int off) {
        return GridClientByteUtils.bytesToUuid(bytes, off);
    }

    /**
     * Constructs double from byte array.
     *
     * @param bytes Byte array.
     * @param off Offset in {@code bytes} array.
     * @return Double value.
     */
    public static double bytesToDouble(byte[] bytes, int off) {
        return Double.longBitsToDouble(bytesToLong(bytes, off));
    }

    /**
     * Constructs float from byte array.
     *
     * @param bytes Byte array.
     * @param off Offset in {@code bytes} array.
     * @return Float value.
     */
    public static float bytesToFloat(byte[] bytes, int off) {
        return Float.intBitsToFloat(bytesToInt(bytes, off));
    }

    /**
     * Compares fragments of byte arrays.
     *
     * @param a First array.
     * @param aOff First array offset.
     * @param b Second array.
     * @param bOff Second array offset.
     * @param len Length of fragments.
     * @return {@code true} if fragments are equal, {@code false} otherwise.
     */
    public static boolean bytesEqual(byte[] a, int aOff, byte[] b, int bOff, int len) {
        if (aOff + len > a.length || bOff + len > b.length)
            return false;
        else {
            for (int i = 0; i < len; i++)
                if (a[aOff + i] != b[bOff + i])
                    return false;

            return true;
        }
    }

    /**
     * Converts an array of characters representing hexidecimal values into an
     * array of bytes of those same values. The returned array will be half the
     * length of the passed array, as it takes two characters to represent any
     * given byte. An exception is thrown if the passed char array has an odd
     * number of elements.
     *
     * @param data An array of characters containing hexidecimal digits
     * @return A byte array containing binary data decoded from
     *         the supplied char array.
     * @throws IgniteCheckedException Thrown if an odd number or illegal of characters is supplied.
     */
    public static byte[] decodeHex(char[] data) throws IgniteCheckedException {
        int len = data.length;

        if ((len & 0x01) != 0)
            throw new IgniteCheckedException("Odd number of characters.");

        byte[] out = new byte[len >> 1];

        // Two characters form the hex value.
        for (int i = 0, j = 0; j < len; i++) {
            int f = toDigit(data[j], j) << 4;

            j++;

            f |= toDigit(data[j], j);

            j++;

            out[i] = (byte)(f & 0xFF);
        }

        return out;
    }

    /**
     * @param bytes Number of bytes to display.
     * @param si If {@code true}, then unit base is 1000, otherwise unit base is 1024.
     * @return Formatted size.
     */
    public static String readableSize(long bytes, boolean si) {
        int unit = si ? 1000 : 1024;

        if (bytes < unit)
            return bytes + " B";

        int exp = (int)(Math.log(bytes) / Math.log(unit));

        String pre = (si ? "kMGTPE" : "KMGTPE").charAt(exp - 1) + (si ? "" : "i");

        return String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
    }

    /**
     * Verifier always returns successful result for any host.
     */
    private static class DeploymentHostnameVerifier implements HostnameVerifier {
        /** {@inheritDoc} */
        @Override public boolean verify(String hostname, SSLSession ses) {
            // Remote host trusted by default.
            return true;
        }
    }

    /**
     * Makes a {@code '+---+'} dash line.
     *
     * @param len Length of the dash line to make.
     * @return Dash line.
     */
    public static String dash(int len) {
        char[] dash = new char[len];

        Arrays.fill(dash, '-');

        dash[0] = dash[len - 1] = '+';

        return new String(dash);
    }

    /**
     * Creates space filled string of given length.
     *
     * @param len Number of spaces.
     * @return Space filled string of given length.
     */
    public static String pad(int len) {
        char[] dash = new char[len];

        Arrays.fill(dash, ' ');

        return new String(dash);
    }

    /**
     * Formats system time in milliseconds for printing in logs.
     *
     * @param sysTime System time.
     * @return Formatted time string.
     */
    public static String format(long sysTime) {
        return LONG_DATE_FMT.format(Instant.ofEpochMilli(sysTime));
    }

    /**
     * Converts enumeration to iterable so it can be used in {@code foreach} construct.
     *
     * @param <T> Types of instances for iteration.
     * @param e Enumeration to convert.
     * @return Iterable over the given enumeration.
     */
    public static <T> Iterable<T> asIterable(final Enumeration<T> e) {
        return new Iterable<T>() {
            @Override public Iterator<T> iterator() {
                return new Iterator<T>() {
                    @Override public boolean hasNext() {
                        return e.hasMoreElements();
                    }

                    @Override public T next() {
                        return e.nextElement();
                    }

                    @Override public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }

    /**
     * Copy source file (or folder) to destination file (or folder). Supported source & destination:
     * <ul>
     * <li>File to File</li>
     * <li>File to Folder</li>
     * <li>Folder to Folder (Copy the content of the directory and not the directory itself)</li>
     * </ul>
     *
     * @param src Source file or folder.
     * @param dest Destination file or folder.
     * @param overwrite Whether or not overwrite existing files and folders.
     * @throws IOException Thrown if an I/O error occurs.
     */
    public static void copy(File src, File dest, boolean overwrite) throws IOException {
        assert src != null;
        assert dest != null;

        /*
         * Supported source & destination:
         * ===============================
         * 1. File -> File
         * 2. File -> Directory
         * 3. Directory -> Directory
         */

        // Source must exist.
        if (!src.exists())
            throw new FileNotFoundException("Source can't be found: " + src);

        // Check that source and destination are not the same.
        if (src.getAbsoluteFile().equals(dest.getAbsoluteFile()))
            throw new IOException("Source and destination are the same [src=" + src + ", dest=" + dest + ']');

        if (dest.exists()) {
            if (!dest.isDirectory() && !overwrite)
                throw new IOException("Destination already exists: " + dest);

            if (!dest.canWrite())
                throw new IOException("Destination is not writable:" + dest);
        }
        else {
            File parent = dest.getParentFile();

            if (parent != null && !parent.exists())
                // Ignore any errors here.
                // We will get errors when we'll try to open the file stream.
                //noinspection ResultOfMethodCallIgnored
                parent.mkdirs();

            // If source is a directory, we should create destination directory.
            if (src.isDirectory())
                //noinspection ResultOfMethodCallIgnored
                dest.mkdir();
        }

        if (src.isDirectory()) {
            // In this case we have Directory -> Directory.
            // Note that we copy the content of the directory and not the directory itself.

            File[] files = src.listFiles();

            for (File file : files) {
                if (file.isDirectory()) {
                    File dir = new File(dest, file.getName());

                    if (!dir.exists() && !dir.mkdirs())
                        throw new IOException("Can't create directory: " + dir);

                    copy(file, dir, overwrite);
                }
                else
                    copy(file, dest, overwrite);
            }
        }
        else {
            // In this case we have File -> File or File -> Directory.
            File file = dest.exists() && dest.isDirectory() ? new File(dest, src.getName()) : dest;

            if (!overwrite && file.exists())
                throw new IOException("Destination already exists: " + file);

            FileInputStream in = null;
            FileOutputStream out = null;

            try {
                in = new FileInputStream(src);
                out = new FileOutputStream(file);

                copy(in, out);
            }
            finally {
                if (in != null)
                    in.close();

                if (out != null) {
                    out.getFD().sync();

                    out.close();
                }
            }
        }
    }

    /**
     * Starts clock timer if grid is first.
     */
    public static void onGridStart() {
        synchronized (mux) {
            if (gridCnt == 0) {
                assert timer == null;

                timer = new Thread(new Runnable() {
                    @SuppressWarnings({"BusyWait"})
                    @Override public void run() {
                        while (true) {
                            curTimeMillis = System.currentTimeMillis();

                            try {
                                Thread.sleep(10);
                            }
                            catch (InterruptedException ignored) {
                                break;
                            }
                        }
                    }
                }, "ignite-clock");

                timer.setDaemon(true);

                timer.setPriority(10);

                timer.start();
            }

            ++gridCnt;
        }
    }

    /**
     * Stops clock timer if all nodes into JVM were stopped.
     * @throws InterruptedException If interrupted.
     */
    public static void onGridStop() throws InterruptedException {
        synchronized (mux) {
            // Grid start may fail and onGridStart() does not get called.
            if (gridCnt == 0)
                return;

            --gridCnt;

            Thread timer0 = timer;

            if (gridCnt == 0 && timer0 != null) {
                timer = null;

                timer0.interrupt();

                timer0.join();
            }
        }
    }

    /**
     * Find file (by path or in resources) and open input stream to it.
     *
     * @param filePath Path to the file.
     * @return Opened input stream.
     * @throws IOException If stream could not be opened.
     */
    public static InputStream openFileInputStream(String filePath) throws IOException {
        File abs = new File(filePath);

        if (abs.exists())
            return new FileInputStream(abs);

        URL clsPthRes = IgniteUtils.class.getClassLoader().getResource(filePath);

        if (clsPthRes != null)
            return clsPthRes.openStream();

        throw new FileNotFoundException("File " + filePath + " not found");
    }

    /**
     * Copies input byte stream to output byte stream.
     *
     * @param in Input byte stream.
     * @param out Output byte stream.
     * @return Number of the copied bytes.
     * @throws IOException Thrown if an I/O error occurs.
     */
    public static int copy(InputStream in, OutputStream out) throws IOException {
        assert in != null;
        assert out != null;

        byte[] buf = new byte[BUF_SIZE];

        int cnt = 0;

        for (int n; (n = in.read(buf)) > 0; ) {
            out.write(buf, 0, n);

            cnt += n;
        }

        return cnt;
    }

    /**
     * Copies input character stream to output character stream.
     *
     * @param in Input character stream.
     * @param out Output character stream.
     * @return Number of the copied characters.
     * @throws IOException Thrown if an I/O error occurs.
     */
    public static int copy(Reader in, Writer out) throws IOException {
        assert in != null;
        assert out != null;

        char[] buf = new char[BUF_SIZE];

        int cnt = 0;

        for (int n; (n = in.read(buf)) > 0; ) {
            out.write(buf, 0, n);

            cnt += n;
        }

        return cnt;
    }

    /**
     * Writes string to file.
     *
     * @param file File.
     * @param s String to write.
     * @throws IOException Thrown if an I/O error occurs.
     */
    public static void writeStringToFile(File file, String s) throws IOException {
        writeStringToFile(file, s, Charset.defaultCharset().toString(), false);
    }

    /**
     * Writes string to file.
     *
     * @param file File.
     * @param s String to write.
     * @param charset Encoding.
     * @throws IOException Thrown if an I/O error occurs.
     */
    public static void writeStringToFile(File file, String s, String charset) throws IOException {
        writeStringToFile(file, s, charset, false);
    }

    /**
     * Reads file to string using specified charset.
     *
     * @param fileName File name.
     * @param charset File charset.
     * @return File content.
     * @throws IOException If error occurred.
     */
    public static String readFileToString(String fileName, String charset) throws IOException {
        try (Reader input = new InputStreamReader(new FileInputStream(fileName), charset)) {
            StringWriter output = new StringWriter();

            char[] buf = new char[4096];

            int n;

            while ((n = input.read(buf)) != -1)
                output.write(buf, 0, n);

            return output.toString();
        }
    }

    /**
     * Writes string to file.
     *
     * @param file File.
     * @param s String to write.
     * @param charset Encoding.
     * @param append If {@code true}, then specified string will be added to the end of the file.
     * @throws IOException Thrown if an I/O error occurs.
     */
    public static void writeStringToFile(File file, String s, String charset, boolean append) throws IOException {
        if (s == null)
            return;

        try (OutputStream out = new FileOutputStream(file, append)) {
            out.write(s.getBytes(charset));
        }
    }

    /**
     * Utility method that sets cause into exception and returns it.
     *
     * @param e Exception to set cause to and return.
     * @param cause Optional cause to set (if not {@code null}).
     * @param <E> Type of the exception.
     * @return Passed in exception with optionally set cause.
     */
    public static <E extends Throwable> E withCause(E e, @Nullable Throwable cause) {
        assert e != null;

        if (cause != null)
            e.initCause(cause);

        return e;
    }

    /**
     * Deletes file or directory with all sub-directories and files.
     *
     * @param file File or directory to delete.
     * @return {@code true} if and only if the file or directory is successfully deleted,
     *      {@code false} otherwise
     */
    public static boolean delete(@Nullable File file) {
        return delete(file, null);
    }

    /**
     * Deletes file or directory with all sub-directories and files.
     *
     * @param file File or directory to delete.
     * @param log Log errors when deleting files, {@code null} if not to be logged.
     * @return {@code true} if and only if the file or directory is successfully deleted,
     *      {@code false} otherwise
     */
    public static boolean delete(@Nullable File file, @Nullable IgniteLogger log) {
        return file != null && delete(file.toPath(), log);
    }

    /**
     * Converts size in bytes to human-readable size in megabytes.
     *
     * @param sizeInBytes Size of any object (file, memory region etc) in bytes.
     * @return Size converted to megabytes.
     */
    public static int sizeInMegabytes(long sizeInBytes) {
        return (int)(sizeInBytes / MB);
    }

    /**
     * Deletes file or directory with all sub-directories and files.
     *
     * @param path File or directory to delete.
     * @return {@code true} if and only if the file or directory is successfully deleted,
     *      {@code false} otherwise
     */
    public static boolean delete(Path path) {
        return delete(path, null);
    }

    /**
     * Deletes file or directory with all sub-directories and files.
     *
     * @param path File or directory to delete.
     * @param log Log errors when deleting files, {@code null} if not to be logged.
     * @return {@code true} if and only if the file or directory is successfully deleted,
     *      {@code false} otherwise
     */
    public static boolean delete(Path path, @Nullable IgniteLogger log) {
        if (Files.isDirectory(path)) {
            try {
                try (DirectoryStream<Path> stream = Files.newDirectoryStream(path)) {
                    for (Path innerPath : stream) {
                        boolean res = delete(innerPath, log);

                        if (!res)
                            return false;
                    }
                }
            } catch (IOException e) {
                if (log != null)
                    log.error("Failed to clear directory: " + path.toFile().getAbsolutePath(), e);

                return false;
            }
        }

        if (path.toFile().getName().endsWith("jar")) {
            try {
                // Why do we do this?
                new JarFile(path.toString(), false).close();
            }
            catch (IOException e) {
                // Just logging.
                if (log != null)
                    log.error("Failed to delete jar: " + path.toFile().getAbsolutePath(), e);
            }
        }

        try {
            Files.delete(path);

            return true;
        } catch (IOException e) {
            if (log != null)
                log.error("Failed to delete file: " + path.toFile().getAbsolutePath(), e);

            return false;
        }
    }

    /**
     * @param dir Directory to create along with all non-existent parent directories.
     * @return {@code True} if directory exists (has been created or already existed),
     *      {@code false} if has not been created and does not exist.
     */
    public static boolean mkdirs(File dir) {
        assert dir != null;

        return dir.mkdirs() || dir.exists();
    }

    /**
     * Resolve project home directory based on source code base.
     *
     * @return Project home directory (or {@code null} if it cannot be resolved).
     */
    @Nullable private static String resolveProjectHome() {
        assert Thread.holdsLock(IgniteUtils.class);

        // Resolve Ignite home via environment variables.
        String ggHome0 = IgniteSystemProperties.getString(IGNITE_HOME);

        if (!F.isEmpty(ggHome0))
            return ggHome0;

        String appWorkDir = System.getProperty("user.dir");

        if (appWorkDir != null) {
            ggHome0 = findProjectHome(new File(appWorkDir));

            if (ggHome0 != null)
                return ggHome0;
        }

        URI classesUri;

        Class<IgniteUtils> cls = IgniteUtils.class;

        try {
            ProtectionDomain domain = cls.getProtectionDomain();

            // Should not happen, but to make sure our code is not broken.
            if (domain == null || domain.getCodeSource() == null || domain.getCodeSource().getLocation() == null) {
                logResolveFailed(cls, null);

                return null;
            }

            // Resolve path to class-file.
            classesUri = domain.getCodeSource().getLocation().toURI();

            // Overcome UNC path problem on Windows (http://www.tomergabel.com/JavaMishandlesUNCPathsOnWindows.aspx)
            if (isWindows() && classesUri.getAuthority() != null)
                classesUri = new URI(classesUri.toString().replace("file://", "file:/"));
        }
        catch (URISyntaxException | SecurityException e) {
            logResolveFailed(cls, e);

            return null;
        }

        File classesFile;

        try {
            classesFile = new File(classesUri);
        }
        catch (IllegalArgumentException e) {
            logResolveFailed(cls, e);

            return null;
        }

        return findProjectHome(classesFile);
    }

    /**
     * Tries to find project home starting from specified directory and moving to root.
     *
     * @param startDir First directory in search hierarchy.
     * @return Project home path or {@code null} if it wasn't found.
     */
    private static String findProjectHome(File startDir) {
        for (File cur = startDir.getAbsoluteFile(); cur != null; cur = cur.getParentFile()) {
            // Check 'cur' is project home directory.
            if (!new File(cur, "bin").isDirectory() ||
                !new File(cur, "config").isDirectory())
                continue;

            return cur.getPath();
        }

        return null;
    }

    /**
     * @param cls Class.
     * @param e Exception.
     */
    private static void logResolveFailed(Class cls, Exception e) {
        warn(null, "Failed to resolve IGNITE_HOME automatically for class codebase " +
            "[class=" + cls + (e == null ? "" : ", e=" + e.getMessage()) + ']');
    }

    /**
     * Retrieves {@code IGNITE_HOME} property. The property is retrieved from system
     * properties or from environment in that order.
     *
     * @return {@code IGNITE_HOME} property.
     */
    @Nullable public static String getIgniteHome() {
        GridTuple<String> ggHomeTup = ggHome;

        String ggHome0;

        if (ggHomeTup == null) {
            synchronized (IgniteUtils.class) {
                // Double check.
                ggHomeTup = ggHome;

                if (ggHomeTup == null) {
                    // Resolve Ignite installation home directory.
                    ggHome = F.t(ggHome0 = resolveProjectHome());

                    if (ggHome0 != null)
                        System.setProperty(IGNITE_HOME, ggHome0);
                }
                else
                    ggHome0 = ggHomeTup.get();
            }
        }
        else
            ggHome0 = ggHomeTup.get();

        return ggHome0;
    }

    /**
     * @param path Ignite home. May be {@code null}.
     */
    public static void setIgniteHome(@Nullable String path) {
        GridTuple<String> ggHomeTup = ggHome;

        String ggHome0;

        if (ggHomeTup == null) {
            synchronized (IgniteUtils.class) {
                // Double check.
                ggHomeTup = ggHome;

                if (ggHomeTup == null) {
                    if (F.isEmpty(path))
                        System.clearProperty(IGNITE_HOME);
                    else
                        System.setProperty(IGNITE_HOME, path);

                    ggHome = F.t(path);

                    return;
                }
                else
                    ggHome0 = ggHomeTup.get();
            }
        }
        else
            ggHome0 = ggHomeTup.get();

        if (ggHome0 != null && !ggHome0.equals(path)) {
            try {
                Path path0 = new File(ggHome0).toPath();

                Path path1 = new File(path).toPath();

                if (!Files.isSameFile(path0, path1))
                    throw new IgniteException("Failed to set IGNITE_HOME after it has been already resolved " +
                        "[igniteHome=" + path0 + ", newIgniteHome=" + path1 + ']');
            }
            catch (IOException ignore) {
                // Throw an exception if failed to follow symlinks.
                throw new IgniteException("Failed to set IGNITE_HOME after it has been already resolved " +
                    "[igniteHome=" + ggHome0 + ", newIgniteHome=" + path + ']');
            }
        }
    }

    /**
     * Gets file associated with path.
     * <p>
     * First check if path is relative to {@code IGNITE_HOME}.
     * If not, check if path is absolute.
     * If all checks fail, then {@code null} is returned.
     * <p>
     * See {@link #getIgniteHome()} for information on how {@code IGNITE_HOME} is retrieved.
     *
     * @param path Path to resolve.
     * @return Resolved path as file, or {@code null} if path cannot be resolved.
     */
    @Nullable public static File resolveIgnitePath(String path) {
        assert path != null;

        /*
         * 1. Check relative to IGNITE_HOME specified in configuration, if any.
         */

        String home = getIgniteHome();

        if (home != null) {
            File file = new File(home, path);

            if (file.exists())
                return file;
        }

        /*
         * 2. Check given path as absolute.
         */

        File file = new File(path);

        if (file.exists())
            return file;

        return null;
    }

    /**
     * Gets URL representing the path passed in. First the check is made if path is absolute.
     * If not, then the check is made if path is relative to {@code META-INF} folder in classpath.
     * If not, then the check is made if path is relative to ${IGNITE_HOME}.
     * If all checks fail,
     * then {@code null} is returned, otherwise URL representing path is returned.
     * <p>
     * See {@link #getIgniteHome()} for information on how {@code IGNITE_HOME} is retrieved.
     *
     * @param path Path to resolve.
     * @return Resolved path as URL, or {@code null} if path cannot be resolved.
     * @see #getIgniteHome()
     */
    @Nullable public static URL resolveIgniteUrl(String path) {
        return resolveIgniteUrl(path, true);
    }

    /**
     * Resolve Spring configuration URL.
     *
     * @param springCfgPath Spring XML configuration file path or URL. This cannot be {@code null}.
     * @return URL.
     * @throws IgniteCheckedException If failed.
     */
    public static URL resolveSpringUrl(String springCfgPath) throws IgniteCheckedException {
        A.notNull(springCfgPath, "springCfgPath");

        URL url;

        try {
            url = new URL(springCfgPath);
        }
        catch (MalformedURLException e) {
            url = U.resolveIgniteUrl(springCfgPath);

            if (url == null)
                url = resolveInClasspath(springCfgPath);

            if (url == null)
                throw new IgniteCheckedException("Spring XML configuration path is invalid: " + springCfgPath +
                    ". Note that this path should be either absolute or a relative local file system path, " +
                    "relative to META-INF in classpath or valid URL to IGNITE_HOME.", e);
        }

        return url;
    }

    /**
     * @param path Resource path.
     * @return Resource URL inside classpath or {@code null}.
     */
    @Nullable private static URL resolveInClasspath(String path) {
        ClassLoader clsLdr = Thread.currentThread().getContextClassLoader();

        if (clsLdr == null)
            return null;

        return clsLdr.getResource(path.replaceAll("\\\\", "/"));
    }

    /**
     * Gets URL representing the path passed in. First the check is made if path is absolute.
     * If not, then the check is made if path is relative to {@code META-INF} folder in classpath.
     * If not, then the check is made if path is relative to ${IGNITE_HOME}.
     * If all checks fail,
     * then {@code null} is returned, otherwise URL representing path is returned.
     * <p>
     * See {@link #getIgniteHome()} for information on how {@code IGNITE_HOME} is retrieved.
     *
     * @param path Path to resolve.
     * @param metaInf Flag to indicate whether META-INF folder should be checked or class path root.
     * @return Resolved path as URL, or {@code null} if path cannot be resolved.
     * @see #getIgniteHome()
     */
    @Nullable public static URL resolveIgniteUrl(String path, boolean metaInf) {
        File f = resolveIgnitePath(path);

        if (f != null) {
            try {
                // Note: we use that method's chain instead of File.getURL() with due
                // Sun bug http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6179468
                return f.toURI().toURL();
            }
            catch (MalformedURLException e) {
                // No-op.
            }
        }

        ClassLoader clsLdr = Thread.currentThread().getContextClassLoader();

        if (clsLdr != null) {
            String locPath = (metaInf ? "META-INF/" : "") + path.replaceAll("\\\\", "/");

            return clsLdr.getResource(locPath);
        }
        else
            return null;
    }

    /**
     * Converts byte array to hex string.
     *
     * @param arr Array of bytes.
     * @return Hex string.
     */
    public static String byteArray2HexString(byte[] arr) {
        StringBuilder sb = new StringBuilder(arr.length << 1);

        for (byte b : arr)
            addByteAsHex(sb, b);

        return sb.toString().toUpperCase();
    }

    /**
     * @param sb String builder.
     * @param b Byte to add in hexadecimal format.
     */
    private static void addByteAsHex(StringBuilder sb, byte b) {
        sb.append(Integer.toHexString(MASK & b >>> 4)).append(Integer.toHexString(MASK & b));
    }

    /**
     * Checks for containment of the value in the array.
     * Both array cells and value may be {@code null}. Two {@code null}s are considered equal.
     *
     * @param arr Array of objects.
     * @param val Value to check for containment inside of array.
     * @param vals Additional values.
     * @return {@code true} if contains object, {@code false} otherwise.
     */
    public static boolean containsObjectArray(@Nullable Object[] arr, Object val, @Nullable Object... vals) {
        if (arr == null || arr.length == 0)
            return false;

        for (Object o : arr) {
            if (F.eq(o, val))
                return true;

            if (vals != null && vals.length > 0)
                for (Object v : vals)
                    if (F.eq(o, v))
                        return true;
        }

        return false;
    }

    /**
     * Checks for containment of the value in the array.
     *
     * @param arr Array of objects.
     * @param val Value to check for containment inside of array.
     * @return {@code true} if contains object, {@code false} otherwise.
     */
    public static boolean containsIntArray(int[] arr, int val) {
        assert arr != null;

        if (arr.length == 0)
            return false;

        for (int i : arr)
            if (i == val)
                return true;

        return false;
    }

    /**
     * Checks for containment of given string value in the specified array.
     * Array's cells and string value can be {@code null}. Tow {@code null}s are considered equal.
     *
     * @param arr Array of strings.
     * @param val Value to check for containment inside of array.
     * @param ignoreCase Ignoring case if {@code true}.
     * @return {@code true} if contains string, {@code false} otherwise.
     */
    public static boolean containsStringArray(String[] arr, @Nullable String val, boolean ignoreCase) {
        assert arr != null;

        for (String s : arr) {
            // If both are nulls, then they are equal.
            if (s == null && val == null)
                return true;

            // Only one is null and the other one isn't.
            if (s == null || val == null)
                continue;

            // Both are not nulls.
            if (ignoreCase) {
                if (s.equalsIgnoreCase(val))
                    return true;
            }
            else if (s.equals(val))
                return true;
        }

        return false;
    }

    /**
     * Checks for containment of given string value in the specified collection.
     * Collection elements and string value can be {@code null}. Tow {@code null}s are considered equal.
     *
     * @param c Array of strings.
     * @param val Value to check for containment inside of array.
     * @param ignoreCase Ignoring case if {@code true}.
     * @return {@code true} if contains string, {@code false} otherwise.
     */
    public static boolean containsStringCollection(Iterable<String> c, @Nullable String val, boolean ignoreCase) {
        assert c != null;

        for (String s : c) {
            // If both are nulls, then they are equal.
            if (s == null && val == null)
                return true;

            // Only one is null and the other one isn't.
            if (s == null || val == null)
                continue;

            // Both are not nulls.
            if (ignoreCase) {
                if (s.equalsIgnoreCase(val))
                    return true;
            }
            else if (s.equals(val))
                return true;
        }

        return false;
    }

    /**
     * Closes given resource logging possible checked exception.
     *
     * @param rsrc Resource to close. If it's {@code null} - it's no-op.
     * @param log Logger to log possible checked exception with (optional).
     */
    public static void close(@Nullable AutoCloseable rsrc, @Nullable IgniteLogger log) {
        if (rsrc != null) {
            try {
                rsrc.close();
            }
            catch (Exception e) {
                warn(log, "Failed to close resource: " + e.getMessage(), e);
            }
        }
    }

    /**
     * Closes given socket logging possible checked exception.
     *
     * @param sock Socket to close. If it's {@code null} - it's no-op.
     * @param log Logger to log possible checked exception with (optional).
     */
    public static void close(@Nullable Socket sock, @Nullable IgniteLogger log) {
        if (sock == null || sock.isClosed())
            return;

        try {
            // Closing output and input first to avoid tls 1.3 incompatibility
            // https://bugs.openjdk.java.net/browse/JDK-8208526
            if (!sock.isOutputShutdown())
                sock.shutdownOutput();
            if (!sock.isInputShutdown())
                sock.shutdownInput();
        }
        catch (ClosedChannelException | SocketException ex) {
            LT.warn(log, "Failed to shutdown socket", ex);
        }
        catch (Exception e) {
            warn(log, "Failed to shutdown socket: " + e.getMessage(), e);
        }

        try {
            sock.close();
        }
        catch (ClosedChannelException | SocketException ex) {
            LT.warn(log, "Failed to close socket", ex);
        }
        catch (Exception e) {
            warn(log, "Failed to close socket: " + e.getMessage(), e);
        }
    }

    /**
     * Closes given resource suppressing possible checked exception.
     *
     * @param rsrc Resource to close. If it's {@code null} - it's no-op.
     * @param e Suppressor exception
     */
    public static void closeWithSuppressingException(@Nullable AutoCloseable rsrc, @NotNull Exception e) {
        if (rsrc != null)
            try {
                rsrc.close();
            }
            catch (Exception suppressed) {
               e.addSuppressed(suppressed);
            }
    }

    /**
     * Quietly closes given resource ignoring possible checked exception.
     *
     * @param rsrc Resource to close. If it's {@code null} - it's no-op.
     */
    public static void closeQuiet(@Nullable AutoCloseable rsrc) {
        if (rsrc != null)
            try {
                rsrc.close();
            }
            catch (Exception ignored) {
                // No-op.
            }
    }

    /**
     * Closes given resource logging possible checked exceptions.
     *
     * @param rsrc Resource to close. If it's {@code null} - it's no-op.
     * @param log Logger to log possible checked exception with (optional).
     */
    public static void close(@Nullable SelectionKey rsrc, @Nullable IgniteLogger log) {
        if (rsrc != null)
            // This apply will automatically deregister the selection key as well.
            close(rsrc.channel(), log);
    }

    /**
     * Quietly closes given resource ignoring possible checked exceptions.
     *
     * @param rsrc Resource to close. If it's {@code null} - it's no-op.
     */
    public static void closeQuiet(@Nullable SelectionKey rsrc) {
        if (rsrc != null)
            // This apply will automatically deregister the selection key as well.
            closeQuiet(rsrc.channel());
    }

    /**
     * Closes given resource.
     *
     * @param rsrc Resource to close. If it's {@code null} - it's no-op.
     */
    public static void close(@Nullable DatagramSocket rsrc) {
        if (rsrc != null)
            rsrc.close();
    }

    /**
     * Closes given resource logging possible checked exception.
     *
     * @param rsrc Resource to close. If it's {@code null} - it's no-op.
     * @param log Logger to log possible checked exception with (optional).
     */
    public static void close(@Nullable Selector rsrc, @Nullable IgniteLogger log) {
        if (rsrc != null)
            try {
                if (rsrc.isOpen())
                    rsrc.close();
            }
            catch (IOException e) {
                warn(log, "Failed to close resource: " + e.getMessage());
            }
    }

    /**
     * Quietly closes given resource ignoring possible checked exception.
     *
     * @param rsrc Resource to close. If it's {@code null} - it's no-op.
     */
    public static void closeQuiet(@Nullable Selector rsrc) {
        if (rsrc != null)
            try {
                if (rsrc.isOpen())
                    rsrc.close();
            }
            catch (IOException ignored) {
                // No-op.
            }
    }

    /**
     * Closes given resource logging possible checked exception.
     *
     * @param rsrc Resource to close. If it's {@code null} - it's no-op.
     * @param log Logger to log possible checked exception with (optional).
     */
    public static void close(@Nullable Context rsrc, @Nullable IgniteLogger log) {
        if (rsrc != null)
            try {
                rsrc.close();
            }
            catch (NamingException e) {
                warn(log, "Failed to close resource: " + e.getMessage());
            }
    }

    /**
     * Quietly closes given resource ignoring possible checked exception.
     *
     * @param rsrc Resource to close. If it's {@code null} - it's no-op.
     */
    public static void closeQuiet(@Nullable Context rsrc) {
        if (rsrc != null)
            try {
                rsrc.close();
            }
            catch (NamingException ignored) {
                // No-op.
            }
    }

    /**
     * Closes class loader logging possible checked exception.
     *
     * @param clsLdr Class loader. If it's {@code null} - it's no-op.
     * @param log Logger to log possible checked exception with (optional).
     */
    public static void close(@Nullable URLClassLoader clsLdr, @Nullable IgniteLogger log) {
        if (clsLdr != null) {
            try {
                clsLdr.close();
            }
            catch (Exception e) {
                warn(log, "Failed to close resource: " + e.getMessage());
            }
        }
    }

    /**
     * Quietly closes given {@link Socket} ignoring possible checked exception.
     *
     * @param sock Socket to close. If it's {@code null} - it's no-op.
     */
    public static void closeQuiet(@Nullable Socket sock) {
        if (sock == null)
            return;

        try {
            // Avoid java 12 bug see https://bugs.openjdk.java.net/browse/JDK-8219658
            sock.shutdownOutput();
            sock.shutdownInput();
        }
        catch (Exception ignored) {
            // No-op.
        }

        try {
            sock.close();
        }
        catch (Exception ignored) {
            // No-op.
        }
    }

    /**
     * Quietly releases file lock ignoring all possible exceptions.
     *
     * @param lock File lock. If it's {@code null} - it's no-op.
     */
    public static void releaseQuiet(@Nullable FileLock lock) {
        if (lock != null)
            try {
                lock.release();
            }
            catch (Exception ignored) {
                // No-op.
            }
    }

    /**
     * Rollbacks JDBC connection logging possible checked exception.
     *
     * @param rsrc JDBC connection to rollback. If connection is {@code null}, it's no-op.
     * @param log Logger to log possible checked exception with (optional).
     */
    public static void rollbackConnection(@Nullable Connection rsrc, @Nullable IgniteLogger log) {
        if (rsrc != null)
            try {
                rsrc.rollback();
            }
            catch (SQLException e) {
                warn(log, "Failed to rollback JDBC connection: " + e.getMessage());
            }
    }

    /**
     * Depending on whether or not log is provided and quiet mode is enabled logs given messages as
     * quiet message or normal log WARN message in {@code org.apache.ignite.CourtesyConfigNotice}
     * category. If {@code log} is {@code null} or in QUIET mode it will add {@code (courtesy)}
     * prefix to the message.
     *
     * @param log Optional logger to use when QUIET mode is not enabled.
     * @param msg Message to log.
     */
    public static void courtesy(@Nullable IgniteLogger log, Object msg) {
        assert msg != null;

        String s = msg.toString();

        courtesy(log, s, s);
    }

    /**
     * Depending on whether or not log is provided and quiet mode is enabled logs given messages as
     * quiet message or normal log WARN message in {@code org.apache.ignite.CourtesyConfigNotice}
     * category. If {@code log} is {@code null} or in QUIET mode it will add {@code (courtesy)}
     * prefix to the message.
     *
     * @param log Optional logger to use when QUIET mode is not enabled.
     * @param longMsg Message to log using normal logger.
     * @param shortMsg Message to log using quiet logger.
     */
    public static void courtesy(@Nullable IgniteLogger log, Object longMsg, Object shortMsg) {
        assert longMsg != null;
        assert shortMsg != null;

        if (log != null)
            log.getLogger(IgniteConfiguration.COURTESY_LOGGER_NAME).warning(compact(longMsg.toString()));
        else
            X.println("[" + SHORT_DATE_FMT.format(Instant.now()) + "] (courtesy) " +
                compact(shortMsg.toString()));
    }

    /**
     * Depending on whether or not log is provided and quiet mode is enabled logs given
     * messages as quiet message or normal log WARN message. If {@code log} is {@code null}
     * or in QUIET mode it will add {@code (wrn)} prefix to the message.
     *
     * @param log Optional logger to use when QUIET mode is not enabled.
     * @param msg Message to log.
     */
    public static void warn(@Nullable IgniteLogger log, Object msg) {
        assert msg != null;

        String s = msg.toString();

        warn(log, s, null);
    }

    /**
     * Logs warning message in both verbose and quiet modes.
     *
     * @param log Logger to use.
     * @param msg Message to log.
     */
    public static void quietAndWarn(IgniteLogger log, Object msg) {
        quietAndWarn(log, msg, msg);
    }

    /**
     * Logs warning message in both verbose and quiet modes.
     *
     * @param log Logger to use.
     * @param shortMsg Short message.
     * @param msg Message to log.
     */
    public static void quietAndWarn(IgniteLogger log, Object msg, Object shortMsg) {
        warn(log, msg);

        if (log.isQuiet())
            quiet(false, shortMsg);
    }

    /**
     * Logs warning message in both verbose and quiet modes.
     *
     * @param log Logger to use.
     * @param msg Message to log.
     * @param e Optional exception.
     */
    public static void quietAndWarn(IgniteLogger log, Object msg, @Nullable Throwable e) {
        warn(log, msg, e);

        if (log.isQuiet())
            quiet(false, msg);
    }

    /**
     * Depending on whether or not log is provided and quiet mode is enabled logs given
     * messages as quiet message or normal log ERROR message. If {@code log} is {@code null}
     * or in QUIET mode it will add {@code (err)} prefix to the message.
     *
     * @param log Optional logger to use when QUIET mode is not enabled.
     * @param msg Message to log.
     */
    public static void error(@Nullable IgniteLogger log, Object msg) {
        assert msg != null;

        if (msg instanceof Throwable) {
            Throwable t = (Throwable)msg;

            error(log, t.getMessage(), t);
        }
        else {
            String s = msg.toString();

            error(log, s, s, null);
        }
    }

    /**
     * Depending on whether or not log is provided and quiet mode is enabled logs given
     * messages as quiet message or normal log WARN message. If {@code log} is {@code null}
     * or in QUIET mode it will add {@code (wrn)} prefix to the message.
     *
     * @param log Optional logger to use when QUIET mode is not enabled.
     * @param msg Message to log using normal logger.
     * @param e Optional exception.
     */
    public static void warn(@Nullable IgniteLogger log, Object msg, @Nullable Throwable e) {
        assert msg != null;

        if (log != null)
            log.warning(compact(msg.toString()), e);
        else {
            X.println("[" + SHORT_DATE_FMT.format(Instant.now()) + "] (wrn) " +
                    compact(msg.toString()));

            if (e != null)
                e.printStackTrace(System.err);
            else
                X.printerrln();
        }
    }

    /**
     * Depending on whether or not log is provided and quiet mode is enabled logs given
     * messages as quiet message or normal log WARN message with {@link IgniteLogger#DEV_ONLY DEV_ONLY} marker.
     * If {@code log} is {@code null} or in QUIET mode it will add {@code (wrn)} prefix to the message.
     * If property {@link IgniteSystemProperties#IGNITE_DEV_ONLY_LOGGING_DISABLED IGNITE_DEV_ONLY_LOGGING_DISABLED}
     * is set to true, the message will not be logged.
     *
     * @param log Optional logger to use when QUIET mode is not enabled.
     * @param msg Message to log.
     */
    public static void warnDevOnly(@Nullable IgniteLogger log, Object msg) {
        assert msg != null;

        // don't log message if DEV_ONLY messages are disabled
        if (devOnlyLogDisabled)
            return;

        if (log != null)
            log.warning(IgniteLogger.DEV_ONLY, compact(msg.toString()), null);
        else
            X.println("[" + SHORT_DATE_FMT.format(Instant.now()) + "] (wrn) " +
                compact(msg.toString()));
    }

    /**
     * Depending on whether or not log is provided and quiet mode is enabled logs given
     * messages as quiet message or normal log INFO message.
     * <p>
     * <b>NOTE:</b> unlike the normal logging when INFO level may not be enabled and
     * therefore no logging will happen - using this method the log will be written
     * always either via INFO log or quiet mode.
     * <p>
     * <b>USE IT APPROPRIATELY.</b>
     *
     * @param log Optional logger to use when QUIET mode is not enabled.
     * @param longMsg Message to log using normal logger.
     * @param shortMsg Message to log using quiet logger.
     */
    public static void log(@Nullable IgniteLogger log, Object longMsg, Object shortMsg) {
        assert longMsg != null;
        assert shortMsg != null;

        if (log != null) {
            if (log.isInfoEnabled())
                log.info(compact(longMsg.toString()));
        }
        else
            quiet(false, shortMsg);
    }

    /**
     * Resolves work directory.
     * @param cfg Ignite configuration.
     */
    public static void initWorkDir(IgniteConfiguration cfg) throws IgniteCheckedException {
        String igniteHome = cfg.getIgniteHome();

        // Set Ignite home.
        if (igniteHome == null)
            igniteHome = U.getIgniteHome();

        String userProvidedWorkDir = cfg.getWorkDirectory();

        // Correctly resolve work directory and set it back to configuration.
        cfg.setWorkDirectory(U.workDirectory(userProvidedWorkDir, igniteHome));
    }

    /**
     * @param cfg Ignite configuration.
     * @param app Application name.
     * @return Initialized logger.
     * @throws IgniteCheckedException If failed.
     */
    public static IgniteLogger initLogger(IgniteConfiguration cfg, String app) throws IgniteCheckedException {
        return initLogger(
            cfg.getGridLogger(),
            app,
            cfg.getNodeId() != null ? cfg.getNodeId() : UUID.randomUUID(),
            cfg.getWorkDirectory()
        );
    }

    /**
     * @param cfgLog Configured logger.
     * @param app Application name.
     * @param workDir Work directory.
     * @return Initialized logger.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("ErrorNotRethrown")
    public static IgniteLogger initLogger(
        @Nullable IgniteLogger cfgLog,
        @Nullable String app,
        UUID nodeId,
        String workDir
    ) throws IgniteCheckedException {
        try {
            Exception log4jInitErr = null;

            if (cfgLog == null) {
                Class<?> log4jCls;

                try {
                    log4jCls = Class.forName("org.apache.ignite.logger.log4j.Log4JLogger");
                }
                catch (ClassNotFoundException | NoClassDefFoundError ignored) {
                    log4jCls = null;
                }

                if (log4jCls != null) {
                    try {
                        URL url = U.resolveIgniteUrl("config/ignite-log4j.xml");

                        if (url == null) {
                            File cfgFile = new File("config/ignite-log4j.xml");

                            if (!cfgFile.exists())
                                cfgFile = new File("../config/ignite-log4j.xml");

                            if (cfgFile.exists()) {
                                try {
                                    url = cfgFile.toURI().toURL();
                                }
                                catch (MalformedURLException ignore) {
                                    // No-op.
                                }
                            }
                        }

                        if (url != null) {
                            boolean configured = (Boolean)log4jCls.getMethod("isConfigured").invoke(null);

                            if (configured)
                                url = null;
                        }

                        if (url != null) {
                            Constructor<?> ctor = log4jCls.getConstructor(URL.class);

                            cfgLog = (IgniteLogger)ctor.newInstance(url);
                        }
                        else
                            cfgLog = (IgniteLogger)log4jCls.newInstance();
                    }
                    catch (Exception e) {
                        log4jInitErr = e;
                    }
                }

                if (log4jCls == null || log4jInitErr != null)
                    cfgLog = new JavaLogger();
            }

            // Special handling for Java logger which requires work directory.
            if (cfgLog instanceof JavaLogger)
                ((JavaLogger)cfgLog).setWorkDirectory(workDir);

            // Set node IDs for all file appenders.
            if (cfgLog instanceof LoggerNodeIdAndApplicationAware)
                ((LoggerNodeIdAndApplicationAware)cfgLog).setApplicationAndNode(app, nodeId);
            else if (cfgLog instanceof LoggerNodeIdAware)
                ((LoggerNodeIdAware)cfgLog).setNodeId(nodeId);

            if (log4jInitErr != null)
                U.warn(cfgLog, "Failed to initialize Log4JLogger (falling back to standard java logging): "
                    + log4jInitErr.getCause());

            return cfgLog;
        }
        catch (Exception e) {
            throw new IgniteCheckedException("Failed to create logger.", e);
        }
    }

    /**
     * Depending on whether or not log is provided and quiet mode is enabled logs given
     * messages as quiet message or normal log INF0 message.
     * <p>
     * <b>NOTE:</b> unlike the normal logging when INFO level may not be enabled and
     * therefore no logging will happen - using this method the log will be written
     * always either via INFO log or quiet mode.
     * <p>
     * <b>USE IT APPROPRIATELY.</b>
     *
     * @param log Optional logger to use when QUIET mode is not enabled.
     * @param msg Message to log.
     */
    public static void log(@Nullable IgniteLogger log, Object msg) {
        assert msg != null;

        String s = msg.toString();

        log(log, s, s);
    }

    /**
     * Depending on whether or not log is provided and quiet mode is enabled logs given
     * messages as quiet message or normal log ERROR message. If {@code log} is {@code null}
     * or in QUIET mode it will add {@code (err)} prefix to the message.
     *
     * @param log Optional logger to use when QUIET mode is not enabled.
     * @param longMsg Message to log using normal logger.
     * @param shortMsg Message to log using quiet logger.
     * @param e Optional exception.
     */
    public static void error(@Nullable IgniteLogger log, Object longMsg, Object shortMsg, @Nullable Throwable e) {
        assert longMsg != null;
        assert shortMsg != null;

        if (log != null) {
            if (e == null)
                log.error(compact(longMsg.toString()));
            else
                log.error(compact(longMsg.toString()), e);
        }
        else {
            X.printerr("[" + SHORT_DATE_FMT.format(Instant.now()) + "] (err) " +
                compact(shortMsg.toString()));

            if (e != null)
                e.printStackTrace(System.err);
            else
                X.printerrln();
        }
    }

    /**
     * Shortcut for {@link #error(org.apache.ignite.IgniteLogger, Object, Object, Throwable)}.
     *
     * @param log Optional logger.
     * @param shortMsg Message to log using quiet logger.
     * @param e Optional exception.
     */
    public static void error(@Nullable IgniteLogger log, Object shortMsg, @Nullable Throwable e) {
        assert shortMsg != null;

        String s = shortMsg.toString();

        error(log, s, s, e);
    }

    /**
     *
     * @param err Whether to print to {@code System.err}.
     * @param objs Objects to log in quiet mode.
     */
    public static void quiet(boolean err, Object... objs) {
        assert objs != null;

        String time = SHORT_DATE_FMT.format(Instant.now());

        SB sb = new SB();

        for (Object obj : objs)
            sb.a('[').a(time).a("] ").a(obj.toString()).a(NL);

        PrintStream ps = err ? System.err : System.out;

        ps.print(compact(sb.toString()));
    }

    /**
     *
     * @param err Whether to print to {@code System.err}.
     * @param multiline Multiple lines string to print.
     */
    public static void quietMultipleLines(boolean err, String multiline) {
        assert multiline != null;

        quiet(err, multiline.split(NL));
    }

    /**
     * Prints out the message in quiet and info modes.
     *
     * @param log Logger.
     * @param msg Message to print.
     */
    public static void quietAndInfo(IgniteLogger log, String msg) {
        if (log.isQuiet())
            U.quiet(false, msg);

        if (log.isInfoEnabled())
            log.info(msg);
    }

    /**
     * Quietly rollbacks JDBC connection ignoring possible checked exception.
     *
     * @param rsrc JDBC connection to rollback. If connection is {@code null}, it's no-op.
     */
    public static void rollbackConnectionQuiet(@Nullable Connection rsrc) {
        if (rsrc != null)
            try {
                rsrc.rollback();
            }
            catch (SQLException ignored) {
                // No-op.
            }
    }

    /**
     * Constructs JMX object name with given properties.
     * Map with ordered {@code groups} used for proper object name construction.
     *
     * @param igniteInstanceName Ignite instance name.
     * @param grp Name of the group.
     * @param name Name of mbean.
     * @return JMX object name.
     * @throws MalformedObjectNameException Thrown in case of any errors.
     */
    public static ObjectName makeMBeanName(@Nullable String igniteInstanceName, @Nullable String grp, String name)
        throws MalformedObjectNameException {
        SB sb = new SB(JMX_DOMAIN + ':');

        appendClassLoaderHash(sb);

        appendJvmId(sb);

        if (igniteInstanceName != null && !igniteInstanceName.isEmpty())
            sb.a("igniteInstanceName=").a(igniteInstanceName).a(',');

        if (grp != null)
            sb.a("group=").a(escapeObjectNameValue(grp)).a(',');

        sb.a("name=").a(escapeObjectNameValue(name));

        return new ObjectName(sb.toString());
    }

    /**
     * @param sb Sb.
     */
    private static void appendClassLoaderHash(SB sb) {
        if (getBoolean(IGNITE_MBEAN_APPEND_CLASS_LOADER_ID, DFLT_MBEAN_APPEND_CLASS_LOADER_ID)) {
            String clsLdrHash = Integer.toHexString(Ignite.class.getClassLoader().hashCode());

            sb.a("clsLdr=").a(clsLdrHash).a(',');
        }
    }

    /**
     * @param sb Sb.
     */
    private static void appendJvmId(SB sb) {
        if (getBoolean(IGNITE_MBEAN_APPEND_JVM_ID)) {
            String jvmId = ManagementFactory.getRuntimeMXBean().getName();

            sb.a("jvmId=").a(jvmId).a(',');
        }
    }

    /**
     * Mask component name to make sure that it is not {@code null}.
     *
     * @param name Component name to mask, possibly {@code null}.
     * @return Component name.
     */
    public static String maskName(@Nullable String name) {
        return name == null ? "default" : name;
    }

    /**
     * Escapes the given string to be used as a value in the ObjectName syntax.
     *
     * @param s A string to be escape.
     * @return An escaped string.
     */
    private static String escapeObjectNameValue(String s) {
        if (alphanumericUnderscore(s))
            return s;

        return '\"' + s.replaceAll("[\\\\\"?*]", "\\\\$0") + '\"';
    }

    /**
     * @param s String to check.
     * @return {@code true} if given string contains only alphanumeric and underscore symbols.
     */
    public static boolean alphanumericUnderscore(String s) {
        return ALPHANUMERIC_UNDERSCORE_PATTERN.matcher(s).matches();
    }

    /**
     * Registers MBean with the server.
     *
     * @param <T> Type of mbean.
     * @param mbeanSrv MBean server.
     * @param igniteInstanceName Ignite instance name.
     * @param grp Name of the group.
     * @param name Name of mbean.
     * @param impl MBean implementation.
     * @param itf MBean interface.
     * @return JMX object name.
     * @throws MBeanRegistrationException if MBeans are disabled.
     * @throws JMException If MBean creation failed.
     */
    public static <T> ObjectName registerMBean(
        MBeanServer mbeanSrv,
        @Nullable String igniteInstanceName,
        @Nullable String grp,
        String name, T impl,
        @Nullable Class<T> itf
    ) throws JMException {
        return registerMBean(mbeanSrv, makeMBeanName(igniteInstanceName, grp, name), impl, itf);
    }

    /**
     * Registers MBean with the server.
     *
     * @param <T> Type of mbean.
     * @param mbeanSrv MBean server.
     * @param name MBean object name.
     * @param impl MBean implementation.
     * @param itf MBean interface.
     * @return JMX object name.
     * @throws MBeanRegistrationException if MBeans are disabled.
     * @throws JMException If MBean creation failed.
     * @throws IgniteException If MBean creation are not allowed.
     */
    public static <T> ObjectName registerMBean(MBeanServer mbeanSrv, ObjectName name, T impl, Class<T> itf)
        throws JMException {
        if (IGNITE_MBEANS_DISABLED)
            throw new MBeanRegistrationException(new IgniteIllegalStateException("MBeans are disabled."));

        assert mbeanSrv != null;
        assert name != null;
        assert itf != null;

        DynamicMBean mbean;

        if (impl instanceof DynamicMBean) {
            mbean = (DynamicMBean)impl;
        }
        else {
            mbean = new IgniteStandardMXBean(impl, itf);

            mbean.getMBeanInfo();
        }

        return mbeanSrv.registerMBean(mbean, name).getObjectName();
    }

    /**
     * Convenience method that interrupts a given thread if it's not {@code null}.
     *
     * @param t Thread to interrupt.
     */
    public static void interrupt(@Nullable Thread t) {
        if (t != null)
            t.interrupt();
    }

    /**
     * Convenience method that interrupts a given thread if it's not {@code null}.
     *
     * @param workers Threads to interrupt.
     */
    public static void interrupt(Iterable<? extends Thread> workers) {
        if (workers != null)
            for (Thread worker : workers)
                worker.interrupt();
    }

    /**
     * Waits for completion of a given thread. If thread is {@code null} then
     * this method returns immediately returning {@code true}
     *
     * @param t Thread to join.
     * @param log Logger for logging errors.
     * @return {@code true} if thread has finished, {@code false} otherwise.
     */
    public static boolean join(@Nullable Thread t, @Nullable IgniteLogger log) {
        return join(t, log, 0);
    }

    /**
     * Waits for completion of a given thread. If thread is {@code null} then
     * this method returns immediately returning {@code true}
     *
     * @param t Thread to join.
     * @param log Logger for logging errors.
     * @param timeout Join timeout.
     * @return {@code true} if thread has finished, {@code false} otherwise.
     */
    public static boolean join(@Nullable Thread t, @Nullable IgniteLogger log, long timeout) {
        if (t != null) {
            try {
                t.join(timeout);

                return !t.isAlive();
            }
            catch (InterruptedException ignore) {
                warn(log, "Got interrupted while waiting for completion of a thread: " + t);

                Thread.currentThread().interrupt();

                return false;
            }
        }

        return true;
    }

    /**
     * Waits for completion of a given threads. If thread is {@code null} then
     * this method returns immediately returning {@code true}
     *
     * @param workers Thread to join.
     * @param log Logger for logging errors.
     * @return {@code true} if thread has finished, {@code false} otherwise.
     */
    public static boolean joinThreads(Iterable<? extends Thread> workers, @Nullable IgniteLogger log) {
        boolean retval = true;

        if (workers != null)
            for (Thread worker : workers)
                if (!join(worker, log))
                    retval = false;

        return retval;
    }

    /**
     * Starts given threads.
     *
     * @param threads Threads to start.
     */
    public static void startThreads(Iterable<? extends Thread> threads) {
        if (threads != null) {
            for (Thread thread : threads) {
                if (thread != null)
                    thread.start();
            }
        }
    }

    /**
     * Cancels given runnable.
     *
     * @param w Worker to cancel - it's no-op if runnable is {@code null}.
     */
    public static void cancel(@Nullable GridWorker w) {
        if (w != null)
            w.cancel();
    }

    /**
     * Cancels collection of runnables.
     *
     * @param ws Collection of workers - it's no-op if collection is {@code null}.
     */
    public static void cancel(Iterable<? extends GridWorker> ws) {
        if (ws != null)
            for (GridWorker w : ws)
                w.cancel();
    }

    /**
     * Joins runnable.
     *
     * @param w Worker to join.
     * @param log The logger to possible exception.
     * @return {@code true} if worker has not been interrupted, {@code false} if it was interrupted.
     */
    public static boolean join(@Nullable GridWorker w, @Nullable IgniteLogger log) {
        if (w != null)
            try {
                w.join();
            }
            catch (InterruptedException ignore) {
                warn(log, "Got interrupted while waiting for completion of runnable: " + w);

                Thread.currentThread().interrupt();

                return false;
            }

        return true;
    }

    /**
     * Joins given collection of runnables.
     *
     * @param ws Collection of workers to join.
     * @param log The logger to possible exceptions.
     * @return {@code true} if none of the worker have been interrupted,
     *      {@code false} if at least one was interrupted.
     */
    public static boolean join(Iterable<? extends GridWorker> ws, IgniteLogger log) {
        boolean retval = true;

        if (ws != null)
            for (GridWorker w : ws)
                if (!join(w, log))
                    retval = false;

        return retval;
    }

    /**
     * Shutdowns given {@code ExecutorService} and wait for executor service to stop.
     *
     * @param owner The ExecutorService owner.
     * @param exec ExecutorService to shutdown.
     * @param log The logger to possible exceptions and warnings.
     */
    public static void shutdownNow(Class<?> owner, @Nullable ExecutorService exec, @Nullable IgniteLogger log) {
        if (exec != null) {
            List<Runnable> tasks = exec.shutdownNow();

            if (!F.isEmpty(tasks))
                U.warn(log, "Runnable tasks outlived thread pool executor service [owner=" + getSimpleName(owner) +
                    ", tasks=" + tasks + ']');

            try {
                exec.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
            }
            catch (InterruptedException ignored) {
                warn(log, "Got interrupted while waiting for executor service to stop.");

                exec.shutdownNow();

                // Preserve interrupt status.
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Creates appropriate empty projection exception.
     *
     * @return Empty projection exception.
     */
    public static ClusterGroupEmptyCheckedException emptyTopologyException() {
        return new ClusterGroupEmptyCheckedException("Cluster group is empty.");
    }

    /**
     * Writes UUIDs to output stream. This method is meant to be used by
     * implementations of {@link Externalizable} interface.
     *
     * @param out Output stream.
     * @param col UUIDs to write.
     * @throws IOException If write failed.
     */
    public static void writeUuids(DataOutput out, @Nullable Collection<UUID> col) throws IOException {
        if (col != null) {
            out.writeInt(col.size());

            for (UUID id : col)
                writeUuid(out, id);
        }
        else
            out.writeInt(-1);
    }

    /**
     * Reads UUIDs from input stream. This method is meant to be used by
     * implementations of {@link Externalizable} interface.
     *
     * @param in Input stream.
     * @return Read UUIDs.
     * @throws IOException If read failed.
     */
    @Nullable public static List<UUID> readUuids(DataInput in) throws IOException {
        int size = in.readInt();

        // Check null flag.
        if (size == -1)
            return null;

        List<UUID> col = new ArrayList<>(size);

        for (int i = 0; i < size; i++)
            col.add(readUuid(in));

        return col;
    }

    /**
     * Writes Ignite UUIDs to output stream. This method is meant to be used by
     * implementations of {@link Externalizable} interface.
     *
     * @param out Output stream.
     * @param col Ignite UUIDs to write.
     * @throws IOException If write failed.
     */
    public static void writeGridUuids(DataOutput out, @Nullable Collection<IgniteUuid> col) throws IOException {
        if (col != null) {
            out.writeBoolean(true);

            out.writeInt(col.size());

            for (IgniteUuid id : col)
                writeGridUuid(out, id);
        }
        else
            out.writeBoolean(false);
    }

    /**
     * Reads Ignite UUIDs from input stream. This method is meant to be used by
     * implementations of {@link Externalizable} interface.
     *
     * @param in Input stream.
     * @return Read Ignite UUIDs.
     * @throws IOException If read failed.
     */
    @Nullable public static List<IgniteUuid> readGridUuids(DataInput in) throws IOException {
        List<IgniteUuid> col = null;

        // Check null flag.
        if (in.readBoolean()) {
            int size = in.readInt();

            col = new ArrayList<>(size);

            for (int i = 0; i < size; i++)
                col.add(readGridUuid(in));
        }

        return col;
    }

    /**
     * Writes UUID to output stream. This method is meant to be used by
     * implementations of {@link Externalizable} interface.
     *
     * @param out Output stream.
     * @param uid UUID to write.
     * @throws IOException If write failed.
     */
    public static void writeUuid(DataOutput out, UUID uid) throws IOException {
        // Write null flag.
        out.writeBoolean(uid == null);

        if (uid != null) {
            out.writeLong(uid.getMostSignificantBits());
            out.writeLong(uid.getLeastSignificantBits());
        }
    }

    /**
     * Reads UUID from input stream. This method is meant to be used by
     * implementations of {@link Externalizable} interface.
     *
     * @param in Input stream.
     * @return Read UUID.
     * @throws IOException If read failed.
     */
    @Nullable public static UUID readUuid(DataInput in) throws IOException {
        // If UUID is not null.
        if (!in.readBoolean()) {
            long most = in.readLong();
            long least = in.readLong();

            return IgniteUuidCache.onIgniteUuidRead(new UUID(most, least));
        }

        return null;
    }

    /**
     * Writes UUID to binary writer.
     *
     * @param out Output Binary writer.
     * @param uid UUID to write.
     * @throws IOException If write failed.
     */
    public static void writeUuid(BinaryRawWriter out, UUID uid) {
        // Write null flag.
        if (uid != null) {
            out.writeBoolean(true);

            out.writeLong(uid.getMostSignificantBits());
            out.writeLong(uid.getLeastSignificantBits());
        }
        else
            out.writeBoolean(false);
    }

    /**
     * Reads UUID from binary reader.
     *
     * @param in Binary reader.
     * @return Read UUID.
     * @throws IOException If read failed.
     */
    @Nullable public static UUID readUuid(BinaryRawReader in) {
        // If UUID is not null.
        if (in.readBoolean()) {
            long most = in.readLong();
            long least = in.readLong();

            return new UUID(most, least);
        }
        else
            return null;
    }

    /**
     * Writes {@link org.apache.ignite.lang.IgniteUuid} to output stream. This method is meant to be used by
     * implementations of {@link Externalizable} interface.
     *
     * @param out Output stream.
     * @param uid UUID to write.
     * @throws IOException If write failed.
     */
    public static void writeGridUuid(DataOutput out, IgniteUuid uid) throws IOException {
        // Write null flag.
        out.writeBoolean(uid == null);

        if (uid != null) {
            out.writeLong(uid.globalId().getMostSignificantBits());
            out.writeLong(uid.globalId().getLeastSignificantBits());

            out.writeLong(uid.localId());
        }
    }

    /**
     * Reads {@link org.apache.ignite.lang.IgniteUuid} from input stream. This method is meant to be used by
     * implementations of {@link Externalizable} interface.
     *
     * @param in Input stream.
     * @return Read UUID.
     * @throws IOException If read failed.
     */
    @Nullable public static IgniteUuid readGridUuid(DataInput in) throws IOException {
        // If UUID is not null.
        if (!in.readBoolean()) {
            long most = in.readLong();
            long least = in.readLong();

            UUID globalId = IgniteUuidCache.onIgniteUuidRead(new UUID(most, least));

            long locId = in.readLong();

            return new IgniteUuid(globalId, locId);
        }

        return null;
    }

    /**
     * Converts {@link IgniteUuid} to bytes.
     *
     * @param uuid {@link IgniteUuid} to convert.
     * @return Bytes.
     */
    public static byte[] igniteUuidToBytes(IgniteUuid uuid) {
        assert uuid != null;

        byte[] out = new byte[24];

        igniteUuidToBytes(uuid, out, 0);

        return out;
    }

    /**
     * Converts {@link IgniteUuid} to bytes.
     *
     * @param uuid {@link IgniteUuid} to convert.
     * @param out Output array to write to.
     * @param off Offset from which to write.
     */
    public static void igniteUuidToBytes(IgniteUuid uuid, byte[] out, int off) {
        assert uuid != null;

        longToBytes(uuid.globalId().getMostSignificantBits(), out, off);
        longToBytes(uuid.globalId().getLeastSignificantBits(), out, off + 8);
        longToBytes(uuid.localId(), out, off + 16);
    }

    /**
     * Converts bytes to {@link IgniteUuid}.
     *
     * @param in Input byte array.
     * @param off Offset from which start reading.
     * @return {@link IgniteUuid} instance.
     */
    public static IgniteUuid bytesToIgniteUuid(byte[] in, int off) {
        long most = bytesToLong(in, off);
        long least = bytesToLong(in, off + 8);
        long locId = bytesToLong(in, off + 16);

        return new IgniteUuid(IgniteUuidCache.onIgniteUuidRead(new UUID(most, least)), locId);
    }

    /**
     * Writes boolean array to output stream accounting for <tt>null</tt> values.
     *
     * @param out Output stream to write to.
     * @param arr Array to write, possibly <tt>null</tt>.
     * @throws IOException If write failed.
     */
    public static void writeBooleanArray(DataOutput out, @Nullable boolean[] arr) throws IOException {
        if (arr == null)
            out.writeInt(-1);
        else {
            out.writeInt(arr.length);

            for (boolean b : arr)
                out.writeBoolean(b);
        }
    }

    /**
     * Writes int array to output stream accounting for <tt>null</tt> values.
     *
     * @param out Output stream to write to.
     * @param arr Array to write, possibly <tt>null</tt>.
     * @throws IOException If write failed.
     */
    public static void writeIntArray(DataOutput out, @Nullable int[] arr) throws IOException {
        if (arr == null)
            out.writeInt(-1);
        else {
            out.writeInt(arr.length);

            for (int b : arr)
                out.writeInt(b);
        }
    }

    /**
     * Writes long array to output stream.
     *
     * @param out Output stream to write to.
     * @param arr Array to write.
     * @throws IOException If write failed.
     */
    public static void writeLongArray(DataOutput out, @Nullable long[] arr) throws IOException {
        if (arr == null)
            out.writeInt(-1);
        else {
            out.writeInt(arr.length);

            for (long b : arr)
                out.writeLong(b);
        }
    }

    /**
     * Reads boolean array from input stream accounting for <tt>null</tt> values.
     *
     * @param in Stream to read from.
     * @return Read byte array, possibly <tt>null</tt>.
     * @throws IOException If read failed.
     */
    @Nullable public static boolean[] readBooleanArray(DataInput in) throws IOException {
        int len = in.readInt();

        if (len == -1)
            return null; // Value "-1" indicates null.

        boolean[] res = new boolean[len];

        for (int i = 0; i < len; i++)
            res[i] = in.readBoolean();

        return res;
    }

    /**
     * Reads int array from input stream accounting for <tt>null</tt> values.
     *
     * @param in Stream to read from.
     * @return Read byte array, possibly <tt>null</tt>.
     * @throws IOException If read failed.
     */
    @Nullable public static int[] readIntArray(DataInput in) throws IOException {
        int len = in.readInt();

        if (len == -1)
            return null; // Value "-1" indicates null.

        int[] res = new int[len];

        for (int i = 0; i < len; i++)
            res[i] = in.readInt();

        return res;
    }

    /**
     * Reads long array from input stream.
     *
     * @param in Stream to read from.
     * @return Read long array, possibly <tt>null</tt>.
     * @throws IOException If read failed.
     */
    @Nullable public static long[] readLongArray(DataInput in) throws IOException {
        int len = in.readInt();

        if (len == -1)
            return null; // Value "-1" indicates null.

        long[] res = new long[len];

        for (int i = 0; i < len; i++)
            res[i] = in.readLong();

        return res;
    }

    /**
     * Calculates hash code for the given byte buffers contents. Compatible with {@link Arrays#hashCode(byte[])}
     * with the same content. Does not change buffers positions.
     *
     * @param bufs Byte buffers.
     * @return Hash code.
     */
    public static int hashCode(ByteBuffer... bufs) {
        int res = 1;

        for (ByteBuffer buf : bufs) {
            int pos = buf.position();

            while (buf.hasRemaining())
                res = 31 * res + buf.get();

            buf.position(pos);
        }

        return res;
    }

    /**
     * @param out Output.
     * @param map Map to write.
     * @throws IOException If write failed.
     */
    public static void writeMap(ObjectOutput out, Map<?, ?> map) throws IOException {
        if (map != null) {
            out.writeInt(map.size());

            for (Map.Entry<?, ?> e : map.entrySet()) {
                out.writeObject(e.getKey());
                out.writeObject(e.getValue());
            }
        }
        else
            out.writeInt(-1);
    }

    /**
     *
     * @param in Input.
     * @return Read map.
     * @throws IOException If de-serialization failed.
     * @throws ClassNotFoundException If deserialized class could not be found.
     */
    @Nullable public static <K, V> Map<K, V> readMap(ObjectInput in) throws IOException, ClassNotFoundException {
        int size = in.readInt();

        if (size == -1)
            return null;

        Map<K, V> map = new HashMap<>(size, 1.0f);

        for (int i = 0; i < size; i++)
            map.put((K)in.readObject(), (V)in.readObject());

        return map;
    }

    /**
     * Calculate a hashCode for an array.
     *
     * @param obj Object.
     */
    public static int hashCode(Object obj) {
        if (obj == null)
            return 0;

        if (obj.getClass().isArray()) {
            if (obj instanceof byte[])
                return Arrays.hashCode((byte[])obj);
            if (obj instanceof short[])
                return Arrays.hashCode((short[])obj);
            if (obj instanceof int[])
                return Arrays.hashCode((int[])obj);
            if (obj instanceof long[])
                return Arrays.hashCode((long[])obj);
            if (obj instanceof float[])
                return Arrays.hashCode((float[])obj);
            if (obj instanceof double[])
                return Arrays.hashCode((double[])obj);
            if (obj instanceof char[])
                return Arrays.hashCode((char[])obj);
            if (obj instanceof boolean[])
                return Arrays.hashCode((boolean[])obj);

            int result = 1;

            for (Object element : (Object[])obj)
                result = 31 * result + hashCode(element);

            return result;
        }
        else
            return obj.hashCode();
    }

    /**
     * @param in Input.
     * @return Read map.
     * @throws IOException If de-serialization failed.
     * @throws ClassNotFoundException If deserialized class could not be found.
     */
    @Nullable public static <K, V> TreeMap<K, V> readTreeMap(
        ObjectInput in) throws IOException, ClassNotFoundException {
        int size = in.readInt();

        if (size == -1)
            return null;

        TreeMap<K, V> map = new TreeMap<>();

        for (int i = 0; i < size; i++)
            map.put((K)in.readObject(), (V)in.readObject());

        return map;
    }

    /**
     * Read hash map.
     *
     * @param in Input.
     * @return Read map.
     * @throws IOException If de-serialization failed.
     * @throws ClassNotFoundException If deserialized class could not be found.
     */
    @Nullable public static <K, V> HashMap<K, V> readHashMap(ObjectInput in)
        throws IOException, ClassNotFoundException {
        int size = in.readInt();

        // Check null flag.
        if (size == -1)
            return null;

        HashMap<K, V> map = U.newHashMap(size);

        for (int i = 0; i < size; i++)
            map.put((K)in.readObject(), (V)in.readObject());

        return map;
    }

    /**
     *
     * @param in Input.
     * @return Read map.
     * @throws IOException If de-serialization failed.
     * @throws ClassNotFoundException If deserialized class could not be found.
     */
    @Nullable public static <K, V> LinkedHashMap<K, V> readLinkedMap(ObjectInput in)
        throws IOException, ClassNotFoundException {
        int size = in.readInt();

        // Check null flag.
        if (size == -1)
            return null;

        LinkedHashMap<K, V> map = new LinkedHashMap<>(size, 1.0f);

        for (int i = 0; i < size; i++)
            map.put((K)in.readObject(), (V)in.readObject());

        return map;
    }

    /**
     * @param out Output.
     * @param map Map to write.
     * @throws IOException If write failed.
     */
    public static void writeIntKeyMap(ObjectOutput out, Map<Integer, ?> map) throws IOException {
        if (map != null) {
            out.writeInt(map.size());

            for (Map.Entry<Integer, ?> e : map.entrySet()) {
                out.writeInt(e.getKey());
                out.writeObject(e.getValue());
            }
        }
        else
            out.writeInt(-1);
    }

    /**
     * @param in Input.
     * @return Read map.
     * @throws IOException If de-serialization failed.
     * @throws ClassNotFoundException If deserialized class could not be found.
     */
    @Nullable public static <V> Map<Integer, V> readIntKeyMap(ObjectInput in) throws IOException,
        ClassNotFoundException {
        int size = in.readInt();

        // Check null flag.
        if (size == -1)
            return null;

        Map<Integer, V> map = new HashMap<>(size, 1.0f);

        for (int i = 0; i < size; i++)
            map.put(in.readInt(), (V)in.readObject());

        return map;
    }

    /**
     * @param out Output.
     * @param map Map to write.
     * @throws IOException If write failed.
     */
    public static void writeIntKeyIntValueMap(DataOutput out, Map<Integer, Integer> map) throws IOException {
        if (map != null) {
            out.writeBoolean(true);

            out.writeInt(map.size());

            for (Map.Entry<Integer, Integer> e : map.entrySet()) {
                out.writeInt(e.getKey());
                out.writeInt(e.getValue());
            }
        }
        else
            out.writeBoolean(false);
    }

    /**
     * @param in Input.
     * @return Read map.
     * @throws IOException If de-serialization failed.
     */
    @Nullable public static Map<Integer, Integer> readIntKeyIntValueMap(DataInput in) throws IOException {
        Map<Integer, Integer> map = null;

        // Check null flag.
        if (in.readBoolean()) {
            int size = in.readInt();

            map = new HashMap<>(size, 1.0f);

            for (int i = 0; i < size; i++)
                map.put(in.readInt(), in.readInt());
        }

        return map;
    }

    /**
     * @param in Input.
     * @return Deserialized list.
     * @throws IOException If deserialization failed.
     * @throws ClassNotFoundException If deserialized class could not be found.
     */
    @Nullable public static <E> List<E> readList(ObjectInput in) throws IOException, ClassNotFoundException {
        int size = in.readInt();

        // Check null flag.
        if (size == -1)
            return null;

        List<E> col = new ArrayList<>(size);

        for (int i = 0; i < size; i++)
            col.add((E)in.readObject());

        return col;
    }

    /**
     * @param in Input.
     * @return Deserialized list.
     * @throws IOException If deserialization failed.
     */
    @Nullable public static List<Integer> readIntList(DataInput in) throws IOException {
        int size = in.readInt();

        // Check null flag.
        if (size == -1)
            return null;

        List<Integer> col = new ArrayList<>(size);

        for (int i = 0; i < size; i++)
            col.add(in.readInt());

        return col;
    }

    /**
     * @param in Input.
     * @return Deserialized set.
     * @throws IOException If deserialization failed.
     * @throws ClassNotFoundException If deserialized class could not be found.
     */
    @SuppressWarnings({"unchecked"})
    @Nullable public static <E> Set<E> readSet(ObjectInput in) throws IOException, ClassNotFoundException {
        int size = in.readInt();

        // Check null flag.
        if (size == -1)
            return null;

        Set<E> set = new HashSet(size, 1.0f);

        for (int i = 0; i < size; i++)
            set.add((E)in.readObject());

        return set;
    }

    /**
     * @param in Input.
     * @return Deserialized set.
     * @throws IOException If deserialization failed.
     */
    @Nullable public static Set<Integer> readIntSet(DataInput in) throws IOException {
        int size = in.readInt();

        // Check null flag.
        if (size == -1)
            return null;

        Set<Integer> set = new HashSet<>(size, 1.0f);

        for (int i = 0; i < size; i++)
            set.add(in.readInt());

        return set;
    }

    /**
     * Writes string to output stream accounting for {@code null} values.
     * <p>
     * Limitation for max string lenght of {@link #UTF_BYTE_LIMIT} bytes is caused by {@link ObjectOutputStream#writeUTF}
     * used under the hood to perform an actual write.
     * </p>
     * <p>
     * If longer string is passes a {@link UTFDataFormatException} exception will be thrown.
     * </p>
     * <p>
     * To write longer strings use one of two options:
     * <ul>
     *     <li>
     *         {@link #writeLongString(DataOutput, String)} writes string as is converting it into binary array of UTF-8
     *         encoded characters.
     *         To read the value back {@link #readLongString(DataInput)} should be used.
     *     </li>
     *     <li>
     *         {@link #writeCutString(DataOutput, String)} cuts passed string to {@link #UTF_BYTE_LIMIT} bytes
     *         and then writes them without converting to byte array.
     *         No exceptions will be thrown for string of any length; written string can be read back with regular
     *         {@link #readString(DataInput)} method.
     *     </li>
     * </ul>
     * </p>
     *
     * @param out Output stream to write to.
     * @param s String to write, possibly {@code null}.
     * @throws IOException If write failed.
     */
    public static void writeString(DataOutput out, String s) throws IOException {
        // Write null flag.
        out.writeBoolean(s == null);

        if (s != null)
            out.writeUTF(s);
    }

    /**
     * Reads string from input stream accounting for {@code null} values.
     *
     * Method enables to read strings shorter than {@link #UTF_BYTE_LIMIT} bytes in UTF-8 otherwise an exception will be thrown.
     *
     * Strings written by {@link #writeString(DataOutput, String)} or {@link #writeCutString(DataOutput, String)}
     * can be read by this method.
     *
     * @see #writeString(DataOutput, String) for more information about writing strings.
     *
     * @param in Stream to read from.
     * @return Read string, possibly {@code null}.
     * @throws IOException If read failed.
     */
    @Nullable public static String readString(DataInput in) throws IOException {
        // If value is not null, then read it. Otherwise return null.
        return !in.readBoolean() ? in.readUTF() : null;
    }

    /**
     * Writes enum to output stream accounting for {@code null} values.
     * Note: method writes only one byte for every enum. Therefore, this method
     * only for Enums with maximum count of values equals to 128.
     *
     * @param out Output stream to write to.
     * @param e Enum value to write, possibly {@code null}.
     * @throws IOException If write failed.
     */
    public static <E extends Enum<E>> void writeEnum(DataOutput out, E e) throws IOException {
        out.writeByte(e == null ? -1 : e.ordinal());
    }

    /** */
    public static <E extends Enum<E>> E readEnum(DataInput in, Class<E> enumCls) throws IOException {
        byte ordinal = in.readByte();

        if (ordinal == (byte)-1)
            return null;

        int idx = ordinal & 0xFF;

        E[] values = enumCls.getEnumConstants();

        return idx < values.length ? values[idx] : null;
    }

    /**
     * Gets collection value by index.
     *
     * @param vals Collection of values.
     * @param idx Index of value in the collection.
     * @param <T> Type of collection values.
     * @return Value at the given index.
     */
    public static <T> T getByIndex(Collection<T> vals, int idx) {
        assert idx < vals.size();

        int i = 0;

        for (T val : vals) {
            if (idx == i)
                return val;

            i++;
        }

        assert false : "Should never be reached.";

        return null;
    }

    /**
     * Gets annotation for a class.
     *
     * @param <T> Type of annotation to return.
     * @param cls Class to get annotation from.
     * @param annCls Annotation to get.
     * @return Instance of annotation, or {@code null} if not found.
     */
    @Nullable public static <T extends Annotation> T getAnnotation(Class<?> cls, Class<T> annCls) {
        if (cls == Object.class)
            return null;

        T ann = cls.getAnnotation(annCls);

        if (ann != null)
            return ann;

        for (Class<?> itf : cls.getInterfaces()) {
            ann = getAnnotation(itf, annCls); // Recursion.

            if (ann != null)
                return ann;
        }

        if (!cls.isInterface()) {
            ann = getAnnotation(cls.getSuperclass(), annCls);

            if (ann != null)
                return ann;
        }

        return null;
    }

    /**
     * Gets declared annotation for a class.
     *
     * @param <T> Type of annotation to return.
     * @param cls Class to get annotation from.
     * @param annCls Annotation to get.
     * @return Instance of annotation, or {@code null} if not found.
     */
    @Nullable public static <T extends Annotation> T getDeclaredAnnotation(Class<?> cls, Class<T> annCls) {
        if (cls == Object.class)
            return null;

        return cls.getDeclaredAnnotation(annCls);
    }

    /**
     * Indicates if class has given declared annotation.
     *
     * @param <T> Annotation type.
     * @param cls Class to get annotation from.
     * @param annCls Annotation to get.
     * @return {@code true} if class has annotation or {@code false} otherwise.
     */
    public static <T extends Annotation> boolean hasDeclaredAnnotation(Class<?> cls, Class<T> annCls) {
        return getDeclaredAnnotation(cls, annCls) != null;
    }

    /**
     * Indicates if class has given annotation.
     *
     * @param o Object to get annotation from.
     * @param annCls Annotation to get.
     * @return {@code true} if class has annotation or {@code false} otherwise.
     */
    public static <T extends Annotation> boolean hasDeclaredAnnotation(Object o, Class<T> annCls) {
        return o != null && hasDeclaredAnnotation(o.getClass(), annCls);
    }

    /**
     * Indicates if class has given annotation.
     *
     * @param <T> Annotation type.
     * @param cls Class to get annotation from.
     * @param annCls Annotation to get.
     * @return {@code true} if class has annotation or {@code false} otherwise.
     */
    public static <T extends Annotation> boolean hasAnnotation(Class<?> cls, Class<T> annCls) {
        return getAnnotation(cls, annCls) != null;
    }

    /**
     * Indicates if class has given annotation.
     *
     * @param o Object to get annotation from.
     * @param annCls Annotation to get.
     * @return {@code true} if class has annotation or {@code false} otherwise.
     */
    public static <T extends Annotation> boolean hasAnnotation(Object o, Class<T> annCls) {
        return o != null && hasAnnotation(o.getClass(), annCls);
    }

    /**
     * Gets simple class name taking care of empty names.
     *
     * @param cls Class to get the name for.
     * @return Simple class name.
     */
    public static String getSimpleName(Class<?> cls) {
        String name = cls.getSimpleName();

        if (F.isEmpty(name))
            name = cls.getName().substring(cls.getPackage().getName().length() + 1);

        return name;
    }

    /**
     * Checks if the map passed in is contained in base map.
     *
     * @param base Base map.
     * @param map Map to check.
     * @return {@code True} if all entries within map are contained in base map,
     *      {@code false} otherwise.
     */
    public static boolean containsAll(Map<?, ?> base, Map<?, ?> map) {
        assert base != null;
        assert map != null;

        for (Map.Entry<?, ?> entry : map.entrySet())
            if (base.containsKey(entry.getKey())) {
                Object val = base.get(entry.getKey());

                if (val == null && entry.getValue() == null)
                    continue;

                if (val == null || entry.getValue() == null || !val.equals(entry.getValue()))
                    // Mismatch found.
                    return false;
            }
            else
                return false;

        // All entries in 'map' are contained in base map.
        return true;
    }

    /**
     * Gets task name for the given task class.
     *
     * @param taskCls Task class.
     * @return Either task name from class annotation (see {@link org.apache.ignite.compute.ComputeTaskName}})
     *      or task class name if there is no annotation.
     */
    public static String getTaskName(Class<? extends ComputeTask<?, ?>> taskCls) {
        ComputeTaskName nameAnn = getAnnotation(taskCls, ComputeTaskName.class);

        return nameAnn == null ? taskCls.getName() : nameAnn.value();
    }

    /**
     * Gets resource name.
     * Returns a task name if it is a Compute task or a class name otherwise.
     *
     * @param rscCls Class of resource.
     * @return Name of resource.
     */
    public static String getResourceName(Class rscCls) {
        if (ComputeTask.class.isAssignableFrom(rscCls))
            return getTaskName(rscCls);

        return rscCls.getName();
    }

    /**
     * Creates SPI attribute name by adding prefix to the attribute name.
     * Prefix is an SPI name + '.'.
     *
     * @param spi SPI.
     * @param attrName attribute name.
     * @return SPI attribute name.
     */
    public static String spiAttribute(IgniteSpi spi, String attrName) {
        assert spi != null;
        assert spi.getName() != null;

        return spi.getName() + '.' + attrName;
    }

    /**
     * Gets resource path for the class.
     *
     * @param clsName Class name.
     * @return Resource name for the class.
     */
    public static String classNameToResourceName(String clsName) {
        return clsName.replaceAll("\\.", "/") + ".class";
    }

    /**
     * Gets runtime MBean.
     *
     * @return Runtime MBean.
     */
    public static RuntimeMXBean getRuntimeMx() {
        return ManagementFactory.getRuntimeMXBean();
    }

    /**
     * Gets threading MBean.
     *
     * @return Threading MBean.
     */
    public static ThreadMXBean getThreadMx() {
        return ManagementFactory.getThreadMXBean();
    }

    /**
     * Gets OS MBean.
     * @return OS MBean.
     */
    public static OperatingSystemMXBean getOsMx() {
        return ManagementFactory.getOperatingSystemMXBean();
    }

    /**
     * Gets memory MBean.
     *
     * @return Memory MBean.
     */
    public static MemoryMXBean getMemoryMx() {
        return ManagementFactory.getMemoryMXBean();
    }

    /**
     * Gets amount of RAM memory available on this machine.
     *
     * @return Total amount of memory in bytes or -1 if any exception happened.
     */
    public static long getTotalMemoryAvailable() {
        MBeanServer mBeanSrv = ManagementFactory.getPlatformMBeanServer();

        Object attr;

        try {
            attr = mBeanSrv.getAttribute(
                    ObjectName.getInstance("java.lang", "type", "OperatingSystem"),
                    "TotalPhysicalMemorySize");
        }
        catch (Exception e) {
            return -1;
        }

        return (attr instanceof Long) ? (Long) attr : -1;
    }

    /**
     * Gets compilation MBean.
     *
     * @return Compilation MBean.
     */
    public static CompilationMXBean getCompilerMx() {
        return ManagementFactory.getCompilationMXBean();
    }

    /**
     * Tries to detect user class from passed in object inspecting
     * collections, arrays or maps.
     *
     * @param obj Object.
     * @return First non-JDK or deployment aware class or passed in object class.
     */
    public static Class<?> detectClass(Object obj) {
        assert obj != null;

        if (obj instanceof GridPeerDeployAware)
            return ((GridPeerDeployAware)obj).deployClass();

        if (U.isPrimitiveArray(obj))
            return obj.getClass();

        if (!U.isJdk(obj.getClass()))
            return obj.getClass();

        if (obj instanceof Iterable<?>) {
            Object o = F.first((Iterable<?>)obj);

            // No point to continue, if null.
            return o != null ? o.getClass() : obj.getClass();
        }

        if (obj instanceof Map) {
            Map.Entry<?, ?> e = F.firstEntry((Map<?, ?>)obj);

            if (e != null) {
                Object k = e.getKey();

                if (k != null && !U.isJdk(k.getClass()))
                    return k.getClass();

                Object v = e.getValue();

                return v != null ? v.getClass() : obj.getClass();
            }
        }

        if (obj.getClass().isArray()) {
            int len = Array.getLength(obj);

            if (len > 0) {
                Object o = Array.get(obj, 0);

                return o != null ? o.getClass() : obj.getClass();
            }
            else
                return obj.getClass().getComponentType();
        }

        return obj.getClass();
    }

    /**
     * Detects class loader for given class.
     * <p>
     * This method will first check if {@link Thread#getContextClassLoader()} is appropriate.
     * If yes, then context class loader will be returned, otherwise
     * the {@link Class#getClassLoader()} will be returned.
     *
     * @param cls Class to find class loader for.
     * @return Class loader for given class (never {@code null}).
     */
    public static ClassLoader detectClassLoader(Class<?> cls) {
        return GridClassLoaderCache.classLoader(cls);
    }

    /**
     * Detects class loader for given object's class.
     *
     * @param obj Object to find class loader for class of.
     * @return Class loader for given object (possibly {@code null}).
     */
    @Nullable public static ClassLoader detectObjectClassLoader(@Nullable Object obj) {
        if (obj == null)
            return null;

        if (obj instanceof GridPeerDeployAware)
            return ((GridPeerDeployAware)obj).classLoader();

        return detectClassLoader(obj.getClass());
    }

    /**
     * Tests whether or not given class is loadable provided class loader.
     *
     * @param clsName Class name to test.
     * @param ldr Class loader to test with. If {@code null} - we'll use system class loader instead.
     *      If System class loader is not set - this method will return {@code false}.
     * @return {@code True} if class is loadable, {@code false} otherwise.
     */
    public static boolean isLoadableBy(String clsName, @Nullable ClassLoader ldr) {
        assert clsName != null;

        if (ldr == null)
            ldr = gridClassLoader;

        String lambdaParent = U.lambdaEnclosingClassName(clsName);

        try {
            ldr.loadClass(lambdaParent == null ? clsName : lambdaParent);

            return true;
        }
        catch (ClassNotFoundException ignore) {
            return false;
        }
    }

    /**
     * Gets the peer deploy aware instance for the object with the widest class loader.
     * If collection is {@code null}, empty or contains only {@code null}s - the peer
     * deploy aware object based on system class loader will be returned.
     *
     * @param c Collection.
     * @return Peer deploy aware object from this collection with the widest class loader.
     * @throws IllegalArgumentException Thrown in case when common class loader for all
     *      elements in this collection cannot be found. In such case - peer deployment
     *      is not possible.
     */
    public static GridPeerDeployAware peerDeployAware0(@Nullable Iterable<?> c) {
        if (!F.isEmpty(c)) {
            assert c != null;

            // We need to find common classloader for all elements AND the collection itself
            Collection<Object> tmpC = new ArrayList<>();

            for (Object e : c)
                tmpC.add(e);

            tmpC.add(c);

            boolean notAllNulls = false;

            for (Object obj : tmpC) {
                if (obj != null) {
                    notAllNulls = true;

                    if (hasCommonClassLoader(obj, tmpC))
                        return obj == c ? peerDeployAware(obj) : peerDeployAware0(obj);
                }
            }

            // If all are nulls - don't throw an exception.
            if (notAllNulls)
                throw new IllegalArgumentException("Failed to find common class loader for all elements in " +
                    "given collection. Peer deployment cannot be performed for such collection.");
        }

        return peerDeployAware(c);
    }

    /**
     * Check if all elements from the collection could be loaded with the same classloader as the given object.
     *
     * @param obj base object.
     * @param c collection to check elements from.
     * @return {@code true} if all elements could be loaded with {@code obj}'s classloader, {@code false} otherwise
     */
    private static boolean hasCommonClassLoader(Object obj, Iterable<?> c) {
        assert obj != null;
        assert c != null;

        ClassLoader ldr = obj instanceof GridPeerDeployAware ?
            ((GridPeerDeployAware)obj).classLoader() : detectClassLoader(obj.getClass());

        boolean found = true;

        for (Object obj2 : c) {
            if (obj2 == null || obj2 == obj)
                continue;

            // Obj2 class name.
            String clsName = obj2 instanceof GridPeerDeployAware ?
                ((GridPeerDeployAware)obj2).deployClass().getName() : obj2.getClass().getName();

            if (!isLoadableBy(clsName, ldr)) {
                found = false;

                break;
            }
        }

        return found;
    }

    /**
     * Gets the peer deploy aware instance for the object with the widest class loader.
     * If array is {@code null}, empty or contains only {@code null}s - the peer
     * deploy aware object based on system class loader will be returned.
     *
     * @param c Objects.
     * @return Peer deploy aware object from this array with the widest class loader.
     * @throws IllegalArgumentException Thrown in case when common class loader for all
     *      elements in this array cannot be found. In such case - peer deployment
     *      is not possible.
     */
    @SuppressWarnings({"ZeroLengthArrayAllocation"})
    public static GridPeerDeployAware peerDeployAware0(@Nullable Object... c) {
        if (!F.isEmpty(c)) {
            assert c != null;

            boolean notAllNulls = false;

            for (Object obj : c) {
                if (obj != null) {
                    notAllNulls = true;

                    ClassLoader ldr = obj instanceof GridPeerDeployAware ?
                        ((GridPeerDeployAware)obj).classLoader() : obj.getClass().getClassLoader();

                    boolean found = true;

                    for (Object obj2 : c) {
                        if (obj2 == null || obj2 == obj)
                            continue;

                        // Obj2 class name.
                        String clsName = obj2 instanceof GridPeerDeployAware ?
                            ((GridPeerDeployAware)obj2).deployClass().getName() : obj2.getClass().getName();

                        if (!isLoadableBy(clsName, ldr)) {
                            found = false;

                            break;
                        }
                    }

                    if (found)
                        return peerDeployAware0(obj);
                }
            }

            // If all are nulls - don't throw an exception.
            if (notAllNulls)
                throw new IllegalArgumentException("Failed to find common class loader for all elements in " +
                    "given collection. Peer deployment cannot be performed for such collection.");
        }

        return peerDeployAware(new Object[0]);
    }

    /**
     * Creates an instance of {@link GridPeerDeployAware} for object.
     *
     * Checks, if the object is an instance of collection or object
     * array.
     *
     * @param obj Object to deploy.
     * @return {@link GridPeerDeployAware} instance for given object.
     */
    public static GridPeerDeployAware peerDeployAware0(Object obj) {
        if (obj instanceof Iterable)
            return peerDeployAware0((Iterable)obj);

        if (obj.getClass().isArray() && !U.isPrimitiveArray(obj))
            return peerDeployAware0((Object[])obj);

        return peerDeployAware(obj);
    }

    /**
     * Creates an instance of {@link GridPeerDeployAware} for object.
     *
     * @param obj Object to deploy.
     * @return {@link GridPeerDeployAware} instance for given object.
     */
    public static GridPeerDeployAware peerDeployAware(Object obj) {
        assert obj != null;

        if (obj instanceof GridPeerDeployAware)
            return (GridPeerDeployAware)obj;

        final Class<?> cls = obj instanceof Class ? (Class)obj : obj.getClass();

        return new GridPeerDeployAware() {
            /** */
            private ClassLoader ldr;

            @Override public Class<?> deployClass() {
                return cls;
            }

            @Override public ClassLoader classLoader() {
                if (ldr == null)
                    ldr = detectClassLoader(cls);

                return ldr;
            }
        };
    }

    /**
     * Unwraps top level user class for wrapped objects.
     *
     * @param obj Object to check.
     * @return Top level user class.
     */
    public static GridPeerDeployAware detectPeerDeployAware(GridPeerDeployAware obj) {
        GridPeerDeployAware p = nestedPeerDeployAware(obj, true, new GridLeanIdentitySet<>());

        // Pass in obj.getClass() to avoid infinite recursion.
        return p != null ? p : peerDeployAware(obj.getClass());
    }

    /**
     * Gets peer deploy class if there is any {@link GridPeerDeployAware} within reach.
     *
     * @param obj Object to check.
     * @param top Indicates whether object is top level or a nested field.
     * @param processed Set of processed objects to avoid infinite recursion.
     * @return Peer deploy class, or {@code null} if one could not be found.
     */
    @Nullable private static GridPeerDeployAware nestedPeerDeployAware(Object obj, boolean top, Set<Object> processed) {
        // Avoid infinite recursion.
        if (!processed.add(obj))
            return null;

        if (obj instanceof GridPeerDeployAware) {
            GridPeerDeployAware p = (GridPeerDeployAware)obj;

            if (!top && p.deployClass() != null)
                return p;

            for (Class<?> cls = obj.getClass(); !cls.equals(Object.class); cls = cls.getSuperclass()) {
                // Cache by class name instead of class to avoid infinite growth of the
                // caching map in case of multiple redeployment of the same class.
                IgniteBiTuple<Class<?>, Collection<Field>> tup = p2pFields.get(cls.getName());

                boolean cached = tup != null && tup.get1().equals(cls);

                Iterable<Field> fields = cached ? tup.get2() : Arrays.asList(cls.getDeclaredFields());

                if (!cached) {
                    tup = new IgniteBiTuple<>();

                    tup.set1(cls);
                }

                for (Field f : fields)
                    // Special handling for anonymous classes.
                    if (cached || f.getName().startsWith("this$") || f.getName().startsWith("val$")) {
                        if (!cached) {
                            f.setAccessible(true);

                            if (tup.get2() == null)
                                tup.set2(new LinkedList<Field>());

                            tup.get2().add(f);
                        }

                        try {
                            Object o = f.get(obj);

                            if (o != null) {
                                // Recursion.
                                p = nestedPeerDeployAware(o, false, processed);

                                if (p != null) {
                                    if (!cached)
                                        // Potentially replace identical value
                                        // stored by another thread.
                                        p2pFields.put(cls.getName(), tup);

                                    return p;
                                }
                            }
                        }
                        catch (IllegalAccessException ignored) {
                            return null;
                        }
                    }
            }
        }
        // Don't go into internal Ignite structures.
        else if (isIgnite(obj.getClass()))
            return null;
        else if (obj instanceof Iterable)
            for (Object o : (Iterable<?>)obj) {
                // Recursion.
                GridPeerDeployAware p = nestedPeerDeployAware(o, false, processed);

                if (p != null)
                    return p;
            }
        else if (obj.getClass().isArray()) {
            Class<?> type = obj.getClass().getComponentType();

            // We don't care about primitives or internal JDK types.
            if (!type.isPrimitive() && !isJdk(type)) {
                Object[] arr = (Object[])obj;

                for (Object o : arr) {
                    // Recursion.
                    GridPeerDeployAware p = nestedPeerDeployAware(o, false, processed);

                    if (p != null)
                        return p;
                }
            }
        }

        return null;
    }

    /**
     * Checks if given class is of {@code Ignite} type.
     *
     * @param cls Class to check.
     * @return {@code True} if given class is of {@code Ignite} type.
     */
    public static boolean isIgnite(Class<?> cls) {
        String name = cls.getName();

        return name.startsWith("org.apache.ignite") || name.startsWith("org.jsr166");
    }

    /**
     * Checks if given class is of {@code Grid} type.
     *
     * @param cls Class to check.
     * @return {@code True} if given class is of {@code Grid} type.
     */
    public static boolean isGrid(Class<?> cls) {
        return cls.getName().startsWith("org.apache.ignite.internal");
    }

    /**
     * Replaces all occurrences of {@code org.apache.ignite.} with {@code o.a.i.},
     * {@code org.apache.ignite.internal.} with {@code o.a.i.i.},
     * {@code org.apache.ignite.internal.visor.} with {@code o.a.i.i.v.} and
     * {@code org.apache.ignite.scalar.} with {@code o.a.i.s.}.
     *
     * @param s String to replace in.
     * @return Replaces string.
     */
    public static String compact(String s) {
        return s.replace("org.apache.ignite.internal.visor.", "o.a.i.i.v.").
            replace("org.apache.ignite.internal.", "o.a.i.i.").
            replace("org.apache.ignite.scalar.", "o.a.i.s.").
                replace(IGNITE_PKG, "o.a.i.");
    }

    /**
     * Check if given class is of JDK type.
     *
     * @param cls Class to check.
     * @return {@code True} if object is JDK type.
     */
    public static boolean isJdk(Class<?> cls) {
        if (cls.isPrimitive())
            return true;

        String s = cls.getName();

        return s.startsWith("java.") || s.startsWith("javax.");
    }

    /**
     * Check if given class represents a Enum.
     *
     * @param cls Class to check.
     * @return {@code True} if this is a Enum class.
     */
    public static boolean isEnum(Class cls) {
        if (cls.isEnum())
            return true;

        Class sCls = cls.getSuperclass();

        return sCls != null && sCls.isEnum();
    }

    /**
     * Converts {@link InterruptedException} to {@link IgniteCheckedException}.
     *
     * @param mux Mux to wait on.
     * @throws IgniteInterruptedCheckedException If interrupted.
     */
    @SuppressWarnings({"WaitNotInLoop"})
    public static void wait(Object mux) throws IgniteInterruptedCheckedException {
        try {
            mux.wait();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IgniteInterruptedCheckedException(e);
        }
    }

    /**
     * Unzip file to folder.
     *
     * @param zipFile ZIP file.
     * @param toDir Directory to unzip file content.
     * @param log Grid logger.
     * @throws IOException In case of error.
     */
    @SuppressWarnings({"ResultOfMethodCallIgnored"})
    public static void unzip(File zipFile, File toDir, @Nullable IgniteLogger log) throws IOException {
        ZipFile zip = null;

        try {
            zip = new ZipFile(zipFile);

            for (ZipEntry entry : asIterable(zip.entries())) {
                if (entry.isDirectory()) {
                    // Assume directories are stored parents first then children.
                    new File(toDir, entry.getName()).mkdirs();

                    continue;
                }

                InputStream in = null;
                OutputStream out = null;

                try {
                    in = zip.getInputStream(entry);

                    File outFile = new File(toDir, entry.getName());

                    if (!outFile.getParentFile().exists())
                        outFile.getParentFile().mkdirs();

                    out = new BufferedOutputStream(new FileOutputStream(outFile));

                    copy(in, out);
                }
                finally {
                    close(in, log);
                    close(out, log);
                }
            }
        }
        finally {
            if (zip != null)
                zip.close();
        }
    }

    /**
     * @return {@code True} if assertions enabled.
     */
    public static boolean assertionsEnabled() {
        return assertionsEnabled;
    }

    /**
     * Gets OS JDK string.
     *
     * @return OS JDK string.
     */
    public static String osJdkString() {
        return osJdkStr;
    }

    /**
     * Gets OS string.
     *
     * @return OS string.
     */
    public static String osString() {
        return osStr;
    }

    /**
     * Gets JDK string.
     *
     * @return JDK string.
     */
    public static String jdkString() {
        return jdkStr;
    }

    /**
     * Indicates whether current OS is Linux flavor.
     *
     * @return {@code true} if current OS is Linux - {@code false} otherwise.
     */
    public static boolean isLinux() {
        return linux;
    }

    /**
     * Gets JDK name.
     * @return JDK name.
     */
    public static String jdkName() {
        return jdkName;
    }

    /**
     * Gets JDK vendor.
     *
     * @return JDK vendor.
     */
    public static String jdkVendor() {
        return jdkVendor;
    }

    /**
     * Gets JDK version.
     *
     * @return JDK version.
     */
    public static String jdkVersion() {
        return jdkVer;
    }

    /**
     * Gets OS CPU-architecture.
     *
     * @return OS CPU-architecture.
     */
    public static String osArchitecture() {
        return osArch;
    }

    /**
     * Gets underlying OS name.
     *
     * @return Underlying OS name.
     */
    public static String osName() {
        return osName;
    }

    /**
     * Gets underlying OS version.
     *
     * @return Underlying OS version.
     */
    public static String osVersion() {
        return osVer;
    }

    /**
     * Indicates whether current OS is Mac OS.
     *
     * @return {@code true} if current OS is Mac OS - {@code false} otherwise.
     */
    public static boolean isMacOs() {
        return mac;
    }

    /**
     * @return {@code True} if current OS is RedHat.
     */
    public static boolean isRedHat() {
        return redHat;
    }

    /**
     * Indicates whether current OS is Netware.
     *
     * @return {@code true} if current OS is Netware - {@code false} otherwise.
     */
    public static boolean isNetWare() {
        return netware;
    }

    /**
     * Indicates whether current OS is Solaris.
     *
     * @return {@code true} if current OS is Solaris (SPARC or x86) - {@code false} otherwise.
     */
    public static boolean isSolaris() {
        return solaris;
    }

    /**
     * Indicates whether current OS is Solaris on Spark box.
     *
     * @return {@code true} if current OS is Solaris SPARC - {@code false} otherwise.
     */
    public static boolean isSolarisSparc() {
        return solaris && sparc;
    }

    /**
     * Indicates whether current OS is Solaris on x86 box.
     *
     * @return {@code true} if current OS is Solaris x86 - {@code false} otherwise.
     */
    public static boolean isSolarisX86() {
        return solaris && x86;
    }

    /**
     * Indicates whether current architecture is Intel x86.
     *
     * @return {@code true} if current architecture is Intel x86 - {@code false} otherwise.
     */
    public static boolean isX86() {
        return x86;
    }

    /**
     * Indicates whether current architecture is AMD64.
     *
     * @return {@code true} if current architecture is AMD64 - {@code false} otherwise.
     */
    public static boolean isAmd64() {
        return amd64;
    }

    /**
     * Indicates whether current OS is UNIX flavor.
     *
     * @return {@code true} if current OS is UNIX - {@code false} otherwise.
     */
    public static boolean isUnix() {
        return unix;
    }

    /**
     * Indicates whether current OS is Windows.
     *
     * @return {@code true} if current OS is Windows (any versions) - {@code false} otherwise.
     */
    public static boolean isWindows() {
        return win7 || win8 || win81 || winXp || win95 || win98 || winNt || win2k ||
            win2003 || win2008 || winVista || unknownWin;
    }

    /**
     * Indicates whether current OS is Windows Vista.
     *
     * @return {@code true} if current OS is Windows Vista - {@code false} otherwise.
     */
    public static boolean isWindowsVista() {
        return winVista;
    }

    /**
     * Indicates whether current OS is Windows 7.
     *
     * @return {@code true} if current OS is Windows 7 - {@code false} otherwise.
     */
    public static boolean isWindows7() {
        return win7;
    }

    /**
     * Indicates whether current OS is Windows 8.
     *
     * @return {@code true} if current OS is Windows 8 - {@code false} otherwise.
     */
    public static boolean isWindows8() {
        return win8;
    }

    /**
     * Indicates whether current OS is Windows 8.1.
     *
     * @return {@code true} if current OS is Windows 8.1 - {@code false} otherwise.
     */
    public static boolean isWindows81() {
        return win81;
    }

    /**
     * Indicates whether current OS is Windows 2000.
     *
     * @return {@code true} if current OS is Windows 2000 - {@code false} otherwise.
     */
    public static boolean isWindows2k() {
        return win2k;
    }

    /**
     * Indicates whether current OS is Windows Server 2003.
     *
     * @return {@code true} if current OS is Windows Server 2003 - {@code false} otherwise.
     */
    public static boolean isWindows2003() {
        return win2003;
    }

    /**
     * Indicates whether current OS is Windows Server 2008.
     *
     * @return {@code true} if current OS is Windows Server 2008 - {@code false} otherwise.
     */
    public static boolean isWindows2008() {
        return win2008;
    }

    /**
     * Indicates whether current OS is Windows 95.
     *
     * @return {@code true} if current OS is Windows 95 - {@code false} otherwise.
     */
    public static boolean isWindows95() {
        return win95;
    }

    /**
     * Indicates whether current OS is Windows 98.
     *
     * @return {@code true} if current OS is Windows 98 - {@code false} otherwise.
     */
    public static boolean isWindows98() {
        return win98;
    }

    /**
     * Indicates whether current OS is Windows NT.
     *
     * @return {@code true} if current OS is Windows NT - {@code false} otherwise.
     */
    public static boolean isWindowsNt() {
        return winNt;
    }

    /**
     * Indicates whether current OS is Windows XP.
     *
     * @return {@code true} if current OS is Windows XP- {@code false} otherwise.
     */
    public static boolean isWindowsXp() {
        return winXp;
    }

    /**
     * Gets JVM specification name.
     *
     * @return JVM specification name.
     */
    public static String jvmSpec() {
        return jvmSpecName;
    }

    /**
     * Gets JVM implementation version.
     *
     * @return JVM implementation version.
     */
    public static String jvmVersion() {
        return jvmImplVer;
    }

    /**
     * Gets JVM implementation vendor.
     *
     * @return JVM implementation vendor.
     */
    public static String jvmVendor() {
        return jvmImplVendor;
    }

    /**
     * Gets JVM implementation name.
     *
     * @return JVM implementation name.
     */
    public static String jvmName() {
        return jvmImplName;
    }

    /**
     * Does a best effort to detect if we a running on a 32-bit JVM.
     *
     * @return {@code true} if detected that we are running on a 32-bit JVM.
     */
    public static boolean jvm32Bit() {
        return jvm32Bit;
    }

    /**
     * Compare java implementation version
     *
     * @param v1 - java implementation version
     * @param v2 - java implementation version
     * @return the value {@code 0} if {@code v1 == v2};
     *         a value less than {@code 0} if {@code v1 < v2}; and
     *         a value greater than {@code 0} if {@code v1 > v2}
     */
    public static int compareVersionNumbers(@Nullable String v1, @Nullable String v2) {
        if (v1 == null && v2 == null)
            return 0;

        if (v1 == null)
            return -1;

        if (v2 == null)
            return 1;

        String[] part1 = v1.split("[\\.\\_\\-]");
        String[] part2 = v2.split("[\\.\\_\\-]");
        int idx = 0;

        for (; idx < part1.length && idx < part2.length; idx++) {
            String p1 = part1[idx];
            String p2 = part2[idx];

            int cmp = (p1.matches("\\d+") && p2.matches("\\d+"))
                ? Integer.valueOf(p1).compareTo(Integer.valueOf(p2)) : p1.compareTo(p2);

            if (cmp != 0)
                return cmp;
        }

        if (part1.length == part2.length)
            return 0;
        else
            return part1.length > idx ? 1 : -1;
    }

    /**
     * Gets node product version based on node attributes.
     *
     * @param node Node to get version from.
     * @return Version object.
     */
    public static IgniteProductVersion productVersion(ClusterNode node) {
        String verStr = node.attribute(ATTR_BUILD_VER);
        String buildDate = node.attribute(ATTR_BUILD_DATE);

        if (buildDate != null)
            verStr += '-' + buildDate;

        return IgniteProductVersion.fromString(verStr);
    }

    /**
     * Compare running Java Runtime version with {@code v}
     *
     * @param v - java implementation version
     * @return {@code true} if running on Java Runtime version greater than {@code v}
     */
    public static boolean isJavaVersionAtLeast(String v) {
        return compareVersionNumbers(javaRtVer, v) >= 0;
    }

    /**
     * Gets Java Runtime name.
     *
     * @return Java Runtime name.
     */
    public static String jreName() {
        return javaRtName;
    }

    /**
     * Gets Java Runtime version.
     *
     * @return Java Runtime version.
     */
    public static String jreVersion() {
        return javaRtVer;
    }

    /**
     * Get major Java version from string.
     *
     * @param verStr Version string.
     * @return Major version or zero if failed to resolve.
     */
    public static int majorJavaVersion(String verStr) {
        if (F.isEmpty(verStr))
            return 0;

        try {
            String[] parts = verStr.split("\\.");

            int major = Integer.parseInt(parts[0]);

            if (parts.length == 1)
                return major;

            int minor = Integer.parseInt(parts[1]);

            return major == 1 ? minor : major;
        }
        catch (Exception e) {
            return 0;
        }
    }

    /**
     * Indicates whether HotSpot VM is used.
     *
     * @return {@code true} if current JVM implementation is a Sun HotSpot VM, {@code false} otherwise.
     */
    public static boolean isHotSpot() {
        return jvmImplName.contains("Java HotSpot(TM)");
    }

    /**
     * Sets thread context class loader to the given loader, executes the closure, and then
     * resets thread context class loader to its initial value.
     *
     * @param ldr Class loader to run the closure under.
     * @param c Callable to run.
     * @param <R> Return type.
     * @return Return value.
     * @throws IgniteCheckedException If call failed.
     */
    @Nullable public static <R> R wrapThreadLoader(ClassLoader ldr, Callable<R> c) throws IgniteCheckedException {
        Thread curThread = Thread.currentThread();

        // Get original context class loader.
        ClassLoader ctxLdr = curThread.getContextClassLoader();

        try {
            curThread.setContextClassLoader(ldr);

            return c.call();
        }
        catch (IgniteCheckedException | RuntimeException e) {
            throw e;
        }
        catch (Exception e) {
            throw new IgniteCheckedException(e);
        }
        finally {
            // Set the original class loader back.
            curThread.setContextClassLoader(ctxLdr);
        }
    }

    /**
     * Sets thread context class loader to the given loader, executes the closure, and then
     * resets thread context class loader to its initial value.
     *
     * @param ldr Class loader to run the closure under.
     * @param c Closure to run.
     * @param <R> Return type.
     * @return Return value.
     */
    @Nullable public static <R> R wrapThreadLoader(ClassLoader ldr, IgniteOutClosure<R> c) {
        Thread curThread = Thread.currentThread();

        // Get original context class loader.
        ClassLoader ctxLdr = curThread.getContextClassLoader();

        try {
            curThread.setContextClassLoader(ldr);

            return c.apply();
        }
        finally {
            // Set the original class loader back.
            curThread.setContextClassLoader(ctxLdr);
        }
    }

    /**
     * Sets thread context class loader to the given loader, executes the closure, and then
     * resets thread context class loader to its initial value.
     *
     * @param ldr Class loader to run the closure under.
     * @param c Closure to run.
     */
    public static void wrapThreadLoader(ClassLoader ldr, Runnable c) {
        Thread curThread = Thread.currentThread();

        // Get original context class loader.
        ClassLoader ctxLdr = curThread.getContextClassLoader();

        try {
            curThread.setContextClassLoader(ldr);

            c.run();
        }
        finally {
            // Set the original class loader back.
            curThread.setContextClassLoader(ctxLdr);
        }
    }

    /**
     * Short node representation.
     *
     * @param n Grid node.
     * @return Short string representing the node.
     */
    public static String toShortString(ClusterNode n) {
        return "ClusterNode [id=" + n.id() + ", order=" + n.order() + ", addr=" + n.addresses() +
            ", daemon=" + n.isDaemon() + ']';
    }

    /**
     * Short node representation.
     *
     * @param ns Grid nodes.
     * @return Short string representing the node.
     */
    public static String toShortString(Collection<? extends ClusterNode> ns) {
        SB sb = new SB("Grid nodes [cnt=" + ns.size());

        for (ClusterNode n : ns)
            sb.a(", ").a(toShortString(n));

        return sb.a(']').toString();
    }

    /**
     * Get string representation of an object properly catching all exceptions.
     *
     * @param obj Object.
     * @return Result or {@code null}.
     */
    @Nullable public static String toStringSafe(@Nullable Object obj) {
        if (obj == null)
            return null;
        else {
            try {
                return obj.toString();
            }
            catch (Exception e) {
                try {
                    return "Failed to convert object to string: " + e.getMessage();
                }
                catch (Exception e0) {
                    return "Failed to convert object to string (error message is not available)";
                }
            }
        }
    }

    /**
     * Converts collection of integers into array.
     *
     * @param c Collection of integers.
     * @return Integer array.
     */
    public static int[] toIntArray(@Nullable Collection<Integer> c) {
        if (c == null || c.isEmpty())
            return EMPTY_INTS;

        int[] arr = new int[c.size()];

        int idx = 0;

        for (Integer i : c)
            arr[idx++] = i;

        return arr;
    }

    /**
     * @param arr1 Array 1.
     * @param arr2 Array 2.
     */
    public static int[] addAll(int[] arr1, int[] arr2) {
        int[] all = new int[arr1.length + arr2.length];

        System.arraycopy(arr1, 0, all, 0, arr1.length);
        System.arraycopy(arr2, 0, all, arr1.length, arr2.length);

        return all;
    }

    /**
     * Converts array of integers into list.
     *
     * @param arr Array of integers.
     * @param p Optional predicate array.
     * @return List of integers.
     */
    public static List<Integer> toIntList(@Nullable int[] arr, IgnitePredicate<Integer>... p) {
        if (arr == null || arr.length == 0)
            return Collections.emptyList();

        List<Integer> ret = new ArrayList<>(arr.length);

        if (F.isEmpty(p))
            for (int i : arr)
                ret.add(i);
        else {
            for (int i : arr)
                if (F.isAll(i, p))
                    ret.add(i);
        }

        return ret;
    }

    /**
     * Converts collection of integers into array.
     *
     * @param c Collection of integers.
     * @return Integer array.
     */
    public static long[] toLongArray(@Nullable Collection<Long> c) {
        if (c == null || c.isEmpty())
            return EMPTY_LONGS;

        long[] arr = new long[c.size()];

        int idx = 0;

        for (Long l : c)
            arr[idx++] = l;

        return arr;
    }

    /**
     * Converts array of longs into list.
     *
     * @param arr Array of longs.
     * @return List of longs.
     */
    public static List<Long> toLongList(@Nullable long[] arr) {
        if (arr == null || arr.length == 0)
            return Collections.emptyList();

        List<Long> ret = new ArrayList<>(arr.length);

        for (long l : arr)
            ret.add(l);

        return ret;
    }

    /**
     * Copies all elements from collection to array and asserts that
     * array is big enough to hold the collection. This method should
     * always be preferred to {@link Collection#toArray(Object[])}
     * method.
     *
     * @param c Collection to convert to array.
     * @param arr Array to populate.
     * @param <T> Element type.
     * @return Passed in array.
     */
    public static <T> T[] toArray(Collection<? extends T> c, T[] arr) {
        T[] a = c.toArray(arr);

        assert a == arr;

        return arr;
    }

    /**
     * Swaps two objects in array.
     *
     * @param arr Array.
     * @param a Index of the first object.
     * @param b Index of the second object.
     */
    public static void swap(Object[] arr, int a, int b) {
        Object tmp = arr[a];
        arr[a] = arr[b];
        arr[b] = tmp;
    }

    /**
     * Returns array which is the union of two arrays
     * (array of elements contained in any of provided arrays).
     * <p/>
     * Note: arrays must be increasing.
     *
     * @param a First array.
     * @param aLen Length of prefix {@code a}.
     * @param b Second array.
     * @param bLen Length of prefix {@code b}.
     * @return Increasing array which is union of {@code a} and {@code b}.
     */
    public static int[] unique(int[] a, int aLen, int[] b, int bLen) {
        assert a != null;
        assert b != null;
        assert isIncreasingArray(a, aLen);
        assert isIncreasingArray(b, bLen);

        int[] res = new int[aLen + bLen];
        int resLen = 0;

        int i = 0;
        int j = 0;

        while (i < aLen && j < bLen) {
            if (a[i] == b[j])
                i++;
            else if (a[i] < b[j])
                res[resLen++] = a[i++];
            else
                res[resLen++] = b[j++];
        }

        while (i < aLen)
            res[resLen++] = a[i++];

        while (j < bLen)
            res[resLen++] = b[j++];

        return copyIfExceeded(res, resLen);
    }

    /**
     * Returns array which is the difference between two arrays
     * (array of elements contained in first array but not contained in second).
     * <p/>
     * Note: arrays must be increasing.
     *
     * @param a First array.
     * @param aLen Length of prefix {@code a}.
     * @param b Second array.
     * @param bLen Length of prefix {@code b}.
     * @return Increasing array which is difference between {@code a} and {@code b}.
     */
    public static int[] difference(int[] a, int aLen, int[] b, int bLen) {
        assert a != null;
        assert b != null;
        assert isIncreasingArray(a, aLen);
        assert isIncreasingArray(b, bLen);

        int[] res = new int[aLen];
        int resLen = 0;

        int i = 0;
        int j = 0;

        while (i < aLen && j < bLen) {
            if (a[i] == b[j])
                i++;
            else if (a[i] < b[j])
                res[resLen++] = a[i++];
            else
                j++;
        }

        while (i < aLen)
            res[resLen++] = a[i++];

        return copyIfExceeded(res, resLen);
    }

    /**
     * Checks if array prefix increases.
     *
     * @param arr Array.
     * @param len Prefix length.
     * @return {@code True} if {@code arr} from 0 to ({@code len} - 1) increases.
     */
    public static boolean isIncreasingArray(int[] arr, int len) {
        assert arr != null;
        assert 0 <= len && len <= arr.length;

        if (arr.length == 0)
            return true;

        for (int i = 1; i < len; i++) {
            if (arr[i - 1] >= arr[i])
                return false;
        }

        return true;
    }

    /**
     * Checks if array prefix do not decreases.
     *
     * @param arr Array.
     * @param len Prefix length.
     * @return {@code True} if {@code arr} from 0 to ({@code len} - 1) do not decreases.
     */
    public static boolean isNonDecreasingArray(int[] arr, int len) {
        assert arr != null;
        assert 0 <= len && len <= arr.length;

        if (arr.length == 0)
            return true;

        for (int i = 1; i < len; i++) {
            if (arr[i - 1] > arr[i])
                return false;
        }

        return true;
    }

    /**
     * Copies array only if array length greater than needed length.
     *
     * @param arr Array.
     * @param len Prefix length.
     * @return Old array if length of {@code arr} is equals to {@code len},
     *      otherwise copy of array.
     */
    public static int[] copyIfExceeded(int[] arr, int len) {
        assert arr != null;
        assert 0 <= len && len <= arr.length;

        return len == arr.length ? arr : Arrays.copyOf(arr, len);
    }

    /**
     *
     * @param t Tokenizer.
     * @param str Input string.
     * @param date Date.
     * @return Next token.
     * @throws IgniteCheckedException Thrown in case of any errors.
     */
    private static boolean checkNextToken(StringTokenizer t, String str, String date) throws IgniteCheckedException {
        try {
            if (t.nextToken().equals(str))
                return true;
            else
                throw new IgniteCheckedException("Invalid date format: " + date);
        }
        catch (NoSuchElementException ignored) {
            return false;
        }
    }

    /**
     * Adds values to collection and returns the same collection to allow chaining.
     *
     * @param c Collection to add values to.
     * @param vals Values.
     * @param <V> Value type.
     * @return Passed in collection.
     */
    public static <V, C extends Collection<? super V>> C addAll(C c, V... vals) {
        Collections.addAll(c, vals);

        return c;
    }

    /**
     * Adds values to collection and returns the same collection to allow chaining.
     *
     * @param m Map to add entries to.
     * @param entries Entries.
     * @param <K> Key type.
     * @param <V> Value type.
     * @param <M> Map type.
     * @return Passed in collection.
     */
    public static <K, V, M extends Map<K, V>> M addAll(M m, Map.Entry<K, V>... entries) {
        for (Map.Entry<K, V> e : entries)
            m.put(e.getKey(), e.getValue());

        return m;
    }

    /**
     * Adds values to collection and returns the same collection to allow chaining.
     *
     * @param m Map to add entries to.
     * @param entries Entries.
     * @param <K> Key type.
     * @param <V> Value type.
     * @param <M> Map type.
     * @return Passed in collection.
     */
    public static <K, V, M extends Map<K, V>> M addAll(M m, IgniteBiTuple<K, V>... entries) {
        for (IgniteBiTuple<K, V> t : entries)
            m.put(t.get1(), t.get2());

        return m;
    }

    /**
     * Create one-element tuple.
     *
     * @param v0 First element.
     * @param <V0> Type of first element.
     * @return Tuple.
     */
    public static <V0> T1<V0> tuple(V0 v0) {
        return new T1<>(v0);
    }

    /**
     * Create two-elements tuple.
     *
     * @param v0 First element.
     * @param v1 Second element.
     * @param <V0> Type of first element.
     * @param <V1> Type of second element.
     * @return Tuple.
     */
    public static <V0, V1> T2<V0, V1> tuple(V0 v0, V1 v1) {
        return new T2<>(v0, v1);
    }

    /**
     * Create three-elements tuple.
     *
     * @param v0 First element.
     * @param v1 Second element.
     * @param v2 Third element.
     * @param <V0> Type of first element.
     * @param <V1> Type of second element.
     * @param <V2> Type of third element.
     * @return Tuple.
     */
    public static <V0, V1, V2> T3<V0, V1, V2> tuple(V0 v0, V1 v1, V2 v2) {
        return new T3<>(v0, v1, v2);
    }

    /**
     * Create four-elements tuple.
     *
     * @param v0 First element.
     * @param v1 Second element.
     * @param v2 Third element.
     * @param v3 Fourth element.
     * @param <V0> Type of first element.
     * @param <V1> Type of second element.
     * @param <V2> Type of third element.
     * @param <V3> Type of fourth element.
     * @return Tuple.
     */
    public static <V0, V1, V2, V3> T4<V0, V1, V2, V3> tuple(V0 v0, V1 v1, V2 v2, V3 v3) {
        return new T4<>(v0, v1, v2, v3);
    }

    /**
     * Create five-elements tuple.
     *
     * @param v0 First element.
     * @param v1 Second element.
     * @param v2 Third element.
     * @param v3 Fourth element.
     * @param v4 Fifth element.
     * @param <V0> Type of first element.
     * @param <V1> Type of second element.
     * @param <V2> Type of third element.
     * @param <V3> Type of fourth element.
     * @param <V4> Type of fifth element.
     * @return Tuple.
     */
    public static <V0, V1, V2, V3, V4> T5<V0, V1, V2, V3, V4> tuple(V0 v0, V1 v1, V2 v2, V3 v3, V4 v4) {
        return new T5<>(v0, v1, v2, v3, v4);
    }

    /**
     * Create six-elements tuple.
     *
     * @param v0 First element.
     * @param v1 Second element.
     * @param v2 Third element.
     * @param v3 Fourth element.
     * @param v4 Fifth element.
     * @param v5 Sixth element.
     * @param <V0> Type of first element.
     * @param <V1> Type of second element.
     * @param <V2> Type of third element.
     * @param <V3> Type of fourth element.
     * @param <V4> Type of fifth element.
     * @param <V5> Type of sixth element.
     * @return Tuple.
     */
    public static <V0, V1, V2, V3, V4, V5> T6<V0, V1, V2, V3, V4, V5> tuple(V0 v0, V1 v1, V2 v2, V3 v3, V4 v4, V5 v5) {
        return new T6<>(v0, v1, v2, v3, v4, v5);
    }

    /**
     * Utility method creating {@link JMException} with given cause.
     *
     * @param e Cause exception.
     * @return Newly created {@link JMException}.
     */
    public static JMException jmException(Throwable e) {
        JMException x = new JMException();

        x.initCause(e);

        return x;
    }

    /**
     * Ignore {@link RuntimeException}.
     *
     * @param runnable Runnable.
     */
    public static void ignoreRuntimeException(Runnable runnable) {
        try {
            runnable.run();
        }
        catch (RuntimeException ignore) {
        }
    }

    /**
     * Unwraps closure exceptions.
     *
     * @param t Exception.
     * @return Unwrapped exception.
     */
    public static Exception unwrap(Throwable t) {
        assert t != null;

        while (true) {
            if (t instanceof Error)
                throw (Error)t;

            if (t instanceof GridClosureException) {
                t = ((GridClosureException)t).unwrap();

                continue;
            }

            return (Exception)t;
        }
    }

    /**
     * Casts the passed {@code Throwable t} to {@link IgniteCheckedException}.<br>
     * If {@code t} is a {@link GridClosureException}, it is unwrapped and then cast to {@link IgniteCheckedException}.
     * If {@code t} is an {@link IgniteCheckedException}, it is returned.
     * If {@code t} is not a {@link IgniteCheckedException}, a new {@link IgniteCheckedException} caused by {@code t}
     * is returned.
     *
     * @param t Throwable to cast.
     * @return {@code t} cast to {@link IgniteCheckedException}.
     */
    public static IgniteCheckedException cast(Throwable t) {
        assert t != null;

        t = unwrap(t);

        return t instanceof IgniteCheckedException
            ? (IgniteCheckedException)t
            : new IgniteCheckedException(t);
    }

    /**
     * Checks if class loader is an internal P2P class loader.
     *
     * @param o Object to check.
     * @return {@code True} if P2P class loader.
     */
    public static boolean p2pLoader(Object o) {
        return o != null && p2pLoader(o.getClass().getClassLoader());
    }

    /**
     * Checks if class loader is an internal P2P class loader.
     *
     * @param ldr Class loader to check.
     * @return {@code True} if P2P class loader.
     */
    public static boolean p2pLoader(ClassLoader ldr) {
        return ldr instanceof GridDeploymentInfo;
    }

    /**
     * Returns Deployment class loader id if method was invoked in the job context
     * (it may be the context of a cache's operation which was triggered by the distributed job)
     * or {@code null} if no context was found or Deployment is switched off.
     *
     * @param ctx Kernal context.
     * @return Deployment class loader id or {@code null}.
     */
    public static IgniteUuid contextDeploymentClassLoaderId(GridKernalContext ctx) {
        if (ctx == null || !ctx.deploy().enabled())
            return null;

        if (ctx.job() != null && ctx.job().currentDeployment() != null)
            return ctx.job().currentDeployment().classLoaderId();

        if (ctx.cache() != null && ctx.cache().context() != null)
            return ctx.cache().context().deploy().locLoaderId();

        return null;
    }

    /**
     * Gets that deployment class loader matching by the specific id, or {@code null}
     * if the class loader was not found.
     *
     * @param ctx Kernal context.
     * @param ldrId Class loader id.
     * @return Deployment class loader or {@code null}.
     */
    public static ClassLoader deploymentClassLoader(GridKernalContext ctx, IgniteUuid ldrId) {
        if (ldrId == null || !ctx.deploy().enabled())
            return null;

        GridDeployment dep = ctx.deploy().getDeployment(ldrId);

        return dep == null ? null : dep.classLoader();
    }

    /**
     * Restores a deployment context for cache deployment.
     *
     * @param ctx Kernal context.
     * @param ldrId Class loader id.
     */
    public static void restoreDeploymentContext(GridKernalContext ctx, IgniteUuid ldrId) {
        if (ctx.deploy().enabled() && ldrId != null) {
            GridDeployment dep = ctx.deploy().getDeployment(ldrId);

            if (dep != null) {
                try {
                    ctx.cache().context().deploy().p2pContext(
                        dep.classLoaderId().globalId(),
                        dep.classLoaderId(),
                        dep.userVersion(),
                        dep.deployMode(),
                        dep.participants()
                    );
                }
                catch (IgnitePeerToPeerClassLoadingException e) {
                    ctx.log(ctx.cache().context().deploy().getClass())
                        .error("Could not restore P2P context [ldrId=" + ldrId + ']', e);
                }
            }
        }
    }

    /**
     * @param ctx Kernal context.
     * @return Closure that converts node ID to a node.
     */
    public static IgniteClosure<UUID, ClusterNode> id2Node(final GridKernalContext ctx) {
        assert ctx != null;

        return new C1<UUID, ClusterNode>() {
            @Nullable @Override public ClusterNode apply(UUID id) {
                return ctx.discovery().node(id);
            }
        };
    }

    /**
     * Dumps stack for given thread.
     *
     * @param t Thread to dump stack for.
     *
     * @deprecated Calls to this method should never be committed to master.
     */
    @Deprecated
    public static void dumpStack(Thread t) {
        dumpStack(t, System.err);
    }

    /**
     * Dumps stack for given thread.
     *
     * @param t Thread to dump stack for.
     * @param s {@code PrintStream} to use for output.
     *
     * @deprecated Calls to this method should never be committed to master.
     */
    @SuppressWarnings({"SynchronizationOnLocalVariableOrMethodParameter"})
    @Deprecated
    public static void dumpStack(Thread t, PrintStream s) {
        synchronized (s) {
            s.println("Dumping stack trace for thread: " + t);

            for (StackTraceElement trace : t.getStackTrace())
                s.println("\tat " + trace);
        }
    }

    /**
     * Checks if object is a primitive array.
     *
     * @param obj Object to check.
     * @return {@code True} if Object is primitive array.
     */
    public static boolean isPrimitiveArray(Object obj) {
        Class<?> cls = obj.getClass();

        return cls.isArray() && cls.getComponentType().isPrimitive();
    }

    /**
     * @param cls Class.
     * @return {@code True} if given class represents a primitive or a primitive wrapper class.
     *
     */
    public static boolean isPrimitiveOrWrapper(Class<?> cls) {
        return cls.isPrimitive() ||
            Boolean.class.equals(cls) ||
            Byte.class.equals(cls) ||
            Character.class.equals(cls) ||
            Short.class.equals(cls) ||
            Integer.class.equals(cls) ||
            Long.class.equals(cls) ||
            Float.class.equals(cls) ||
            Double.class.equals(cls) ||
            Void.class.equals(cls);
    }

    /**
     * Awaits for condition.
     *
     * @param cond Condition to await for.
     * @throws IgniteInterruptedCheckedException Wrapped {@link InterruptedException}
     */
    public static void await(Condition cond) throws IgniteInterruptedCheckedException {
        try {
            cond.await();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IgniteInterruptedCheckedException(e);
        }
    }

    /**
     * Awaits for condition ignoring interrupts.
     *
     * @param cond Condition to await for.
     */
    public static void awaitQuiet(Condition cond) {
        cond.awaitUninterruptibly();
    }

    /**
     * Awaits for condition.
     *
     * @param cond Condition to await for.
     * @param time The maximum time to wait,
     * @param unit The unit of the {@code time} argument.
     * @return {@code false} if the waiting time detectably elapsed before return from the method, else {@code true}
     * @throws IgniteInterruptedCheckedException Wrapped {@link InterruptedException}
     */
    public static boolean await(Condition cond, long time, TimeUnit unit) throws IgniteInterruptedCheckedException {
        try {
            return cond.await(time, unit);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IgniteInterruptedCheckedException(e);
        }
    }

    /**
     * Awaits for the latch.
     *
     * @param latch Latch to wait for.
     * @throws IgniteInterruptedCheckedException Wrapped {@link InterruptedException}.
     */
    public static void await(CountDownLatch latch) throws IgniteInterruptedCheckedException {
        try {
            if (latch.getCount() > 0)
                latch.await();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IgniteInterruptedCheckedException(e);
        }
    }

    /**
     * Awaits for the latch.
     *
     * @param latch Latch to wait for.
     * @param timeout Maximum time to wait.
     * @param unit Time unit for timeout.
     * @return {@code True} if the count reached zero and {@code false}
     *      if the waiting time elapsed before the count reached zero.
     * @throws IgniteInterruptedCheckedException Wrapped {@link InterruptedException}.
     */
    public static boolean await(CountDownLatch latch, long timeout, TimeUnit unit)
        throws IgniteInterruptedCheckedException {
        try {
            return latch.await(timeout, unit);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IgniteInterruptedCheckedException(e);
        }
    }

    /**
     * Awaits for the latch until it is counted down,
     * ignoring interruptions.
     * <p>
     * If calling thread was interrupted, interrupted status will be
     * recovered prior to return.
     *
     * @param latch Latch to wait for.
     */
    public static void awaitQuiet(CountDownLatch latch) {
        boolean interrupted = false;

        while (true) {
            try {
                latch.await();

                break;
            }
            catch (InterruptedException ignored) {
                interrupted = true;
            }
        }

        if (interrupted)
            Thread.currentThread().interrupt();
    }

    /**
     * Awaits for the barrier ignoring interruptions.
     * <p>
     * If calling thread was interrupted, interrupted status will be recovered prior to return. If the barrier is
     * already broken, return immediately without throwing any exceptions.
     *
     * @param barrier Barrier to wait for.
     */
    public static void awaitQuiet(CyclicBarrier barrier) {
        boolean interrupted = false;

        while (true) {
            try {
                barrier.await();

                break;
            }
            catch (InterruptedException ignored) {
                interrupted = true;
            }
            catch (BrokenBarrierException ignored) {
                break;
            }
        }

        if (interrupted)
            Thread.currentThread().interrupt();
    }

    /**
     * Returns URLs of class loader
     *
     * @param clsLdr Class loader.
     */
    public static URL[] classLoaderUrls(ClassLoader clsLdr) {
        if (clsLdr == null)
            return EMPTY_URL_ARR;
        else if (clsLdr instanceof URLClassLoader)
            return ((URLClassLoader)clsLdr).getURLs();
        else if (bltClsLdrCls != null && urlClsLdrField != null && bltClsLdrCls.isAssignableFrom(clsLdr.getClass())) {
            try {
                synchronized (urlClsLdrField) {
                    // Backup accessible field state.
                    boolean accessible = urlClsLdrField.isAccessible();

                    try {
                        if (!accessible)
                            urlClsLdrField.setAccessible(true);

                        Object ucp = urlClsLdrField.get(clsLdr);

                        if (ucp instanceof URLClassLoader)
                            return ((URLClassLoader)ucp).getURLs();
                        else if (clsURLClassPath != null && clsURLClassPath.isInstance(ucp))
                            return (URL[])mthdURLClassPathGetUrls.invoke(ucp);
                        else
                            throw new RuntimeException("Unknown classloader: " + clsLdr.getClass());
                    }
                    finally {
                        // Recover accessible field state.
                        if (!accessible)
                            urlClsLdrField.setAccessible(false);
                    }
                }
            }
            catch (InvocationTargetException | IllegalAccessException e) {
                e.printStackTrace(System.err);

                return EMPTY_URL_ARR;
            }
        }
        else
            return EMPTY_URL_ARR;
    }

    /** */
    @Nullable private static Class defaultClassLoaderClass() {
        try {
            return Class.forName("jdk.internal.loader.BuiltinClassLoader");
        }
        catch (ClassNotFoundException e) {
            return null;
        }
    }

    /** */
    @Nullable private static Field urlClassLoaderField() {
        try {
            Class cls = defaultClassLoaderClass();

            return cls == null ? null : cls.getDeclaredField("ucp");
        }
        catch (NoSuchFieldException e) {
            return null;
        }
    }

    /**
     * Sleeps for given number of milliseconds.
     *
     * @param ms Time to sleep.
     * @throws IgniteInterruptedCheckedException Wrapped {@link InterruptedException}.
     */
    public static void sleep(long ms) throws IgniteInterruptedCheckedException {
        try {
            Thread.sleep(ms);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IgniteInterruptedCheckedException(e);
        }
    }

    /**
     * Joins worker.
     *
     * @param w Worker.
     * @throws IgniteInterruptedCheckedException Wrapped {@link InterruptedException}.
     */
    public static void join(GridWorker w) throws IgniteInterruptedCheckedException {
        try {
            if (w != null)
                w.join();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IgniteInterruptedCheckedException(e);
        }
    }

    /**
     * Gets result from the given future with right exception handling.
     *
     * @param fut Future.
     * @return Future result.
     * @throws IgniteCheckedException If failed.
     */
    public static <T> T get(Future<T> fut) throws IgniteCheckedException {
        try {
            return fut.get();
        }
        catch (ExecutionException e) {
            throw new IgniteCheckedException(e.getCause());
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IgniteInterruptedCheckedException(e);
        }
        catch (CancellationException e) {
            throw new IgniteCheckedException(e);
        }
    }

    /**
     * Joins thread.
     *
     * @param t Thread.
     * @throws org.apache.ignite.internal.IgniteInterruptedCheckedException Wrapped {@link InterruptedException}.
     */
    public static void join(@Nullable Thread t) throws IgniteInterruptedCheckedException {
        if (t == null)
            return;

        try {
            t.join();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IgniteInterruptedCheckedException(e);
        }
    }

    /**
     * Acquires a permit from provided semaphore.
     *
     * @param sem Semaphore.
     * @throws org.apache.ignite.internal.IgniteInterruptedCheckedException Wrapped {@link InterruptedException}.
     */
    public static void acquire(Semaphore sem) throws IgniteInterruptedCheckedException {
        try {
            sem.acquire();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IgniteInterruptedCheckedException(e);
        }
    }

    /**
     * Tries to acquire a permit from provided semaphore during {@code timeout}.
     *
     * @param sem Semaphore.
     * @param timeout The maximum time to wait.
     * @param unit The unit of the {@code time} argument.
     * @throws org.apache.ignite.internal.IgniteInterruptedCheckedException Wrapped {@link InterruptedException}.
     * @return {@code True} if acquires a permit, {@code false} another.
     */
    public static boolean tryAcquire(Semaphore sem, long timeout, TimeUnit unit)
        throws IgniteInterruptedCheckedException {
        try {
            return sem.tryAcquire(timeout, unit);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IgniteInterruptedCheckedException(e);
        }
    }

    /**
     * Gets cache attributes for the node.
     *
     * @param n Node to get cache attributes for.
     * @return Array of cache attributes for the node.
     */
    public static GridCacheAttributes[] cacheAttributes(ClusterNode n) {
        return n.attribute(ATTR_CACHE);
    }

    /**
     * Checks if given node has near cache enabled for the specified
     * partitioned cache.
     *
     * @param n Node.
     * @param cacheName Cache name.
     * @return {@code true} if given node has near cache enabled for the
     *      specified partitioned cache.
     */
    public static boolean hasNearCache(ClusterNode n, String cacheName) {
        GridCacheAttributes[] caches = n.attribute(ATTR_CACHE);

        if (caches != null)
            for (GridCacheAttributes attrs : caches)
                if (F.eq(cacheName, attrs.cacheName()))
                    return attrs.nearCacheEnabled();

        return false;
    }

    /**
     * Adds listener to asynchronously log errors.
     *
     * @param f Future to listen to.
     * @param log Logger.
     */
    public static void asyncLogError(IgniteInternalFuture<?> f, final IgniteLogger log) {
        if (f != null)
            f.listen(new CI1<IgniteInternalFuture<?>>() {
                @Override public void apply(IgniteInternalFuture<?> f) {
                    try {
                        f.get();
                    }
                    catch (IgniteCheckedException e) {
                        U.error(log, "Failed to execute future: " + f, e);
                    }
                }
            });
    }

    /**
     * Converts collection of nodes to collection of node IDs.
     *
     * @param nodes Nodes.
     * @return Node IDs.
     */
    public static Collection<UUID> nodeIds(@Nullable Collection<? extends ClusterNode> nodes) {
        return F.viewReadOnly(nodes, F.node2id());
    }

    /**
     * Converts collection of Grid instances to collection of node IDs.
     *
     * @param grids Grids.
     * @return Node IDs.
     */
    public static Collection<UUID> gridIds(@Nullable Collection<? extends Ignite> grids) {
        return F.viewReadOnly(grids, new C1<Ignite, UUID>() {
            @Override public UUID apply(Ignite g) {
                return g.cluster().localNode().id();
            }
        });
    }

    /**
     * Converts collection of Grid instances to collection of grid names.
     *
     * @param grids Grids.
     * @return Grid names.
     */
    public static Collection<String> grids2names(@Nullable Collection<? extends Ignite> grids) {
        return F.viewReadOnly(grids, new C1<Ignite, String>() {
            @Override public String apply(Ignite g) {
                return g.name();
            }
        });
    }

    /**
     * Converts collection of grid nodes to collection of grid names.
     *
     * @param nodes Nodes.
     * @return Grid names.
     */
    public static Collection<String> nodes2names(@Nullable Collection<? extends ClusterNode> nodes) {
        return F.viewReadOnly(nodes, new C1<ClusterNode, String>() {
            @Override public String apply(ClusterNode n) {
                return G.ignite(n.id()).name();
            }
        });
    }

    /**
     * Adds cause to the end of cause chain.
     *
     * @param e Error to add cause to.
     * @param cause Cause to add.
     * @param log Logger to log failure when cause can not be added.
     * @return {@code True} if cause was added.
     */
    public static boolean addLastCause(@Nullable Throwable e, @Nullable Throwable cause, IgniteLogger log) {
        if (e == null || cause == null)
            return false;

        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t == cause)
                return false;

            if (t.getCause() == null || t.getCause() == t) {
                try {
                    t.initCause(cause);
                }
                catch (IllegalStateException ignored) {
                    error(log, "Failed to add cause to the end of cause chain (cause is printed here but will " +
                        "not be propagated to callee): " + e,
                        "Failed to add cause to the end of cause chain: " + e, cause);
                }

                return true;
            }
        }

        return false;
    }

    /**
     * @return {@code line.separator} system property.
     */
    public static String nl() {
        return NL;
    }

    /**
     * Initializes logger into/from log reference passed in.
     *
     * @param ctx Context.
     * @param logRef Log reference.
     * @param obj Object to get logger for.
     * @return Logger for the object.
     */
    public static IgniteLogger logger(GridKernalContext ctx, AtomicReference<IgniteLogger> logRef, Object obj) {
        IgniteLogger log = logRef.get();

        if (log == null) {
            logRef.compareAndSet(null, ctx.log(obj.getClass()));

            log = logRef.get();
        }

        return log;
    }

    /**
     * Initializes logger into/from log reference passed in.
     *
     * @param ctx Context.
     * @param logRef Log reference.
     * @param cls Class to get logger for.
     * @return Logger for the object.
     */
    public static IgniteLogger logger(GridKernalContext ctx, AtomicReference<IgniteLogger> logRef, Class<?> cls) {
        IgniteLogger log = logRef.get();

        if (log == null) {
            logRef.compareAndSet(null, ctx.log(cls));

            log = logRef.get();
        }

        return log;
    }

    /**
     * @param hash Hash code of the object to put.
     * @param concurLvl Concurrency level.
     * @return Segment index.
     */
    public static int concurrentMapSegment(int hash, int concurLvl) {
        hash += (hash << 15) ^ 0xffffcd7d;
        hash ^= (hash >>> 10);
        hash += (hash << 3);
        hash ^= (hash >>> 6);
        hash += (hash << 2) + (hash << 14);

        int shift = 0;
        int size = 1;

        while (size < concurLvl) {
            ++shift;
            size <<= 1;
        }

        int segmentShift = 32 - shift;
        int segmentMask = size - 1;

        return (hash >>> segmentShift) & segmentMask;
    }

    /**
     * @param map Map.
     */
    public static <K, V> void printConcurrentHashMapInfo(ConcurrentHashMap<K, V> map) {
        assert map != null;

        Object[] segs = field(map, "segments");

        X.println("Concurrent map stats [identityHash= " + System.identityHashCode(map) +
            ", segsCnt=" + segs.length + ']');

        int emptySegsCnt = 0;

        int totalCollisions = 0;

        for (int i = 0; i < segs.length; i++) {
            int segCnt = IgniteUtils.<Integer>field(segs[i], "count");

            if (segCnt == 0) {
                emptySegsCnt++;

                continue;
            }

            Object[] tab = field(segs[i], "table");

            int tabLen = tab.length;

            X.println("    Segment-" + i + " [count=" + segCnt + ", len=" + tabLen + ']');

            // Group buckets by entries count.
            Map<Integer, Integer> bucketsStats = new TreeMap<>();

            for (Object entry : tab) {
                int cnt = 0;

                while (entry != null) {
                    cnt++;

                    entry = field(entry, "next");
                }

                Integer bucketCnt = bucketsStats.get(cnt);

                if (bucketCnt == null)
                    bucketCnt = 0;

                bucketCnt++;

                bucketsStats.put(cnt, bucketCnt);

                if (cnt > 1)
                    totalCollisions += (cnt - 1);
            }

            for (Map.Entry<Integer, Integer> e : bucketsStats.entrySet())
                X.println("        Buckets with count " + e.getKey() + ": " + e.getValue());
        }

        X.println("    Map summary [emptySegs=" + emptySegsCnt + ", collisions=" + totalCollisions + ']');
    }

    /**
     * Compares two byte arrays by bytes.
     * @param bytes1 Bytes 1.
     * @param bytes2 Bytes 2.
     */
    public static int compareByteArrays(byte[] bytes1, byte[] bytes2) {
        final int len = bytes1.length;
        int lenCmp = Integer.compare(len, bytes2.length);

        if (lenCmp != 0)
            return lenCmp;

        final int words = len / 8;
        int cmp;

        for (int i = 0; i < words; i++) {
            int off = i * 8;
            long b1 = GridUnsafe.getLong(bytes1, GridUnsafe.BYTE_ARR_OFF + off);
            long b2 = GridUnsafe.getLong(bytes2, GridUnsafe.BYTE_ARR_OFF + off);
            cmp = Long.compare(b1, b2);
            if (cmp != 0)
                return cmp;
        }

        for (int i = words * 8; i < len; i++) {
            byte b1 = bytes1[i];
            byte b2 = bytes2[i];
            if (b1 != b2)
                return b1 > b2 ? 1 : -1;
        }

        return 0;
    }

    /**
     * Gets field value.
     *
     * @param obj Object.
     * @param fieldName Field name.
     * @return Field value.
     */
    public static <T> T field(Object obj, String fieldName) {
        assert obj != null;
        assert fieldName != null;

        try {
            for (Class cls = obj.getClass(); cls != Object.class; cls = cls.getSuperclass()) {
                for (Field field : cls.getDeclaredFields()) {
                    if (field.getName().equals(fieldName)) {
                        field.setAccessible(true);

                        return (T)field.get(obj);
                    }
                }
            }
        }
        catch (Exception e) {
            throw new IgniteException("Failed to get field value [fieldName=" + fieldName + ", obj=" + obj + ']', e);
        }

        throw new IgniteException("Failed to get field value [fieldName=" + fieldName + ", obj=" + obj + ']');
    }

    /**
     * Check that field exist.
     *
     * @param obj Object.
     * @param fieldName Field name.
     * @return Boolean flag.
     */
    public static boolean hasField(Object obj, String fieldName) {
        try {
            field(obj, fieldName);

            return true;
        }
        catch (IgniteException e) {
            return false;
        }
    }

    /**
     * Gets object field offset.
     *
     * @param cls Object class.
     * @param fieldName Field name.
     * @return Field offset.
     */
    public static long fieldOffset(Class<?> cls, String fieldName) {
        try {
            return objectFieldOffset(cls.getDeclaredField(fieldName));
        }
        catch (NoSuchFieldException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * @param cls Class to check.
     * @return {@code True} if class is final.
     */
    public static boolean isFinal(Class<?> cls) {
        return Modifier.isFinal(cls.getModifiers());
    }

    /**
     * Gets field value.
     *
     * @param cls Class.
     * @param fieldName Field name.
     * @return Field value.
     * @throws IgniteCheckedException If static field with given name cannot be retreived.
     */
    public static <T> T field(Class<?> cls, String fieldName) throws IgniteCheckedException {
        assert cls != null;
        assert fieldName != null;

        try {
            for (Class c = cls; cls != Object.class; cls = cls.getSuperclass()) {
                for (Field field : c.getDeclaredFields()) {
                    if (field.getName().equals(fieldName)) {
                        if (!Modifier.isStatic(field.getModifiers()))
                            throw new IgniteCheckedException("Failed to get class field (field is not static) [cls=" +
                                cls + ", fieldName=" + fieldName + ']');

                        boolean accessible = field.isAccessible();

                        T val;

                        try {
                            field.setAccessible(true);

                            val = (T)field.get(null);
                        }
                        finally {
                            if (!accessible)
                                field.setAccessible(false);
                        }

                        return val;
                    }
                }
            }
        }
        catch (Exception e) {
            throw new IgniteCheckedException("Failed to get field value [fieldName=" + fieldName + ", cls=" + cls + ']',
                e);
        }

        throw new IgniteCheckedException("Failed to get field value (field was not found) [fieldName=" + fieldName +
            ", cls=" + cls + ']');
    }

    /**
     * Invokes method.
     *
     * @param cls Object.
     * @param obj Object.
     * @param mtdName Field name.
     * @param params Parameters.
     * @return Field value.
     * @throws IgniteCheckedException If static field with given name cannot be retreived.
     */
    public static <T> T invoke(@Nullable Class<?> cls, @Nullable Object obj, String mtdName,
        Object... params) throws IgniteCheckedException {
        assert cls != null || obj != null;
        assert mtdName != null;

        try {
            for (cls = cls != null ? cls : obj.getClass(); cls != Object.class; cls = cls.getSuperclass()) {
                Method mtd = null;

                for (Method declaredMtd : cls.getDeclaredMethods()) {
                    if (declaredMtd.getName().equals(mtdName)) {
                        if (mtd == null)
                            mtd = declaredMtd;
                        else
                            throw new IgniteCheckedException("Failed to invoke (ambigous method name) [mtdName=" +
                                mtdName + ", cls=" + cls + ']');
                    }
                }

                if (mtd == null)
                    continue;

                boolean accessible = mtd.isAccessible();

                T res;

                try {
                    mtd.setAccessible(true);

                    res = (T)mtd.invoke(obj, params);
                }
                finally {
                    if (!accessible)
                        mtd.setAccessible(false);
                }

                return res;
            }
        }
        catch (Exception e) {
            throw new IgniteCheckedException("Failed to invoke [mtdName=" + mtdName + ", cls=" + cls + ']',
                e);
        }

        throw new IgniteCheckedException("Failed to invoke (method was not found) [mtdName=" + mtdName +
            ", cls=" + cls + ']');
    }

    /**
     * Invokes method.
     *
     * @param cls Object.
     * @param obj Object.
     * @param mtdName Field name.
     * @param paramTypes Parameter types.
     * @param params Parameters.
     * @return Field value.
     * @throws IgniteCheckedException If static field with given name cannot be retrieved.
     */
    public static <T> T invoke(@Nullable Class<?> cls, @Nullable Object obj, String mtdName,
        Class[] paramTypes, Object... params) throws IgniteCheckedException {
        assert cls != null || obj != null;
        assert mtdName != null;

        try {
            for (cls = cls != null ? cls : obj.getClass(); cls != Object.class; cls = cls.getSuperclass()) {
                Method mtd;

                try {
                    mtd = cls.getDeclaredMethod(mtdName, paramTypes);
                }
                catch (NoSuchMethodException ignored) {
                    continue;
                }

                boolean accessible = mtd.isAccessible();

                T res;

                try {
                    mtd.setAccessible(true);

                    res = (T)mtd.invoke(obj, params);
                }
                finally {
                    if (!accessible)
                        mtd.setAccessible(false);
                }

                return res;
            }
        }
        catch (Exception e) {
            throw new IgniteCheckedException("Failed to invoke [mtdName=" + mtdName + ", cls=" + cls + ']',
                e);
        }

        throw new IgniteCheckedException("Failed to invoke (method was not found) [mtdName=" + mtdName +
            ", cls=" + cls + ']');
    }

    /**
     * Gets property value.
     *
     * @param obj Object.
     * @param propName Property name.
     * @return Field value.
     */
    public static <T> T property(Object obj, String propName) {
        assert obj != null;
        assert propName != null;

        try {
            Method m;

            try {
                m = obj.getClass().getMethod("get" + capitalFirst(propName));
            }
            catch (NoSuchMethodException ignored) {
                m = obj.getClass().getMethod("is" + capitalFirst(propName));
            }

            assert F.isEmpty(m.getParameterTypes());

            boolean accessible = m.isAccessible();

            try {
                m.setAccessible(true);

                return (T)m.invoke(obj);
            }
            finally {
                m.setAccessible(accessible);
            }
        }
        catch (Exception e) {
            throw new IgniteException(
                "Failed to get property value [property=" + propName + ", obj=" + obj + ']', e);
        }
    }

    /**
     * Gets static field value.
     *
     * @param cls Class.
     * @param fieldName Field name.
     * @return Field value.
     * @throws IgniteCheckedException If failed.
     */
    public static <T> T staticField(Class<?> cls, String fieldName) throws IgniteCheckedException {
        assert cls != null;
        assert fieldName != null;

        try {
            for (Field field : cls.getDeclaredFields())
                if (field.getName().equals(fieldName)) {
                    boolean accessible = field.isAccessible();

                    if (!accessible)
                        field.setAccessible(true);

                    T val = (T)field.get(null);

                    if (!accessible)
                        field.setAccessible(false);

                    return val;
                }
        }
        catch (Exception e) {
            throw new IgniteCheckedException("Failed to get field value [fieldName=" + fieldName + ", cls=" + cls + ']', e);
        }

        throw new IgniteCheckedException("Failed to get field value [fieldName=" + fieldName + ", cls=" + cls + ']');
    }

    /**
     * Capitalizes the first character of the given string.
     *
     * @param str String.
     * @return String with capitalized first character.
     */
    private static String capitalFirst(@Nullable String str) {
        return str == null ? null :
            str.isEmpty() ? "" : Character.toUpperCase(str.charAt(0)) + str.substring(1);
    }

    /**
     * Checks whether property is one added by Visor when node is started via remote SSH session.
     *
     * @param name Property name to check.
     * @return {@code True} if property is Visor node startup property, {@code false} otherwise.
     */
    public static boolean isVisorNodeStartProperty(String name) {
        return IGNITE_SSH_HOST.equals(name) || IGNITE_SSH_USER_NAME.equals(name);
    }

    /**
     * Checks whether property is one required by Visor to work correctly.
     *
     * @param name Property name to check.
     * @return {@code True} if property is required by Visor, {@code false} otherwise.
     */
    public static boolean isVisorRequiredProperty(String name) {
        return "java.version".equals(name) || "java.vm.name".equals(name) || "os.arch".equals(name) ||
            "os.name".equals(name) || "os.version".equals(name);
    }

    /**
     * Adds no-op logger to remove no-appender warning.
     *
     * @return Tuple with root log and no-op appender instances. No-op appender can be {@code null}
     *      if it did not found in classpath. Notice that in this case logging is not suppressed.
     * @throws IgniteCheckedException In case of failure to add no-op logger for Log4j.
     */
    public static IgniteBiTuple<Object, Object> addLog4jNoOpLogger() throws IgniteCheckedException {
        Object rootLog;
        Object nullApp;

        try {
            // Add no-op logger to remove no-appender warning.
            Class<?> logCls = Class.forName("org.apache.log4j.Logger");

            rootLog = logCls.getMethod("getRootLogger").invoke(logCls);

            try {
                nullApp = Class.forName("org.apache.log4j.varia.NullAppender").newInstance();
            }
            catch (ClassNotFoundException ignore) {
                // Can't found log4j no-op appender in classpath (for example, log4j was added through
                // log4j-over-slf4j library. No-appender warning will not be suppressed.
                return new IgniteBiTuple<>(rootLog, null);
            }

            Class appCls = Class.forName("org.apache.log4j.Appender");

            rootLog.getClass().getMethod("addAppender", appCls).invoke(rootLog, nullApp);
        }
        catch (Exception e) {
            throw new IgniteCheckedException("Failed to add no-op logger for Log4j.", e);
        }

        return new IgniteBiTuple<>(rootLog, nullApp);
    }

    /**
     * Removes previously added no-op logger via method {@link #addLog4jNoOpLogger}.
     *
     * @param t Tuple with root log and null appender instances.
     * @throws IgniteCheckedException In case of failure to remove previously added no-op logger for Log4j.
     */
    public static void removeLog4jNoOpLogger(IgniteBiTuple<Object, Object> t) throws IgniteCheckedException {
        Object rootLog = t.get1();
        Object nullApp = t.get2();

        if (nullApp == null)
            return;

        try {
            Class appenderCls = Class.forName("org.apache.log4j.Appender");

            rootLog.getClass().getMethod("removeAppender", appenderCls).invoke(rootLog, nullApp);
        }
        catch (Exception e) {
            throw new IgniteCheckedException("Failed to remove previously added no-op logger for Log4j.", e);
        }
    }

    /**
     * Adds no-op console handler for root java logger.
     *
     * @return Removed handlers.
     */
    public static Collection<Handler> addJavaNoOpLogger() {
        Collection<Handler> savedHnds = new ArrayList<>();

        Logger log = Logger.getLogger("");

        for (Handler h : log.getHandlers()) {
            log.removeHandler(h);

            savedHnds.add(h);
        }

        ConsoleHandler hnd = new ConsoleHandler();

        hnd.setLevel(Level.OFF);

        log.addHandler(hnd);

        return savedHnds;
    }

    /**
     * Removes previously added no-op handler for root java logger.
     *
     * @param rmvHnds Previously removed handlers.
     */
    public static void removeJavaNoOpLogger(Collection<Handler> rmvHnds) {
        Logger log = Logger.getLogger("");

        for (Handler h : log.getHandlers())
            log.removeHandler(h);

        if (!F.isEmpty(rmvHnds)) {
            for (Handler h : rmvHnds)
                log.addHandler(h);
        }
    }

    /**
     * Attaches node ID to log file name.
     *
     * @param nodeId Node ID.
     * @param fileName File name.
     * @return File name with node ID.
     */
    @SuppressWarnings("IfMayBeConditional")
    public static String nodeIdLogFileName(UUID nodeId, String fileName) {
        assert nodeId != null;
        assert fileName != null;

        fileName = GridFilenameUtils.separatorsToSystem(fileName);

        int dot = fileName.lastIndexOf('.');

        if (dot < 0 || dot == fileName.length() - 1)
            return fileName + '-' + U.id8(nodeId);
        else
            return fileName.substring(0, dot) + '-' + U.id8(nodeId) + fileName.substring(dot);
    }

    /**
     * Substitutes log directory with a custom one.
     *
     * @param dir Directory.
     * @param fileName Original path.
     * @return New path.
     */
    public static String customDirectoryLogFileName(@Nullable String dir, String fileName) {
        assert fileName != null;

        if (dir == null)
            return fileName;

        int sep = fileName.lastIndexOf(File.separator);

        return dir + (sep < 0 ? File.separator + fileName : fileName.substring(sep));
    }

    /**
     * Creates string for log output.
     *
     * @param msg Message to start string.
     * @param args Even length array where the odd elements are parameter names
     *      and even elements are parameter values.
     * @return Log message, formatted as recommended by Ignite guidelines.
     */
    public static String fl(String msg, Object... args) {
        assert args.length % 2 == 0;

        StringBuilder sb = new StringBuilder(msg);

        if (args.length > 0) {
            sb.append(" [");

            for (int i = 0; i < args.length / 2; i++) {
                sb.append(args[i * 2]).append('=').append(args[i * 2 + 1]);
                sb.append(", ");
            }

            sb.delete(sb.length() - 2, sb.length());
            sb.append(']');
        }

        return sb.toString();
    }

    /**
     * Round up the argument to the next highest power of 2;
     *
     * @param v Value to round up.
     * @return Next closest power of 2.
     */
    public static int nextPowerOf2(int v) {
        A.ensure(v >= 0, "v must not be negative");

        if (v == 0)
            return 1;

        return 1 << (32 - Integer.numberOfLeadingZeros(v - 1));
    }

    /**
     * Gets absolute value for integer. If integer is {@link Integer#MIN_VALUE}, then {@code 0} is returned.
     *
     * @param i Integer.
     * @return Absolute value.
     */
    public static int safeAbs(int i) {
        i = Math.abs(i);

        return i < 0 ? 0 : i;
    }

    /**
     * Gets absolute value for long. If argument is {@link Long#MIN_VALUE}, then {@code 0} is returned.
     *
     * @param i Argument.
     * @return Absolute value.
     */
    public static long safeAbs(long i) {
        i = Math.abs(i);

        return i < 0 ? 0 : i;
    }

    /**
     * When {@code long} value given is positive returns that value, otherwise returns provided default value.
     *
     * @param i Input value.
     * @param dflt Default value.
     * @return {@code i} if {@code i > 0} and {@code dflt} otherwise.
     */
    public static long ensurePositive(long i, long dflt) {
        return i <= 0 ? dflt : i;
    }

    /**
     * Gets wrapper class for a primitive type.
     *
     * @param cls Class. If {@code null}, method is no-op.
     * @return Wrapper class or original class if it is non-primitive.
     */
    @Nullable public static Class<?> box(@Nullable Class<?> cls) {
        if (cls == null)
            return null;

        if (!cls.isPrimitive())
            return cls;

        return boxedClsMap.get(cls);
    }

    /**
     * Gets class for provided name. Accepts primitive types names.
     *
     * @param clsName Class name.
     * @param ldr Class loader.
     * @return Class.
     * @throws ClassNotFoundException If class not found.
     */
    public static Class<?> forName(String clsName, @Nullable ClassLoader ldr) throws ClassNotFoundException {
        return U.forName(clsName, ldr, null, GridBinaryMarshaller.USE_CACHE.get());
    }

    /**
     * Gets class for provided name. Accepts primitive types names.
     *
     * @param clsName Class name.
     * @param ldr Class loader.
     * @return Class.
     * @throws ClassNotFoundException If class not found.
     */
    public static Class<?> forName(
        String clsName,
        @Nullable ClassLoader ldr,
        IgnitePredicate<String> clsFilter
    ) throws ClassNotFoundException {
        return forName(clsName, ldr, clsFilter, GridBinaryMarshaller.USE_CACHE.get());
    }

    /**
     * Gets class for provided name. Accepts primitive types names.
     *
     * @param clsName Class name.
     * @param ldr Class loader.
     * @param useCache If true class loader and result should be cached internally, false otherwise.
     * @return Class.
     * @throws ClassNotFoundException If class not found.
     */
    public static Class<?> forName(
        String clsName,
        @Nullable ClassLoader ldr,
        IgnitePredicate<String> clsFilter,
        boolean useCache
    ) throws ClassNotFoundException {
        assert clsName != null;

        Class<?> cls = primitiveMap.get(clsName);

        if (cls != null)
            return cls;

        if (ldr != null) {
            if (ldr instanceof ClassCache)
                return ((ClassCache)ldr).getFromCache(clsName);
            else if (!useCache) {
                cls = Class.forName(clsName, true, ldr);

                return cls;
            }
        }
        else
            ldr = gridClassLoader;

        if (!useCache) {
            cls = Class.forName(clsName, true, ldr);

            return cls;
        }

        ConcurrentMap<String, Class> ldrMap = classCache.get(ldr);

        if (ldrMap == null) {
            ConcurrentMap<String, Class> old = classCache.putIfAbsent(ldr, ldrMap = new ConcurrentHashMap<>());

            if (old != null)
                ldrMap = old;
        }

        cls = ldrMap.get(clsName);

        if (cls == null) {
            if (clsFilter != null && !clsFilter.apply(clsName))
                throw new ClassNotFoundException("Deserialization of class " + clsName + " is disallowed.");

            // Avoid class caching inside Class.forName
            if (ldr instanceof CacheClassLoaderMarker)
                cls = ldr.loadClass(clsName);
            else
                cls = Class.forName(clsName, true, ldr);

            Class old = ldrMap.putIfAbsent(clsName, cls);

            if (old != null)
                cls = old;
        }

        return cls;
    }

    /**
     * Clears class associated with provided class loader from class cache.
     *
     * @param ldr Class loader.
     * @param clsName Class name of clearing class.
     */
    public static void clearClassFromClassCache(ClassLoader ldr, String clsName) {
        ConcurrentMap<String, Class> map = classCache.get(ldr);

        if (map != null)
            map.remove(clsName);
    }

    /**
     * Clears class cache for provided loader.
     *
     * @param ldr Class loader.
     */
    public static void clearClassCache(ClassLoader ldr) {
        classCache.remove(ldr);
    }

    /**
     * Completely clears class cache.
     */
    public static void clearClassCache() {
        classCache.clear();
    }

    /**
     * Applies a supplemental hash function to a given hashCode, which
     * defends against poor quality hash functions.  This is critical
     * because ConcurrentHashMap uses power-of-two length hash tables,
     * that otherwise encounter collisions for hashCodes that do not
     * differ in lower or upper bits.
     * <p>
     * This function has been taken from Java 8 ConcurrentHashMap with
     * slightly modifications.
     *
     * @param h Value to hash.
     * @return Hash value.
     */
    public static int hash(int h) {
        // Spread bits to regularize both segment and index locations,
        // using variant of single-word Wang/Jenkins hash.
        h += (h << 15) ^ 0xffffcd7d;
        h ^= (h >>> 10);
        h += (h << 3);
        h ^= (h >>> 6);
        h += (h << 2) + (h << 14);

        return h ^ (h >>> 16);
    }

    /**
     * Applies a supplemental hash function to a given hashCode, which
     * defends against poor quality hash functions.  This is critical
     * because ConcurrentHashMap uses power-of-two length hash tables,
     * that otherwise encounter collisions for hashCodes that do not
     * differ in lower or upper bits.
     * <p>
     * This function has been taken from Java 8 ConcurrentHashMap with
     * slightly modifications.
     *
     * @param key Value to hash.
     * @return Hash value.
     */
    public static int hash(Object key) {
        return hash(key.hashCode());
    }

    /**
     * A primitive override of {@link #hash(Object)} to avoid unnecessary boxing.
     *
     * @param key Value to hash.
     * @return Hash value.
     */
    public static int hash(long key) {
        int val = (int)(key ^ (key >>> 32));

        return hash(val);
    }

    /**
     * @return PID of the current JVM or {@code -1} if it can't be determined.
     */
    public static int jvmPid() {
        // Should be something like this: 1160@mbp.local
        String name = ManagementFactory.getRuntimeMXBean().getName();

        try {
            int idx = name.indexOf('@');

            return idx > 0 ? Integer.parseInt(name.substring(0, idx)) : -1;
        }
        catch (NumberFormatException ignored) {
            return -1;
        }
    }

    /**
     * @return Input arguments passed to the JVM which does not include the arguments to the <tt>main</tt> method.
     */
    public static List<String> jvmArgs() {
        return ManagementFactory.getRuntimeMXBean().getInputArguments();
    }

    /**
     * As long as array copying uses JVM-private API, which is not guaranteed
     * to be available on all JVM, this method should be called to ensure
     * logic could work properly.
     *
     * @return {@code True} if unsafe copying can work on the current JVM or
     *      {@code false} if it can't.
     */
    @SuppressWarnings("TypeParameterExtendsFinalClass")
    private static boolean unsafeByteArrayCopyAvailable() {
        try {
            Class<? extends Unsafe> unsafeCls = Unsafe.class;

            unsafeCls.getMethod("copyMemory", Object.class, long.class, Object.class, long.class, long.class);

            return true;
        }
        catch (Exception ignored) {
            return false;
        }
    }

    /**
     * @param src Buffer to copy from (length included).
     * @param off Offset in source buffer.
     * @param resBuf Result buffer.
     * @param resOff Result offset.
     * @param len Length.
     * @return Number of bytes overwritten in {@code bytes} array.
     */
    public static int arrayCopy(byte[] src, int off, byte[] resBuf, int resOff, int len) {
        assert resBuf.length >= resOff + len;

        if (UNSAFE_BYTE_ARR_CP)
            GridUnsafe.copyMemory(src, GridUnsafe.BYTE_ARR_OFF + off, resBuf, GridUnsafe.BYTE_ARR_OFF + resOff, len);
        else
            System.arraycopy(src, off, resBuf, resOff, len);

        return resOff + len;
    }

    /**
     * @param addrs Node's addresses.
     * @return A string compatible with {@link ClusterNode#consistentId()} requirements.
     */
    public static String consistentId(Collection<String> addrs) {
        assert !F.isEmpty(addrs);

        StringBuilder sb = new StringBuilder();

        for (String addr : addrs)
            sb.append(addr).append(',');

        sb.delete(sb.length() - 1, sb.length());

        return sb.toString();
    }

    /**
     * @param addrs Node's addresses.
     * @param port Port discovery number.
     * @return A string compatible with {@link ClusterNode#consistentId()} requirements.
     */
    public static String consistentId(Collection<String> addrs, int port) {
        assert !F.isEmpty(addrs);

        return consistentId(addrs) + ':' + port;
    }

    /**
     * Masks name for a valid directory path.
     *
     * @param name Name.
     * @return Masked name.
     */
    public static String maskForFileName(CharSequence name) {
        StringBuilder b = new StringBuilder(name.length());

        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);

            if (Character.isLetterOrDigit(c))
                b.append(c);
            else
                b.append('_');
        }

        return b.toString();
    }

    /**
     * @param obj Object.
     * @return {@code True} if given object has overridden equals and hashCode method.
     */
    public static boolean overridesEqualsAndHashCode(Object obj) {
        return overridesEqualsAndHashCode(obj.getClass());
    }

    /**
     * @param cls Class.
     * @return {@code True} if given class has overridden equals and hashCode method.
     */
    public static boolean overridesEqualsAndHashCode(Class<?> cls) {
        try {
            return !Object.class.equals(cls.getMethod("equals", Object.class).getDeclaringClass()) &&
                !Object.class.equals(cls.getMethod("hashCode").getDeclaringClass());
        }
        catch (NoSuchMethodException | SecurityException ignore) {
            return true; // Ignore.
        }
    }

    /**
     * Checks if error is MAC invalid argument error which ususally requires special handling.
     *
     * @param e Exception.
     * @return {@code True} if error is invalid argument error on MAC.
     */
    public static boolean isMacInvalidArgumentError(Exception e) {
        return U.isMacOs() && e instanceof SocketException && e.getMessage() != null &&
            e.getMessage().toLowerCase().contains("invalid argument");
    }

    /**
     * Returns a first non-null value in a given array, if such is present.
     *
     * @param vals Input array.
     * @return First non-null value, or {@code null}, if array is empty or contains
     *      only nulls.
     */
    @Nullable public static <T> T firstNotNull(@Nullable T... vals) {
        if (vals == null)
            return null;

        for (T val : vals) {
            if (val != null)
                return val;
        }

        return null;
    }

    /**
     * For each object provided by the given {@link Iterable} checks if it implements
     * {@link LifecycleAware} interface and executes {@link LifecycleAware#start} method.
     *
     * @param objs Objects.
     * @throws IgniteCheckedException If {@link LifecycleAware#start} fails.
     */
    public static void startLifecycleAware(Iterable<?> objs) throws IgniteCheckedException {
        try {
            for (Object obj : objs) {
                if (obj instanceof LifecycleAware)
                    ((LifecycleAware)obj).start();
            }
        }
        catch (Exception e) {
            throw new IgniteCheckedException("Failed to start component: " + e, e);
        }
    }

    /**
     * For each object provided by the given {@link Iterable} checks if it implements {@link org.apache.ignite.lifecycle.LifecycleAware}
     * interface and executes {@link org.apache.ignite.lifecycle.LifecycleAware#stop} method.
     *
     * @param log Logger used to log error message in case of stop failure.
     * @param objs Object passed to Ignite configuration.
     */
    public static void stopLifecycleAware(IgniteLogger log, Iterable<?> objs) {
        for (Object obj : objs) {
            if (obj instanceof LifecycleAware) {
                try {
                    ((LifecycleAware)obj).stop();
                }
                catch (Exception e) {
                    U.error(log, "Failed to stop component (ignoring): " + obj, e);
                }
            }
        }
    }

    /**
     * Groups given nodes by the node's physical computer (host).
     * <p>
     * Detection of the same physical computer (host) is based on comparing set of network interface MACs.
     * If two nodes have the same set of MACs, Ignite considers these nodes running on the same
     * physical computer.
     *
     * @param nodes Nodes.
     * @return Collection of projections where each projection represents all nodes (in this projection)
     *      from a single physical computer. Result collection can be empty if this projection is empty.
     */
    public static Map<String, Collection<ClusterNode>> neighborhood(Iterable<ClusterNode> nodes) {
        Map<String, Collection<ClusterNode>> map = new HashMap<>();

        for (ClusterNode n : nodes) {
            String macs = n.attribute(ATTR_MACS);

            assert macs != null : "Missing MACs attribute: " + n;

            Collection<ClusterNode> neighbors = map.get(macs);

            if (neighbors == null)
                map.put(macs, neighbors = new ArrayList<>(2));

            neighbors.add(n);
        }

        return map;
    }

    /**
     * Returns tha list of resolved inet addresses. First addresses are resolved by host names,
     * if this attempt fails then the addresses are resolved by ip addresses.
     *
     * @param node Grid node.
     * @return Inet addresses for given addresses and host names.
     * @throws IgniteCheckedException If non of addresses can be resolved.
     */
    public static Collection<InetAddress> toInetAddresses(ClusterNode node) throws IgniteCheckedException {
        return toInetAddresses(node.addresses(), node.hostNames());
    }

    /**
     * Returns tha list of resolved inet addresses. First addresses are resolved by host names,
     * if this attempt fails then the addresses are resolved by ip addresses.
     *
     * @param addrs Addresses.
     * @param hostNames Host names.
     * @return Inet addresses for given addresses and host names.
     * @throws IgniteCheckedException If non of addresses can be resolved.
     */
    public static Collection<InetAddress> toInetAddresses(Collection<String> addrs,
        Collection<String> hostNames) throws IgniteCheckedException {
        Set<InetAddress> res = new HashSet<>(addrs.size());

        Iterator<String> hostNamesIt = hostNames.iterator();

        for (String addr : addrs) {
            String hostName = hostNamesIt.hasNext() ? hostNamesIt.next() : null;

            InetAddress inetAddr = null;

            if (!F.isEmpty(hostName)) {
                try {
                    inetAddr = InetAddress.getByName(hostName);
                }
                catch (UnknownHostException ignored) {
                }
            }

            if (inetAddr == null || inetAddr.isLoopbackAddress()) {
                try {
                    inetAddr = InetAddress.getByName(addr);
                }
                catch (UnknownHostException ignored) {
                }
            }

            if (inetAddr != null)
                res.add(inetAddr);
        }

        if (res.isEmpty())
            throw new IgniteCheckedException("Addresses can not be resolved [addr=" + addrs +
                ", hostNames=" + hostNames + ']');

        return res;
    }

    /**
     * Returns tha list of resolved socket addresses. First addresses are resolved by host names,
     * if this attempt fails then the addresses are resolved by ip addresses.
     *
     * @param node Grid node.
     * @param port Port.
     * @return Socket addresses for given addresses and host names.
     */
    public static Collection<InetSocketAddress> toSocketAddresses(ClusterNode node, int port) {
        return toSocketAddresses(node.addresses(), node.hostNames(), port, true);
    }

    /**
     * Returns tha list of resolved socket addresses. First addresses are resolved by host names,
     * if this attempt fails then the addresses are resolved by ip addresses.
     *
     * @param addrs Addresses.
     * @param hostNames Host names.
     * @param port Port.
     * @param resolve Whether to resolve addresses or not.
     * @return Socket addresses for given addresses and host names.
     */
    public static Collection<InetSocketAddress> toSocketAddresses(Collection<String> addrs,
        Collection<String> hostNames, int port, boolean resolve) {
        Set<InetSocketAddress> res = new HashSet<>(addrs.size());

        Iterator<String> hostNamesIt = hostNames.iterator();

        for (String addr : addrs) {
            String hostName = hostNamesIt.hasNext() ? hostNamesIt.next() : null;

            if (!F.isEmpty(hostName)) {
                InetSocketAddress inetSockAddr = resolve
                    ? createResolved(hostName, port)
                    : InetSocketAddress.createUnresolved(hostName, port);

                if (resolve && inetSockAddr.isUnresolved() ||
                    !inetSockAddr.isUnresolved() && inetSockAddr.getAddress().isLoopbackAddress()
                )
                    inetSockAddr = createResolved(addr, port);

                res.add(inetSockAddr);
            }

            // Always append address because local and remote nodes may have the same hostname
            // therefore remote hostname will be always resolved to local address.
            res.add(createResolved(addr, port));
        }

        return res;
    }

    /**
     * Creates a resolved inet socket address, writing the diagnostic information into a log if operation took
     * a significant amount of time.
     *
     * @param addr Host address.
     * @param port Port value.
     * @return Resolved address.
     */
    private static InetSocketAddress createResolved(String addr, int port) {
        log.log(Level.FINE, () -> S.toString(
            "Resolving address",
            "addr", addr, false,
            "port", port, false,
            "thread", Thread.currentThread().getName(), false
        ));

        long startNanos = System.nanoTime();

        try {
            return new InetSocketAddress(addr, port);
        }
        finally {
            long endNanos = System.nanoTime();

            long duration = endNanos - startNanos;

            long threshold = U.millisToNanos(200);

            if (duration > threshold) {
                log.log(Level.FINE, new TimeoutException(), () -> S.toString(
                    "Resolving address took too much time",
                    "duration(ms)", U.nanosToMillis(duration), false,
                    "addr", addr, false,
                    "port", port, false,
                    "thread", Thread.currentThread().getName(), false
                ));
            }
        }
    }

    /**
     * Resolves all not loopback addresses and collect results.
     *
     * @param addrRslvr Address resolver.
     * @param addrs Addresses.
     * @param port Port.
     * @return Resolved socket addresses.
     * @throws IgniteSpiException If failed.
     */
    public static Collection<InetSocketAddress> resolveAddresses(
        AddressResolver addrRslvr,
        Iterable<String> addrs,
        int port
    ) throws IgniteSpiException {
        assert addrRslvr != null;

        Collection<InetSocketAddress> extAddrs = new HashSet<>();

        for (String addr : addrs) {
            InetSocketAddress sockAddr = new InetSocketAddress(addr, port);

            if (!sockAddr.isUnresolved()) {
                Collection<InetSocketAddress> extAddrs0 = resolveAddress(addrRslvr, sockAddr);

                if (extAddrs0 != null)
                    extAddrs.addAll(extAddrs0);
            }
        }

        return extAddrs;
    }

    /**
     * @param addrRslvr Address resolver.
     * @param sockAddr Addresses.
     * @return Resolved addresses.
     */
    public static Collection<InetSocketAddress> resolveAddresses(AddressResolver addrRslvr,
        Collection<InetSocketAddress> sockAddr) {
        if (addrRslvr == null)
            return sockAddr;

        Collection<InetSocketAddress> resolved = new HashSet<>();

        for (InetSocketAddress address : sockAddr)
            resolved.addAll(resolveAddress(addrRslvr, address));

        return resolved;
    }

    /**
     * @param addrRslvr Address resolver.
     * @param sockAddr Addresses.
     * @return Resolved addresses.
     */
    private static Collection<InetSocketAddress> resolveAddress(AddressResolver addrRslvr, InetSocketAddress sockAddr) {
        try {
            return addrRslvr.getExternalAddresses(sockAddr);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteSpiException("Failed to get mapped external addresses " +
                "[addrRslvr=" + addrRslvr + ", addr=" + sockAddr + ']', e);
        }
    }

    /**
     * Returns string representation of node addresses.
     *
     * @param node Grid node.
     * @return String representation of addresses.
     */
    public static String addressesAsString(ClusterNode node) {
        return addressesAsString(node.addresses(), node.hostNames());
    }

    /**
     * Returns string representation of addresses.
     *
     * @param addrs Addresses.
     * @param hostNames Host names.
     * @return String representation of addresses.
     */
    public static String addressesAsString(Collection<String> addrs, Collection<String> hostNames) {
        if (F.isEmpty(addrs))
            return "";

        if (F.isEmpty(hostNames))
            return addrs.toString();

        SB sb = new SB("[");

        Iterator<String> hostNamesIt = hostNames.iterator();

        boolean first = true;

        for (String addr : addrs) {
            if (first)
                first = false;
            else
                sb.a(", ");

            String hostName = hostNamesIt.hasNext() ? hostNamesIt.next() : null;

            sb.a(hostName != null ? hostName : "").a('/').a(addr);
        }

        sb.a(']');

        return sb.toString();
    }

    /**
     * Get default work directory.
     *
     * @return Default work directory.
     */
    public static String defaultWorkDirectory() throws IgniteCheckedException {
        return workDirectory(null, null);
    }

    /**
     * Get work directory for the given user-provided work directory and Ignite home.
     *
     * @param userWorkDir Ignite work folder provided by user.
     * @param userIgniteHome Ignite home folder provided by user.
     */
    public static String workDirectory(@Nullable String userWorkDir, @Nullable String userIgniteHome)
        throws IgniteCheckedException {
        if (userIgniteHome == null)
            userIgniteHome = getIgniteHome();

        File workDir;

        if (!F.isEmpty(userWorkDir))
            workDir = new File(userWorkDir);
        else if (!F.isEmpty(IGNITE_WORK_DIR))
            workDir = new File(IGNITE_WORK_DIR);
        else if (!F.isEmpty(userIgniteHome))
            workDir = new File(userIgniteHome, DEFAULT_WORK_DIR);
        else {
            String userDir = System.getProperty("user.dir");

            if (F.isEmpty(userDir))
                throw new IgniteCheckedException(
                    "Failed to resolve Ignite work directory. Either IgniteConfiguration.setWorkDirectory or " +
                        "one of the system properties (" + IGNITE_HOME + ", " +
                        IgniteSystemProperties.IGNITE_WORK_DIR + ") must be explicitly set."
                );

            File igniteDir = new File(userDir, "ignite");

            try {
                igniteDir.mkdirs();

                File readme = new File(igniteDir, "README.txt");

                if (!readme.exists()) {
                    U.writeStringToFile(readme,
                        "This is GridGain working directory that contains information that \n" +
                        "    GridGain nodes need in order to function normally.\n" +
                        "Don't delete it unless you're sure you know what you're doing.\n\n" +
                        "You can change the location of working directory with \n" +
                        "    igniteConfiguration.setWorkingDirectory(location) or \n" +
                        "    <property name=\"workingDirectory\" value=\"location\"/> in IgniteConfiguration <bean>.\n");
                }
            }
            catch (Exception ignore) {
                // Ignore.
            }

            workDir = new File(igniteDir, DEFAULT_WORK_DIR);
        }

        if (!workDir.isAbsolute())
            throw new IgniteCheckedException("Work directory path must be absolute: " + workDir);

        if (!mkdirs(workDir))
            throw new IgniteCheckedException("Work directory does not exist and cannot be created: " + workDir);

        if (!workDir.canRead())
            throw new IgniteCheckedException("Cannot read from work directory: " + workDir);

        if (!workDir.canWrite())
            throw new IgniteCheckedException("Cannot write to work directory: " + workDir);

        return workDir.getAbsolutePath();
    }

    /**
     * Nullifies Ignite home directory. For test purposes only.
     */
    public static void nullifyHomeDirectory() {
        ggHome = null;
    }

    /**
     * Resolves work directory.
     *
     * @param workDir Work directory.
     * @param path Path to resolve.
     * @param delIfExist Flag indicating whether to delete the specify directory or not.
     * @return Resolved work directory.
     * @throws IgniteCheckedException If failed.
     */
    public static File resolveWorkDirectory(String workDir, String path, boolean delIfExist)
        throws IgniteCheckedException {
        File dir = new File(path);

        if (!dir.isAbsolute()) {
            if (F.isEmpty(workDir))
                throw new IgniteCheckedException("Failed to resolve path (work directory has not been set): " + path);

            dir = new File(workDir, dir.getPath());
        }

        if (delIfExist && dir.exists()) {
            if (!U.delete(dir))
                throw new IgniteCheckedException("Failed to delete directory: " + dir);
        }

        if (!mkdirs(dir))
            throw new IgniteCheckedException("Directory does not exist and cannot be created: " + dir);

        if (!dir.canRead())
            throw new IgniteCheckedException("Cannot read from directory: " + dir);

        if (!dir.canWrite())
            throw new IgniteCheckedException("Cannot write to directory: " + dir);

        return dir;
    }

    /**
     * Checks if the given directory exists and attempts to create one if not.
     *
     * @param dir Directory to check.
     * @param msg Directory name for the messages.
     * @param log Optional logger to log a message that the directory has been resolved.
     * @throws IgniteCheckedException If directory does not exist and failed to create it, or if a file with
     *      the same name already exists.
     */
    public static void ensureDirectory(File dir, String msg, IgniteLogger log) throws IgniteCheckedException {
        if (!dir.exists()) {
            if (!dir.mkdirs())
                throw new IgniteCheckedException("Failed to create " + msg + ": " +
                    dir.getAbsolutePath());
        }
        else if (!dir.isDirectory())
            throw new IgniteCheckedException("Failed to initialize " + msg +
                " (a file with the same name already exists): " + dir.getAbsolutePath());

        if (log != null && log.isInfoEnabled())
            log.info("Resolved " + msg + ": " + dir.getAbsolutePath());
    }

    /**
     * Checks if the given directory exists and attempts to create one if not.
     *
     * @param dir Directory to check.
     * @param msg Directory name for the messages.
     * @param log Optional logger to log a message that the directory has been resolved.
     * @throws IgniteCheckedException If directory does not exist and failed to create it, or if a file with
     *      the same name already exists.
     */
    public static void ensureDirectory(Path dir, String msg, IgniteLogger log) throws IgniteCheckedException {
        if (!Files.exists(dir)) {
            try {
                Files.createDirectories(dir);
            }
            catch (IOException e) {
                throw new IgniteCheckedException("Failed to create " + msg + ": " + dir.toAbsolutePath(), e);
            }
        }
        else if (!Files.isDirectory(dir))
            throw new IgniteCheckedException("Failed to initialize " + msg +
                " (a file with the same name already exists): " + dir.toAbsolutePath());

        if (log != null && log.isInfoEnabled())
            log.info("Resolved " + msg + ": " + dir.toAbsolutePath());
    }

    /**
     * Creates {@code IgniteCheckedException} with the collection of suppressed exceptions.
     *
     * @param msg Message.
     * @param suppressed The collections of suppressed exceptions.
     * @return {@code IgniteCheckedException}.
     */
    public static IgniteCheckedException exceptionWithSuppressed(String msg, @Nullable Collection<Throwable> suppressed) {
        IgniteCheckedException e = new IgniteCheckedException(msg);

        if (suppressed != null) {
            for (Throwable th : suppressed)
                e.addSuppressed(th);
        }

        return e;
    }

    /**
     * Extracts full name of enclosing class from JDK8 lambda class name.
     *
     * @param clsName JDK8 lambda class name.
     * @return Full name of enclosing class for JDK8 lambda class name or
     *      {@code null} if passed in name is not related to lambda.
     */
    @Nullable public static String lambdaEnclosingClassName(String clsName) {
        int idx0 = clsName.indexOf("$$Lambda$"); // Java 8+
        int idx1 = clsName.indexOf("$$Lambda/"); // Java 21+

        if (idx0 == idx1)
            return null;

        return clsName.substring(0, idx0 >= 0 ? idx0 : idx1);
    }

    /**
     * Converts a hexadecimal character to an integer.
     *
     * @param ch A character to convert to an integer digit
     * @param idx The index of the character in the source
     * @return An integer
     * @throws IgniteCheckedException Thrown if ch is an illegal hex character
     */
    public static int toDigit(char ch, int idx) throws IgniteCheckedException {
        int digit = Character.digit(ch, 16);

        if (digit == -1)
            throw new IgniteCheckedException("Illegal hexadecimal character " + ch + " at index " + idx);

        return digit;
    }

    /**
     * Gets oldest node out of collection of nodes.
     *
     * @param c Collection of nodes.
     * @return Oldest node.
     */
    public static ClusterNode oldest(Collection<ClusterNode> c, @Nullable IgnitePredicate<ClusterNode> p) {
        ClusterNode oldest = null;

        long minOrder = Long.MAX_VALUE;

        for (ClusterNode n : c) {
            if ((p == null || p.apply(n)) && n.order() < minOrder) {
                oldest = n;

                minOrder = n.order();
            }
        }

        return oldest;
    }

    /**
     * Gets youngest node out of collection of nodes.
     *
     * @param c Collection of nodes.
     * @return Youngest node.
     */
    public static ClusterNode youngest(Collection<ClusterNode> c, @Nullable IgnitePredicate<ClusterNode> p) {
        ClusterNode youngest = null;

        long maxOrder = Long.MIN_VALUE;

        for (ClusterNode n : c) {
            if ((p == null || p.apply(n)) && n.order() > maxOrder) {
                youngest = n;

                maxOrder = n.order();
            }
        }

        return youngest;
    }

    /**
     * @param ctx Kernal context.
     * @return Whether current node is oldest.
     */
    public static boolean isCurrentNodeOldest(GridKernalContext ctx) {
        ClusterNode oldest = ctx.grid().cluster().forServers().forOldest().node();

        return F.eq(ctx.localNodeId(), oldest.id());
    }

    /**
     * @param ptr Address.
     * @param size Size.
     * @return Bytes.
     */
    public static byte[] copyMemory(long ptr, int size) {
        byte[] res = new byte[size];

        GridUnsafe.copyMemory(null, ptr, res, GridUnsafe.BYTE_ARR_OFF, size);

        return res;
    }

    /**
     * Returns a capacity that is sufficient to keep the map from being resized as
     * long as it grows no larger than expSize and the load factor is >= its
     * default (0.75).
     *
     * Copy pasted from guava. See com.google.common.collect.Maps#capacity(int)
     *
     * @param expSize Expected size of created map.
     * @return Capacity.
     */
    public static int capacity(int expSize) {
        if (expSize < 3)
            return expSize + 1;

        if (expSize < (1 << 30))
            return expSize + expSize / 3;

        return Integer.MAX_VALUE; // any large value
    }

    /**
     * Creates new {@link HashMap} with expected size.
     *
     * @param expSize Expected size of created map.
     * @param <K> Type of map keys.
     * @param <V> Type of map values.
     * @return New map.
     */
    public static <K, V> HashMap<K, V> newHashMap(int expSize) {
        return new HashMap<>(capacity(expSize));
    }

    /**
     * Creates new {@link LinkedHashMap} with expected size.
     *
     * @param expSize Expected size of created map.
     * @param <K> Type of map keys.
     * @param <V> Type of map values.
     * @return New map.
     */
    public static <K, V> LinkedHashMap<K, V> newLinkedHashMap(int expSize) {
        return new LinkedHashMap<>(capacity(expSize));
    }

    /**
     * Creates new {@link HashSet} with expected size.
     *
     * @param expSize Expected size of created map.
     * @param <T> Type of elements.
     * @return New set.
     */
    public static <T> HashSet<T> newHashSet(int expSize) {
        return new HashSet<>(capacity(expSize));
    }

    /**
     * Creates new {@link LinkedHashSet} with expected size.
     *
     * @param expSize Expected size of created map.
     * @param <T> Type of elements.
     * @return New set.
     */
    public static <T> LinkedHashSet<T> newLinkedHashSet(int expSize) {
        return new LinkedHashSet<>(capacity(expSize));
    }

    /**
     * Creates new {@link Set} based on {@link ConcurrentHashMap}.
     *
     * @param <T> Type of elements.
     * @return New concurrent set.
     */
    public static <T> Set<T> newConcurrentHashSet() {
        return Collections.newSetFromMap(new ConcurrentHashMap<>());
    }

    /**
     * Constructs a new {@link Set} based on {@link ConcurrentHashMap},
     * containing the elements in the specified collection.
     *
     * @param <T> Type of elements.
     * @param c Source collection.
     * @return New concurrent set.
     */
    public static <T> Set<T> newConcurrentHashSet(Collection<T> c) {
        Set<T> set = newConcurrentHashSet();

        set.addAll(c);

        return set;
    }

    /**
     * Creates new map that limited by size.
     *
     * @param limit Limit for size.
     */
    public static <K, V> Map<K, V> limitedMap(int limit) {
        if (limit == 0)
            return Collections.emptyMap();

        if (limit < 5)
            return new GridLeanMap<>(limit);

        return new HashMap<>(capacity(limit), 0.75f);
    }

    /**
     * Create a map with single key-value pair.
     *
     * @param k Key.
     * @param v Value.
     * @return Map.
     */
    public static <K, V> Map<K, V> map(K k, V v) {
        GridLeanMap<K, V> map = new GridLeanMap<>(1);

        map.put(k, v);

        return map;
    }

    /**
     * Create a map with two key-value pairs.
     *
     * @param k1 Key 1.
     * @param v1 Value 1.
     * @param k2 Key 2.
     * @param v2 Value 2.
     * @return Map.
     */
    public static <K, V> Map<K, V> map(K k1, V v1, K k2, V v2) {
        GridLeanMap<K, V> map = new GridLeanMap<>(2);

        map.put(k1, v1);
        map.put(k2, v2);

        return map;
    }

    /**
     * Create a map with three key-value pairs.
     *
     * @param k1 Key 1.
     * @param v1 Value 1.
     * @param k2 Key 2.
     * @param v2 Value 2.
     * @param k3 Key 3.
     * @param v3 Value 3.
     * @return Map.
     */
    public static <K, V> Map<K, V> map(K k1, V v1, K k2, V v2, K k3, V v3) {
        GridLeanMap<K, V> map = new GridLeanMap<>(3);

        map.put(k1, v1);
        map.put(k2, v2);
        map.put(k3, v3);

        return map;
    }

    /**
     * @param col non-null collection with one element
     * @return a SingletonList containing the element in the original collection
     */
    public static <T> Collection<T> convertToSingletonList(Collection<T> col) {
        if (col.size() != 1) {
            throw new IllegalArgumentException("Unexpected collection size for singleton list, expecting 1 but was: " + col.size());
        }
        return Collections.singletonList(col.iterator().next());
    }

    /**
     * Returns comparator that sorts remote node addresses. If remote node resides on the same host, then put
     * loopback addresses first, last otherwise.
     *
     * @param sameHost {@code True} if remote node resides on the same host, {@code false} otherwise.
     * @return Comparator.
     */
    public static Comparator<InetSocketAddress> inetAddressesComparator(final boolean sameHost) {
        return new Comparator<InetSocketAddress>() {
            @Override public int compare(InetSocketAddress addr1, InetSocketAddress addr2) {
                if (addr1.isUnresolved() && addr2.isUnresolved())
                    return 0;

                if (addr1.isUnresolved() || addr2.isUnresolved())
                    return addr1.isUnresolved() ? 1 : -1;

                boolean addr1Loopback = addr1.getAddress().isLoopbackAddress();

                // No need to reorder.
                if (addr1Loopback == addr2.getAddress().isLoopbackAddress())
                    return 0;

                if (sameHost)
                    return addr1Loopback ? -1 : 1;
                else
                    return addr1Loopback ? 1 : -1;
            }
        };
    }

    /**
     * Finds a method in the class and it parents.
     *
     * Method.getMethod() does not return non-public method,
     * Method.getDeclaratedMethod() does not look at parent classes.
     *
     * @param cls The class to search,
     * @param name Name of the method.
     * @param paramTypes Method parameters.
     * @return Method or {@code null}.
     */
    @Nullable public static Method findNonPublicMethod(Class<?> cls, String name, Class<?>... paramTypes) {
        while (cls != null) {
            Method mtd = getNonPublicMethod(cls, name, paramTypes);

            if (mtd != null)
                return mtd;

            cls = cls.getSuperclass();
        }

        return null;
    }

    /**
     * Gets a method from the class.
     *
     * Method.getMethod() does not return non-public method.
     *
     * @param cls Target class.
     * @param name Name of the method.
     * @param paramTypes Method parameters.
     * @return Method or {@code null}.
     */
    @Nullable public static Method getNonPublicMethod(Class<?> cls, String name, Class<?>... paramTypes) {
        try {
            Method mtd = cls.getDeclaredMethod(name, paramTypes);

            mtd.setAccessible(true);

            return mtd;
        }
        catch (NoSuchMethodException ignored) {
            // No-op.
        }

        return null;
    }

    /**
     * Finds a non-static and non-abstract method from the class it parents.
     *
     * Method.getMethod() does not return non-public method.
     *
     * @param cls Target class.
     * @param name Name of the method.
     * @param paramTypes Method parameters.
     * @return Method or {@code null}.
     */
    @Nullable public static Method findInheritableMethod(Class<?> cls, String name, Class<?>... paramTypes) {
        Method mtd = null;

        Class<?> cls0 = cls;

        while (cls0 != null) {
            try {
                mtd = cls0.getDeclaredMethod(name, paramTypes);

                break;
            }
            catch (NoSuchMethodException e) {
                cls0 = cls0.getSuperclass();
            }
        }

        if (mtd == null)
            return null;

        mtd.setAccessible(true);

        int mods = mtd.getModifiers();

        if ((mods & (Modifier.STATIC | Modifier.ABSTRACT)) != 0)
            return null;
        else if ((mods & (Modifier.PUBLIC | Modifier.PROTECTED)) != 0)
            return mtd;
        else if ((mods & Modifier.PRIVATE) != 0)
            return cls == cls0 ? mtd : null;
        else {
            ClassLoader clsLdr = cls.getClassLoader();

            ClassLoader clsLdr0 = cls0.getClassLoader();

            return clsLdr == clsLdr0 && packageName(cls).equals(packageName(cls0)) ? mtd : null;
        }
    }

    /**
     * @param cls Class.
     * @return Package name.
     */
    private static String packageName(Class<?> cls) {
        Package pkg = cls.getPackage();

        return pkg == null ? "" : pkg.getName();
    }

    /**
     * @param cls The class to search.
     * @param name Name of a field to get.
     * @return Field or {@code null}.
     */
    @Nullable public static Field findField(Class<?> cls, String name) {
        while (cls != null) {
            try {
                Field fld = cls.getDeclaredField(name);

                if (!fld.isAccessible())
                    fld.setAccessible(true);

                return fld;
            }
            catch (NoSuchFieldException ignored) {
                // No-op.
            }

            cls = cls.getSuperclass();
        }

        return null;
    }

    /**
     * @param c Collection.
     * @param p Optional filters.
     * @return Resulting array list.
     */
    public static <T extends R, R> List<R> arrayList(Collection<T> c, @Nullable IgnitePredicate<? super T>... p) {
        assert c != null;

        return arrayList(c.iterator(), c.size(), p);
    }

    /**
     * @param c Collection.
     * @return Resulting array list.
     */
    public static <T extends R, R> List<R> arrayList(Collection<T> c) {
        assert c != null;

        return new ArrayList<R>(c);
    }

    /**
     * @param c Collection.
     * @param cap Initial capacity.
     * @param p Optional filters.
     * @return Resulting array list.
     */
    public static <T extends R, R> List<R> arrayList(Iterator<T> c, int cap,
        @Nullable IgnitePredicate<? super T>... p) {
        assert c != null;
        assert cap >= 0;

        List<R> list = new ArrayList<>(cap);

        while (c.hasNext()) {
            T t = c.next();

            if (F.isAll(t, p))
                list.add(t);
        }

        return list;
    }

    /**
     * Calculate MD5 digits.
     *
     * @param in Input stream.
     * @return Calculated MD5 digest for given input stream.
     * @throws NoSuchAlgorithmException If MD5 algorithm was not found.
     * @throws IOException If an I/O exception occurs.
     */
    public static byte[] calculateMD5Digest(@NotNull InputStream in) throws NoSuchAlgorithmException, IOException {
        MessageDigest md = MessageDigest.getInstance("MD5");
        InputStream fis = new BufferedInputStream(in);
        byte[] dataBytes = new byte[1024];

        int nread;

        while ((nread = fis.read(dataBytes)) != -1)
            md.update(dataBytes, 0, nread);

        return md.digest();
    }

    /**
     * Calculate MD5 string.
     *
     * @param in Input stream.
     * @return Calculated MD5 string for given input stream.
     * @throws NoSuchAlgorithmException If MD5 algorithm was not found.
     * @throws IOException If an I/O exception occurs.
     */
    public static String calculateMD5(InputStream in) throws NoSuchAlgorithmException, IOException {
        byte[] md5Bytes = calculateMD5Digest(in);

        // Convert the byte to hex format.
        StringBuilder sb = new StringBuilder();

        for (byte md5Byte : md5Bytes)
            sb.append(Integer.toString((md5Byte & 0xff) + 0x100, 16).substring(1));

        return sb.toString();
    }

    /**
     * Fully writes communication message to provided stream.
     *
     * @param msg Message.
     * @param out Stream to write to.
     * @param buf Byte buffer that will be passed to {@link Message#writeTo(ByteBuffer, MessageWriter)} method.
     * @param writer Message writer.
     * @return Number of written bytes.
     * @throws IOException In case of error.
     */
    public static int writeMessageFully(Message msg, OutputStream out, ByteBuffer buf,
        MessageWriter writer) throws IOException {
        assert msg != null;
        assert out != null;
        assert buf != null;
        assert buf.hasArray();

        if (writer != null)
            writer.setCurrentWriteClass(msg.getClass());

        boolean finished = false;
        int cnt = 0;

        while (!finished) {
            finished = msg.writeTo(buf, writer);

            out.write(buf.array(), 0, buf.position());

            cnt += buf.position();

            buf.clear();
        }

        return cnt;
    }

    /**
     * Throws exception with uniform error message if given parameter's assertion condition
     * is {@code false}.
     *
     * @param cond Assertion condition to check.
     * @param condDesc Description of failed condition.
     */
    public static void assertParameter(boolean cond, String condDesc) throws IgniteException {
        if (!cond)
            throw new IgniteException("Parameter failed condition check: " + condDesc);
    }

    /**
     * @return Whether shared memory libraries exist.
     */
    public static boolean hasSharedMemory() {
        if (hasShmem == null) {
            if (isWindows())
                hasShmem = false;
            else {
                try {
                    IpcSharedMemoryNativeLoader.load(null);

                    hasShmem = true;
                }
                catch (IgniteCheckedException ignore) {
                    hasShmem = false;
                }
            }
        }

        return hasShmem;
    }

    /**
     * @param lock Lock.
     * @throws IgniteInterruptedCheckedException If interrupted.
     */
    public static void writeLock(ReadWriteLock lock) throws IgniteInterruptedCheckedException {
        try {
            lock.writeLock().lockInterruptibly();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IgniteInterruptedCheckedException(e);
        }
    }

    /**
     * Defines which protocol version to use for
     * communication with the provided node.
     *
     * @param ctx Context.
     * @param nodeId Node ID.
     * @return Protocol version.
     * @throws IgniteCheckedException If node doesn't exist.
     */
    public static byte directProtocolVersion(GridKernalContext ctx, UUID nodeId) throws IgniteCheckedException {
        assert nodeId != null;

        ClusterNode node = ctx.discovery().node(nodeId);

        if (node == null)
            throw new IgniteCheckedException("Failed to define communication protocol version " +
                "(has node left topology?): " + nodeId);

        assert !node.isLocal();

        Byte attr = node.attribute(GridIoManager.DIRECT_PROTO_VER_ATTR);

        byte rmtProtoVer = attr != null ? attr : 1;

        if (rmtProtoVer < GridIoManager.DIRECT_PROTO_VER)
            return rmtProtoVer;
        else
            return GridIoManager.DIRECT_PROTO_VER;
    }

    /**
     * @return Whether provided method is {@code Object.hashCode()}.
     */
    public static boolean isHashCodeMethod(Method mtd) {
        return hashCodeMtd.equals(mtd);
    }

    /**
     * @return Whether provided method is {@code Object.equals(...)}.
     */
    public static boolean isEqualsMethod(Method mtd) {
        return equalsMtd.equals(mtd);
    }

    /**
     * @return Whether provided method is {@code Object.toString()}.
     */
    public static boolean isToStringMethod(Method mtd) {
        return toStringMtd.equals(mtd);
    }

    /**
     * @param threadId Thread ID.
     * @return Thread name if found.
     */
    public static String threadName(long threadId) {
        Thread[] threads = new Thread[Thread.activeCount()];

        int cnt = Thread.enumerate(threads);

        for (int i = 0; i < cnt; i++)
            if (threads[i].getId() == threadId)
                return threads[i].getName();

        return "<failed to find active thread " + threadId + '>';
    }

    /**
     * @param t0 Comparable object.
     * @param t1 Comparable object.
     * @param <T> Comparable type.
     * @return Maximal object o t0 and t1.
     */
    public static <T extends Comparable<? super T>> T max(T t0, T t1) {
        return t0.compareTo(t1) > 0 ? t0 : t1;
    }

    /**
     * Unmarshals object from the input stream using given class loader.
     * This method should not close given input stream.
     * <p/>
     * This method wraps marshaller invocations and guaranty throws {@link IgniteCheckedException} in fail case.
     *
     * @param <T> Type of unmarshalled object.
     * @param in Input stream.
     * @param clsLdr Class loader to use.
     * @return Unmarshalled object.
     * @throws IgniteCheckedException If unmarshalling failed.
     */
    public static <T> T unmarshal(Marshaller marsh, InputStream in, @Nullable ClassLoader clsLdr)
        throws IgniteCheckedException {
        assert marsh != null;
        assert in != null;

        try {
            return marsh.unmarshal(in, clsLdr);
        }
        catch (IgniteCheckedException e) {
            throw e;
        }
        catch (Exception e) {
            throw new IgniteCheckedException(e);
        }
    }

    /**
     * @param marsh Marshaller.
     * @param zipBytes Zip-compressed bytes.
     * @param clsLdr Class loader to use.
     * @return Unmarshalled object.
     * @throws IgniteCheckedException
     */
    public static <T> T unmarshalZip(Marshaller marsh, byte[] zipBytes, @Nullable ClassLoader clsLdr) throws IgniteCheckedException {
        assert marsh != null;
        assert zipBytes != null;

        try {
            ZipInputStream in = new ZipInputStream(new ByteArrayInputStream(zipBytes));

            in.getNextEntry();

            return marsh.unmarshal(in, clsLdr);
        }
        catch (IgniteCheckedException e) {
            throw e;
        }
        catch (Exception e) {
            throw new IgniteCheckedException(e);
        }
    }

    /**
     * @param zipBytes Zipped bytes.
     * @return Raw bytes.
     * @throws IgniteCheckedException If unzip resulted in error.
     */
    public static byte[] unzip(byte[] zipBytes) throws IgniteCheckedException {
        assert zipBytes != null;

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ZipInputStream in = new ZipInputStream(new ByteArrayInputStream(zipBytes))) {

            in.getNextEntry();

            byte[] tmp = new byte[4 << 10];

            int size;

            while ((size = in.read(tmp)) != -1)
                baos.write(tmp, 0, size);

            return baos.toByteArray();
        }
        catch (Exception e) {
            throw new IgniteCheckedException(e);
        }
    }

    /**
     * Unmarshals object from the input stream using given class loader.
     * This method should not close given input stream.
     * <p/>
     * This method wraps marshaller invocations and guaranty throws {@link IgniteCheckedException} in fail case.
     *
     * @param <T> Type of unmarshalled object.
     * @param marsh Marshaller.
     * @param arr Byte array.
     * @param clsLdr Class loader to use.
     * @return Unmarshalled object.
     * @throws IgniteCheckedException If unmarshalling failed.
     */
    public static <T> T unmarshal(Marshaller marsh, byte[] arr, @Nullable ClassLoader clsLdr)
        throws IgniteCheckedException {
        assert marsh != null;
        assert arr != null;

        try {
            return marsh.unmarshal(arr, clsLdr);
        }
        catch (IgniteCheckedException e) {
            throw e;
        }
        catch (Exception e) {
            throw new IgniteCheckedException(e);
        }
    }

    /**
     * Unmarshals object from the input stream using given class loader.
     * This method should not close given input stream.
     * <p/>
     * This method wraps marshaller invocations and guaranty throws {@link IgniteCheckedException} in fail case.
     *
     * @param <T> Type of unmarshalled object.
     * @param ctx Kernal contex.
     * @param arr Byte array.
     * @param clsLdr Class loader to use.
     * @return Unmarshalled object.
     * @throws IgniteCheckedException If unmarshalling failed.
     */
    public static <T> T unmarshal(GridKernalContext ctx, byte[] arr, @Nullable ClassLoader clsLdr)
        throws IgniteCheckedException {
        assert ctx != null;
        assert arr != null;

        try {
            return U.unmarshal(ctx.config().getMarshaller(), arr, clsLdr);
        }
        catch (IgniteCheckedException e) {
            throw e;
        }
        catch (Exception e) {
            throw new IgniteCheckedException(e);
        }
    }

    /**
     * Unmarshals object from the input stream using given class loader.
     * This method should not close given input stream.
     * <p/>
     * This method wraps marshaller invocations and guaranty throws {@link IgniteCheckedException} in fail case.
     *
     * @param <T> Type of unmarshalled object.
     * @param ctx Kernal contex.
     * @param arr Byte array.
     * @param clsLdr Class loader to use.
     * @return Unmarshalled object.
     * @throws IgniteCheckedException If unmarshalling failed.
     */
    public static <T> T unmarshal(GridCacheSharedContext ctx, byte[] arr, @Nullable ClassLoader clsLdr)
        throws IgniteCheckedException {
        assert ctx != null;
        assert arr != null;

        try {
            return U.unmarshal(ctx.marshaller(), arr, clsLdr);
        }
        catch (IgniteCheckedException e) {
            throw e;
        }
        catch (Exception e) {
            throw new IgniteCheckedException(e);
        }
    }

    /**
     * Marshals object to byte array.
     * <p/>
     * This method wraps marshaller invocations and guaranty throws {@link IgniteCheckedException} in fail case.
     *
     * @param marsh Marshaller.
     * @param obj Object to marshal.
     * @return Byte array.
     * @throws IgniteCheckedException If marshalling failed.
     */
    public static byte[] marshal(Marshaller marsh, Object obj) throws IgniteCheckedException {
        assert marsh != null;

        try {
            return marsh.marshal(obj);
        }
        catch (IgniteCheckedException e) {
            throw e;
        }
        catch (Exception e) {
            throw new IgniteCheckedException(e);
        }
    }

    /**
     * Marshals object to byte array.
     * <p/>
     * This method wraps marshaller invocations and guaranty throws {@link IgniteCheckedException} in fail case.
     *
     * @param marsh Marshaller.
     * @param obj Object to marshal.
     * @param out Output stream.
     * @throws IgniteCheckedException If marshalling failed.
     */
    public static void marshal(Marshaller marsh, @Nullable Object obj, OutputStream out)
        throws IgniteCheckedException {
        assert marsh != null;

        try {
            marsh.marshal(obj, out);
        }
        catch (IgniteCheckedException e) {
            throw e;
        }
        catch (Exception e) {
            throw new IgniteCheckedException(e);
        }
    }

    /**
     * Marshals object to byte array. Wrap marshaller
     * <p/>
     * This method wraps marshaller invocations and guaranty throws {@link IgniteCheckedException} in fail case.
     *
     * @param ctx Kernal context.
     * @param obj Object to marshal.
     * @return Byte array.
     * @throws IgniteCheckedException If marshalling failed.
     */
    public static byte[] marshal(GridKernalContext ctx, Object obj) throws IgniteCheckedException {
        assert ctx != null;

        return marshal(ctx.config().getMarshaller(), obj);
    }

    /**
     * Marshals object to byte array. Wrap marshaller
     * <p/>
     * This method wraps marshaller invocations and guaranty throws {@link IgniteCheckedException} in fail case.
     *
     * @param ctx Cache context.
     * @param obj Object to marshal.
     * @return Byte array.
     * @throws IgniteCheckedException If marshalling failed.
     */
    public static byte[] marshal(GridCacheSharedContext ctx, Object obj) throws IgniteCheckedException {
        assert ctx != null;

        return marshal(ctx.marshaller(), obj);
    }

    /**
     * Get current Ignite name.
     *
     * @return Current Ignite name.
     */
    @Nullable public static String getCurrentIgniteName() {
        return LOC_IGNITE_NAME.get();
    }

    /**
     * Check if current Ignite name is set.
     *
     * @param name Name to check.
     * @return {@code True} if set.
     */
    @SuppressWarnings("StringEquality")
    public static boolean isCurrentIgniteNameSet(@Nullable String name) {
        return name != LOC_IGNITE_NAME_EMPTY;
    }

    /**
     * Set current Ignite name.
     *
     * @param newName New name.
     * @return Old name.
     */
    @SuppressWarnings("StringEquality")
    @Nullable public static String setCurrentIgniteName(@Nullable String newName) {
        String oldName = LOC_IGNITE_NAME.get();

        if (oldName != newName)
            LOC_IGNITE_NAME.set(newName);

        return oldName;
    }

    /**
     * Restore old Ignite name.
     *
     * @param oldName Old name.
     * @param curName Current name.
     */
    @SuppressWarnings("StringEquality")
    public static void restoreOldIgniteName(@Nullable String oldName, @Nullable String curName) {
        if (oldName != curName)
            LOC_IGNITE_NAME.set(oldName);
    }

    /**
     * Zip binary payload using default compression.
     *
     * @param bytes Byte array to compress.
     * @return Compressed bytes.
     * @throws IgniteCheckedException If failed.
     */
    public static byte[] zip(@Nullable byte[] bytes) throws IgniteCheckedException {
        return zip(bytes, Deflater.DEFAULT_COMPRESSION);
    }

    /**
     * @param bytes Byte array to compress.
     * @param compressionLevel Level of compression to encode.
     * @return Compressed bytes.
     * @throws IgniteCheckedException If failed.
     */
    public static byte[] zip(@Nullable byte[] bytes, int compressionLevel) throws IgniteCheckedException {
        try {
            if (bytes == null)
                return null;

            ByteArrayOutputStream bos = new ByteArrayOutputStream();

            try (ZipOutputStream zos = new ZipOutputStream(bos)) {
                zos.setLevel(compressionLevel);

                ZipEntry entry = new ZipEntry("");

                try {
                    entry.setSize(bytes.length);

                    zos.putNextEntry(entry);

                    zos.write(bytes);
                }
                finally {
                    zos.closeEntry();
                }
            }

            return bos.toByteArray();
        }
        catch (Exception e) {
            throw new IgniteCheckedException(e);
        }
    }

    /**
     * Serialize object to byte array.
     *
     * @param obj Object.
     * @return Serialized object.
     */
    public static byte[] toBytes(Serializable obj) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(bos)) {

            oos.writeObject(obj);
            oos.flush();

            return bos.toByteArray();
        }
        catch (IOException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * Deserialize object from byte array.
     *
     * @param data Serialized object.
     * @return Object.
     */
    public static <T> T fromBytes(byte[] data) {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(data);
             ObjectInputStream ois = new ObjectInputStream(bis)) {

            return (T)ois.readObject();
        }
        catch (IOException | ClassNotFoundException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * Get checkpoint buffer size for the given configuration.
     *
     * @param regCfg Configuration.
     * @return Checkpoint buffer size.
     */
    public static long checkpointBufferSize(DataRegionConfiguration regCfg) {
        if (!regCfg.isPersistenceEnabled())
            return 0L;

        long res = regCfg.getCheckpointPageBufferSize();

        if (res == 0L) {
            if (regCfg.getMaxSize() < GB)
                res = Math.min(DFLT_MIN_CHECKPOINTING_PAGE_BUFFER_SIZE, regCfg.getMaxSize());
            else if (regCfg.getMaxSize() < 8 * GB)
                res = regCfg.getMaxSize() / 4;
            else
                res = DFLT_MAX_CHECKPOINTING_PAGE_BUFFER_SIZE;
        }

        return res;
    }

    /**
     * Calculates maximum WAL archive size based on maximum checkpoint buffer size, if the default value of
     * {@link DataStorageConfiguration#getMaxWalArchiveSize()} is not overridden.
     *
     * @return User-set max WAL archive size of triple size of the maximum checkpoint buffer.
     */
    public static long adjustedWalHistorySize(DataStorageConfiguration dsCfg, @Nullable IgniteLogger log) {
        if (dsCfg.getMaxWalArchiveSize() != DataStorageConfiguration.UNLIMITED_WAL_ARCHIVE &&
            dsCfg.getMaxWalArchiveSize() != DataStorageConfiguration.DFLT_WAL_ARCHIVE_MAX_SIZE)
            return dsCfg.getMaxWalArchiveSize();

        // Find out the maximum checkpoint buffer size.
        long maxCpBufSize = 0;

        if (dsCfg.getDataRegionConfigurations() != null) {
            for (DataRegionConfiguration regCfg : dsCfg.getDataRegionConfigurations()) {
                long cpBufSize = checkpointBufferSize(regCfg);

                if (cpBufSize > regCfg.getMaxSize())
                    cpBufSize = regCfg.getMaxSize();

                if (cpBufSize > maxCpBufSize)
                    maxCpBufSize = cpBufSize;
            }
        }

        {
            DataRegionConfiguration regCfg = dsCfg.getDefaultDataRegionConfiguration();

            long cpBufSize = checkpointBufferSize(regCfg);

            if (cpBufSize > regCfg.getMaxSize())
                cpBufSize = regCfg.getMaxSize();

            if (cpBufSize > maxCpBufSize)
                maxCpBufSize = cpBufSize;
        }

        long adjustedWalArchiveSize = maxCpBufSize * 256;

        if (adjustedWalArchiveSize > dsCfg.getMaxWalArchiveSize()) {
            if (log != null)
                U.quietAndInfo(log, "Automatically adjusted max WAL archive size to " +
                    U.readableSize(adjustedWalArchiveSize, false) +
                    " (to override, use DataStorageConfiguration.setMaxWalArchiveSize)");

            return adjustedWalArchiveSize;
        }

        return dsCfg.getMaxWalArchiveSize();
    }

    /**
     * Return count of regular file in the directory (including in sub-directories)
     *
     * @param dir path to directory
     * @return count of regular file
     * @throws IOException sometimes
     */
    public static int fileCount(Path dir) throws IOException {
        int cnt = 0;

        try (DirectoryStream<Path> ds = Files.newDirectoryStream(dir)) {
            for (Path d : ds) {
                if (Files.isDirectory(d))
                    cnt += fileCount(d);

                else if (Files.isRegularFile(d))
                    cnt++;
            }
        }

        return cnt;
    }

    /**
     * Will calculate the size of a directory.
     *
     * If there is concurrent activity in the directory, than returned value may be wrong.
     */
    public static long dirSize(Path path) throws IgniteCheckedException {
        final AtomicLong s = new AtomicLong(0);

        try {
            Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
                @Override public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                    s.addAndGet(attrs.size());

                    return FileVisitResult.CONTINUE;
                }

                @Override public FileVisitResult visitFileFailed(Path file, IOException exc) {
                    U.error(null, "file skipped - " + file, exc);

                    // Skip directory or file
                    return FileVisitResult.CONTINUE;
                }

                @Override public FileVisitResult postVisitDirectory(Path dir, IOException exc) {
                    if (exc != null)
                        U.error(null, "error during size calculation of directory - " + dir, exc);

                    // Ignoring
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
            throw new IgniteCheckedException("walkFileTree will not throw IOException if the FileVisitor does not");
        }

        return s.get();
    }

    /**
     * @param path Path.
     * @param name Name.
     */
    public static Path searchFileRecursively(Path path, @NotNull final String name) throws IgniteCheckedException {
        final AtomicReference<Path> res = new AtomicReference<>();

        try {
            Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
                @Override public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                    if (name.equals(file.getFileName().toString())) {
                        res.set(file);

                        return FileVisitResult.TERMINATE;
                    }

                    return FileVisitResult.CONTINUE;
                }

                @Override public FileVisitResult visitFileFailed(Path file, IOException exc) {
                    U.error(null, "file skipped during recursive search - " + file, exc);

                    // Ignoring.
                    return FileVisitResult.CONTINUE;
                }

                @Override public FileVisitResult postVisitDirectory(Path dir, IOException exc) {
                    if (exc != null)
                        U.error(null, "error during recursive search - " + dir, exc);

                    // Ignoring.
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
            throw new IgniteCheckedException("walkFileTree will not throw IOException if the FileVisitor does not");
        }

        return res.get();
    }

    /**
     * Returns {@link GridIntIterator} for range of primitive integers.
     * @param start Start.
     * @param cnt Count.
     */
    public static GridIntIterator forRange(final int start, final int cnt) {
        return new GridIntIterator() {
            int c = 0;

            @Override public boolean hasNext() {
                return c < cnt;
            }

            @Override public int next() {
                return start + c++;
            }
        };
    }

    /**
     * @param x X.
     */
    public static int nearestPow2(int x) {
        return nearestPow2(x, true);
    }

    /**
     * @param x X.
     * @param less Less.
     */
    public static int nearestPow2(int x, boolean less) {
        assert x > 0 : "can not calculate for less zero";

        long y = 1;

        while (y < x) {
            if (y * 2 > Integer.MAX_VALUE)
                return (int)y;

            y *= 2;
        }

        if (less)
            y /= 2;

        return (int)y;
    }

    /**
     * Puts additional text to thread name.
     * Calls {@code enhanceThreadName(Thread.currentThread(), text)}.
     * For details see {@link #enhanceThreadName(Thread, String)}.
     *
     * @param text Text to be set in thread name in square [] braces. Does nothing if cannot find suitable braces.
     */
    public static void enhanceThreadName(String text) {
        enhanceThreadName(Thread.currentThread(), text);
    }

    /**
     * Puts additional text to thread name. It finds first square braces in thread name and
     * sets required text in them. For example, thread has name "my-thread-[]-%gridname%". After
     * calling {@code enhanceThreadName(thread, "myText")}, the name of the thread will
     * be "my-thread-[myText]-%gridname%".<br>
     * This allows to set additional mutable info to thread, like remote host IP address or so.
     *
     * @param thread Thread to be renamed, it must contain square braces in name []. Does nothing if {@code null}.
     * @param text Text to be set in thread name in square [] braces. Does nothing if cannot find suitable braces.
     */
    public static void enhanceThreadName(@Nullable Thread thread, String text) {
        if (thread == null)
            return;

        String threadName = thread.getName();

        int idxStart = threadName.indexOf('[');
        int idxEnd = threadName.indexOf(']');

        if (idxStart < 0 || idxEnd < 0 || idxStart >= idxEnd)
            return;

        StringBuilder sb = new StringBuilder(threadName.length());

        sb.append(threadName, 0, idxStart + 1);
        sb.append(text);
        sb.append(threadName, idxEnd, threadName.length());

        thread.setName(sb.toString());
    }

    /**
     * @param ctx Context.
     *
     * @return instance of current baseline topology if it exists
     */
    public static BaselineTopology getBaselineTopology(@NotNull GridKernalContext ctx) {
        return ctx.state().clusterState().baselineTopology();
    }

    /**
     * @param cctx Context.
     *
     * @return instance of current baseline topology if it exists
     */
    public static BaselineTopology getBaselineTopology(@NotNull GridCacheSharedContext cctx) {
        return getBaselineTopology(cctx.kernalContext());
    }

    /**
     * @param cctx Context.
     *
     * @return instance of current baseline topology if it exists
     */
    public static BaselineTopology getBaselineTopology(@NotNull GridCacheContext cctx) {
        return getBaselineTopology(cctx.kernalContext());
    }

    /**
     * Check that node Ignite product version is not less then specified.
     *
     * @param ver Target Ignite product version.
     * @param nodes Cluster nodes.
     * @return {@code True} if ignite product version of all nodes is not less then {@code ver}.
     */
    public static boolean isOldestNodeVersionAtLeast(IgniteProductVersion ver, Iterable<ClusterNode> nodes) {
        for (ClusterNode node : nodes) {
            if (node.version().compareToIgnoreTimestamp(ver) < 0)
                return false;
        }

        return true;
    }

    /**
     * @param addr pointer in memory
     * @param len how much byte to read (should divide 8)
     *
     * @return hex representation of memory region
     */
    public static String toHexString(long addr, int len) {
        StringBuilder sb = new StringBuilder(len * 2);

        for (int i = 0; i < len; i++) // Can not use getLong because on little-endian it produces bs.
            addByteAsHex(sb, GridUnsafe.getByte(addr + i));

        return sb.toString();
    }

    /**
     * @param buf which content should be converted to string
     *
     * @return hex representation of memory region
     */
    public static String toHexString(ByteBuffer buf) {
        StringBuilder sb = new StringBuilder(buf.capacity() * 2);

        for (int i = 0; i < buf.capacity(); i++)
            addByteAsHex(sb, buf.get(i)); // Can not use getLong because on little-endian it produces bs.

        return sb.toString();
    }

    /**
     * @param ctx Kernel context.
     * @return Random alive server node.
     */
    public static ClusterNode randomServerNode(GridKernalContext ctx) {
        Collection<ClusterNode> aliveNodes = ctx.discovery().aliveServerNodes();

        int rndIdx = RND.nextInt(aliveNodes.size()) + 1;

        int i = 0;
        ClusterNode rndNode = null;

        for (Iterator<ClusterNode> it = aliveNodes.iterator(); i < rndIdx && it.hasNext(); i++)
            rndNode = it.next();

        if (rndNode == null)
            assert rndNode != null;

        return rndNode;
    }

    /**
     * @param ctx Kernal context.
     * @param plc IO Policy.
     * @param reserved Thread to reserve.
     * @return Number of available threads in executor service for {@code plc}. If {@code plc}
     *         is invalid, return {@code 1}.
     */
    public static int availableThreadCount(GridKernalContext ctx, byte plc, int reserved) {
        IgniteConfiguration cfg = ctx.config();

        int parallelismLvl;

        switch (plc) {
            case GridIoPolicy.P2P_POOL:
                parallelismLvl = cfg.getPeerClassLoadingThreadPoolSize();

                break;

            case GridIoPolicy.SYSTEM_POOL:
                parallelismLvl = cfg.getSystemThreadPoolSize();

                break;

            case GridIoPolicy.PUBLIC_POOL:
                parallelismLvl = cfg.getPublicThreadPoolSize();

                break;

            case GridIoPolicy.MANAGEMENT_POOL:
                parallelismLvl = cfg.getManagementThreadPoolSize();

                break;

            case GridIoPolicy.UTILITY_CACHE_POOL:
                parallelismLvl = cfg.getUtilityCacheThreadPoolSize();

                break;

            case GridIoPolicy.SERVICE_POOL:
                parallelismLvl = cfg.getServiceThreadPoolSize();

                break;

            case GridIoPolicy.DATA_STREAMER_POOL:
                parallelismLvl = cfg.getDataStreamerThreadPoolSize();

                break;

            case GridIoPolicy.QUERY_POOL:
                parallelismLvl = cfg.getQueryThreadPoolSize();

                break;

            default:
                parallelismLvl = -1;
        }

        return Math.max(1, parallelismLvl - reserved);
    }

    /**
     * Execute operation on data in parallel.
     *
     * @param executorSvc Service for parallel execution.
     * @param srcDatas List of data for parallelization.
     * @param operation Logic for execution of on each item of data.
     * @param <T> Type of data.
     * @throws IgniteCheckedException if parallel execution was failed.
     */
    public static <T, R> Collection<R> doInParallel(
        ExecutorService executorSvc,
        Collection<T> srcDatas,
        IgniteThrowableFunction<T, R> operation
    ) throws IgniteCheckedException, IgniteInterruptedCheckedException {
        return doInParallel(srcDatas.size(), executorSvc, srcDatas, operation);
    }

    /**
     * Execute operation on data in parallel.
     *
     * @param parallelismLvl Number of threads on which it should be executed.
     * @param executorSvc Service for parallel execution.
     * @param srcDatas List of data for parallelization.
     * @param operation Logic for execution of on each item of data.
     * @param <T> Type of data.
     * @param <R> Type of return value.
     * @throws IgniteCheckedException if parallel execution was failed.
     */
    public static <T, R> Collection<R> doInParallel(
        int parallelismLvl,
        ExecutorService executorSvc,
        Collection<T> srcDatas,
        IgniteThrowableFunction<T, R> operation
    ) throws IgniteCheckedException, IgniteInterruptedCheckedException {
        return doInParallel(parallelismLvl, executorSvc, srcDatas, operation, false);
    }

    /**
     * Execute operation on data in parallel uninterruptibly.
     *
     * @param parallelismLvl Number of threads on which it should be executed.
     * @param executorSvc Service for parallel execution.
     * @param srcDatas List of data for parallelization.
     * @param operation Logic for execution of on each item of data.
     * @param <T> Type of data.
     * @param <R> Type of return value.
     * @throws IgniteCheckedException if parallel execution was failed.
     */
    public static <T, R> Collection<R> doInParallelUninterruptibly(
        int parallelismLvl,
        ExecutorService executorSvc,
        Collection<T> srcDatas,
        IgniteThrowableFunction<T, R> operation
    ) throws IgniteCheckedException, IgniteInterruptedCheckedException {
        return doInParallel(parallelismLvl, executorSvc, srcDatas, operation, true);
    }

    /**
     * Execute operation on data in parallel.
     *
     * @param parallelismLvl Number of threads on which it should be executed.
     * @param executorSvc Service for parallel execution.
     * @param srcDatas List of data for parallelization.
     * @param operation Logic for execution of on each item of data.
     * @param <T> Type of data.
     * @param <R> Type of return value.
     * @param uninterruptible {@code true} if a result should be awaited in any case.
     * @throws IgniteCheckedException if parallel execution was failed.
     */
    private static <T, R> Collection<R> doInParallel(
        int parallelismLvl,
        ExecutorService executorSvc,
        Collection<T> srcDatas,
        IgniteThrowableFunction<T, R> operation,
        boolean uninterruptible
    ) throws IgniteCheckedException, IgniteInterruptedCheckedException {
        if (srcDatas.isEmpty())
            return Collections.emptyList();

        int[] batchSizes = calculateOptimalBatchSizes(parallelismLvl, srcDatas.size());

        List<Batch<T, R>> batches = new ArrayList<>(batchSizes.length);

        // Set for sharing batches between executor and current thread.
        // If executor cannot perform immediately, we will execute task in the current thread.
        Set<Batch<T, R>> sharedBatchesSet = new GridConcurrentHashSet<>(batchSizes.length);

        Iterator<T> iterator = srcDatas.iterator();

        for (int idx = 0; idx < batchSizes.length; idx++) {
            int batchSize = batchSizes[idx];

            Batch<T, R> batch = new Batch<>(batchSize, uninterruptible);

            for (int i = 0; i < batchSize; i++)
                batch.addTask(iterator.next());

            batches.add(batch);
        }

        batches = batches.stream()
            .filter(batch -> !batch.tasks.isEmpty())
            // Add to set only after check that batch is not empty.
            .peek(sharedBatchesSet::add)
            // Setup future in batch for waiting result.
            .peek(batch -> batch.fut = executorSvc.submit(() -> {
                // Batch was stolen by the main stream.
                if (!sharedBatchesSet.remove(batch))
                    return null;

                Collection<R> results = new ArrayList<>(batch.tasks.size());

                for (T item : batch.tasks)
                    results.add(operation.apply(item));

                return results;
            }))
            .collect(Collectors.toList());

        Throwable error = null;

        // Stealing jobs if executor is busy and cannot process task immediately.
        // Perform batches in a current thread.
        for (Batch<T, R> batch : sharedBatchesSet) {
            // Executor steal task.
            if (!sharedBatchesSet.remove(batch))
                continue;

            Collection<R> res = new ArrayList<>(batch.tasks.size());

            try {
                for (T item : batch.tasks)
                    res.add(operation.apply(item));

                batch.result(res);
            }
            catch (Throwable e) {
                batch.result(e);
            }
        }

        // Final result collection.
        Collection<R> results = new ArrayList<>(srcDatas.size());

        for (Batch<T, R> batch : batches) {
            try {
                Throwable err = batch.error;

                if (err != null) {
                    error = addSuppressed(error, err);

                    continue;
                }

                Collection<R> res = batch.result();

                if (res != null)
                    results.addAll(res);
                else
                    assert error != null;
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();

                throw new IgniteInterruptedCheckedException(e);
            }
            catch (ExecutionException e) {
                error = addSuppressed(error, e.getCause());
            }
            catch (CancellationException e) {
                error = addSuppressed(error, e);
            }
        }

        if (error != null) {
            if (error instanceof IgniteCheckedException)
                throw (IgniteCheckedException)error;

            if (error instanceof RuntimeException)
                throw (RuntimeException)error;

            if (error instanceof Error)
                throw (Error)error;

            throw new IgniteCheckedException(error);
        }

        return results;
    }

    /**
     * Utility method to add the given throwable error to the given throwable root error. If the given
     * suppressed throwable is an {@code Error}, but the root error is not, will change the root to the {@code Error}.
     *
     * @param root Root error to add suppressed error to.
     * @param err Error to add.
     * @return New root error.
     */
    private static Throwable addSuppressed(Throwable root, Throwable err) {
        assert err != null;

        if (root == null)
            return err;

        if (err instanceof Error && !(root instanceof Error)) {
            err.addSuppressed(root);

            root = err;
        }
        else
            root.addSuppressed(err);

        return root;
    }

    /**
     * @return {@code true} if local node is coordinator.
     */
    public static boolean isLocalNodeCoordinator(GridDiscoveryManager discoMgr) {
        if (discoMgr.localNode().isClient() || discoMgr.localNode().isDaemon())
            return false;

        DiscoverySpi spi = discoMgr.getInjectedDiscoverySpi();

        return spi instanceof TcpDiscoverySpi
            ? ((TcpDiscoverySpi)spi).isLocalNodeCoordinator()
            : F.eq(discoMgr.localNode(), U.oldest(discoMgr.aliveServerNodes(), null));
    }

    /**
     * The batch of tasks with a batch index in global array.
     */
    private static class Batch<T, R> {
        /** List tasks. */
        private final List<T> tasks;

        /** */
        private Collection<R> result;

        /** */
        private Throwable error;

        /** */
        private Future<Collection<R>> fut;

        /** */
        private final boolean uninterruptible;

        /**
         * @param batchSize Batch size.
         * @param uninterruptible {@code true} if a result should be awaited in any case.
         */
        private Batch(int batchSize, boolean uninterruptible) {
            tasks = new ArrayList<>(batchSize);

            this.uninterruptible = uninterruptible;
        }

        /**
         * @param task Add task.
         */
        public void addTask(T task) {
            tasks.add(task);
        }

        /**
         * @param res Setup results for tasks.
         */
        public void result(Collection<R> res) {
            result = res;
        }

        /**
         * @param e Throwable if task was completed with error.
         */
        public void result(Throwable e) {
            this.error = e;
        }

        /**
         * Get tasks results.
         */
        public Collection<R> result() throws ExecutionException, InterruptedException {
            assert fut != null;

            if (result != null)
                return result;

            return uninterruptible ? getUninterruptibly(fut) : fut.get();
        }
    }

    /**
     * Split number of tasks into optimized batches.
     * @param parallelismLvl Level of parallelism.
     * @param size number of tasks to split.
     * @return array of batch sizes.
     */
    public static int[] calculateOptimalBatchSizes(int parallelismLvl, int size) {
        int[] batcheSizes = new int[Math.min(parallelismLvl, size)];

        for (int i = 0; i < size; i++)
            batcheSizes[i % batcheSizes.length]++;

        return batcheSizes;
    }

    /**
     * @param fut Future to wait for completion.
     * @throws ExecutionException If the future
     */
    private static <R> R getUninterruptibly(Future<R> fut) throws ExecutionException {
        boolean interrupted = false;

        try {
            while (true) {
                try {
                    return fut.get();
                }
                catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        }
        finally {
            if (interrupted)
                Thread.currentThread().interrupt();
        }
    }

    /**
     *
     * @param r Runnable.
     * @param fut Grid future apater.
     * @return Runnable with wrapped future.
     */
    public static Runnable wrapIgniteFuture(Runnable r, GridFutureAdapter<?> fut) {
        return () -> {
            try {
                r.run();

                fut.onDone();
            }
            catch (Throwable e) {
                fut.onDone(e);

                throw e;
            }
        };
    }

    /**
     * @param key Cipher Key.
     * @param encMode Enc mode see {@link Cipher#ENCRYPT_MODE}, {@link Cipher#DECRYPT_MODE}, etc.
     */
    public static Cipher createCipher(Key key, int encMode) {
        if (key == null)
            throw new IgniteException("Cipher Key cannot be null");

        try {
            Cipher cipher = Cipher.getInstance(key.getAlgorithm());

            cipher.init(encMode, key);

            return cipher;
        }
        catch (NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeyException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * Special wrapper over address that can be sorted in following order:
     *     IPv4, private IPv4, IPv4 local host, IPv6.
     *     Lower addresses first.
     */
    private static class SortableAddress implements Comparable<SortableAddress> {
        /**
         *
         */
        private int type;

        /**
         *
         */
        private BigDecimal bits;

        /**
         *
         */
        private String addr;

        /**
         * Constructor.
         *
         * @param addr Address as string.
         */
        private SortableAddress(String addr) {
            this.addr = addr;

            if (addr.indexOf(':') > 0)
                type = 4; // IPv6
            else {
                try {
                    InetAddress inetAddr = InetAddress.getByName(addr);

                    if (inetAddr.isLoopbackAddress())
                        type = 3;  // localhost
                    else if (inetAddr.isSiteLocalAddress())
                        type = 2;  // private IPv4
                    else
                        type = 1; // other IPv4
                }
                catch (UnknownHostException ignored) {
                    type = 5;
                }
            }

            bits = BigDecimal.valueOf(0L);

            try {
                String[] octets = addr.contains(".") ? addr.split(".") : addr.split(":");

                int len = octets.length;

                for (int i = 0; i < len; i++) {
                    long oct = F.isEmpty(octets[i]) ? 0 : Long.valueOf(octets[i]);
                    long pow = Double.valueOf(Math.pow(256, octets.length - 1 - i)).longValue();

                    bits = bits.add(BigDecimal.valueOf(oct * pow));
                }
            }
            catch (Exception ignore) {
                // No-op.
            }
        }

        /** {@inheritDoc} */
        @Override public int compareTo(@NotNull SortableAddress o) {
            return (type == o.type ? bits.compareTo(o.bits) : Integer.compare(type, o.type));
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            SortableAddress other = (SortableAddress)o;

            return addr != null ? addr.equals(other.addr) : other.addr == null;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return addr != null ? addr.hashCode() : 0;
        }

        /**
         * @return Address.
         */
        public String address() {
            return addr;
        }
    }

    /**
     *  Safely write buffer fully to blocking socket channel.
     *  Will throw assert if non blocking channel passed.
     *
     * @param sockCh WritableByteChannel.
     * @param buf Buffer.
     * @throws IOException IOException.
     */
    public static void writeFully(SocketChannel sockCh, ByteBuffer buf) throws IOException {
        int totalWritten = 0;

        assert sockCh.isBlocking() : "SocketChannel should be in blocking mode " + sockCh;

        while (buf.hasRemaining()) {
            int written = sockCh.write(buf);

            if (written < 0)
                throw new IOException("Error writing buffer to channel " +
                    "[written = " + written + ", buf " + buf + ", totalWritten = " + totalWritten + "]");

            totalWritten += written;
        }
    }

    /**
     * @return New identity hash set.
     */
    public static <X> Set<X> newIdentityHashSet() {
        return Collections.newSetFromMap(new IdentityHashMap<>());
    }

    /**
     * @param stripes Number of stripes.
     * @param grpId Group Id.
     * @param partId Partition Id.
     * @return Stripe idx.
     */
    public static int stripeIdx(int stripes, int grpId, int partId) {
        assert partId >= 0;

        return Math.abs((Math.abs(grpId) + partId)) % stripes;
    }

    /**
     * Check if flag set.
     *
     * @param flags Flags.
     * @param flag Flag.
     * @return {@code True} if set.
     */
    public static boolean isFlagSet(int flags, int flag) {
        return (flags & flag) == flag;
    }

    /**
     * Notifies provided {@code lsnrs} with the value {@code t}.
     *
     * @param t Consumed object.
     * @param lsnrs Listeners.
     * @param <T> Type of consumed object.
     */
    public static <T> void notifyListeners(T t, Collection<Consumer<T>> lsnrs, IgniteLogger log) {
        if (lsnrs == null)
            return;

        for (Consumer<T> lsnr : lsnrs) {
            try {
                lsnr.accept(t);
            }
            catch (Exception e) {
                U.warn(log, "Listener error", e);
            }
        }
    }

    /**
     * Stops workers from given collection and waits for their completion.
     *
     * @param workers Workers collection.
     * @param cancel Wheter should cancel workers.
     * @param log Logger.
     */
    public static void awaitForWorkersStop(
        Collection<GridWorker> workers,
        boolean cancel,
        @Nullable IgniteLogger log
    ) {
        for (GridWorker worker : workers) {
            try {
                if (cancel)
                    worker.cancel();

                worker.join();
            }
            catch (Exception e) {
                if (log != null)
                    log.warning("Failed to cancel grid runnable [" + worker.toString() + "]: " + e.getMessage());
            }
        }
    }

    /**
     * Unquote the given string.
     * @param s String.
     * @return Unquoted string.
     */
    public static String unquote(String s) {
        return s == null ? null : s.replaceAll("^\"|\"$", "");
    }

    /**
     * Utility method for parsing strings like '10g', '200m', '1000k' as gigabytes, megabytes and kilobytes.
     * Plain numbers are parsed as number of bytes. Numbers followed by the '%' sign are parsed as a
     * percent of the max heap size.
     *
     * @param bytesStr String to be parsed.
     * @return Number of bytes.
     */
    public static long parseBytes(String bytesStr) {
        bytesStr = bytesStr.trim();

        if (bytesStr.matches("-?[0-9]+")) // Plain number.
            return Long.parseLong(bytesStr);
        else if (bytesStr.matches("-?[0-9]+[kK]")) // Kilobytes.
            return Long.parseLong(bytesStr.replaceAll("[^-0-9]", "")) * KB;
        else if (bytesStr.matches("-?[0-9]+[mM]")) // Megabytes.
            return Long.parseLong(bytesStr.replaceAll("[^-0-9]", "")) * MB;
        else if (bytesStr.matches("-?[0-9]+[gG]")) // Gigabytes.
            return Long.parseLong(bytesStr.replaceAll("[^-0-9]", "")) * GB;
        else if (bytesStr.matches("-?[0-9]+%")) { // Percent of heap.
            long percent = Long.parseLong(bytesStr.replaceAll("[^-0-9]", ""));

            if (percent < 0 || percent > 100) {
                throw new IllegalArgumentException("The percentage should be in the range from 0 to 100, but was: " +
                    percent);
            }

            return (long) (percent / 100.0 * Runtime.getRuntime().maxMemory());
        }
        else
            throw new IllegalArgumentException("Wrong format of bytes string. It is expected to be a number or " +
                "a number followed by one of the symbols: 'k', 'm', 'g', '%'.\n " +
                "For example: '10000', '10k', '33m', '2G'. But was: " + bytesStr);
    }

    /**
     * Writes string to output stream accounting for {@code null} values. <br/>
     *
     * This method can write string of any length, no {@link #UTF_BYTE_LIMIT} limits are applied.
     *
     * @param out Output stream to write to.
     * @param s String to write, possibly {@code null}.
     * @throws IOException If write failed.
     */
    public static void writeLongString(DataOutput out, @Nullable String s) throws IOException {
        // Write null flag.
        out.writeBoolean(isNull(s));

        if (isNull(s))
            return;

        int sLen = s.length();

        // Write string length.
        out.writeInt(sLen);

        // Write byte array.
        for (int i = 0; i < sLen; i++) {
            char c = s.charAt(i);
            int utfBytes = utfBytes(c);

            if (utfBytes == 1)
                out.writeByte((byte)c);
            else if (utfBytes == 3) {
                out.writeByte((byte)(0xE0 | (c >> 12) & 0x0F));
                out.writeByte((byte)(0x80 | (c >> 6) & 0x3F));
                out.writeByte((byte)(0x80 | (c & 0x3F)));
            }
            else {
                out.writeByte((byte)(0xC0 | ((c >> 6) & 0x1F)));
                out.writeByte((byte)(0x80 | (c & 0x3F)));
            }
        }
    }

    /**
     * Heuristically checks whether a string is long (greater than {@link #UTF_BYTE_LIMIT}) for
     * {@link DataOutput#writeUTF} (or {@link #writeString(DataOutput, String)}).
     *
     * <p>Long string can be written using {@link #writeLongString(DataOutput, String)}
     * or {@link #writeCutString(DataOutput, String)}.</p>
     *
     * @param s String to check.
     * @return {@code True} if the string is heuristically long.
     */
    public static boolean isStringTooLongForWriteHeuristically(String s) {
        return s.length() > UTF_BYTE_LIMIT / MAX_UTF_BYTES;
    }

    /**
     * Reads string from input stream accounting for {@code null} values. <br/>
     *
     * This method can read string of any length, no {@link #UTF_BYTE_LIMIT} limits are applied.
     *
     * @param in Stream to read from.
     * @return Read string, possibly {@code null}.
     * @throws IOException If read failed.
     */
    @Nullable public static String readLongString(DataInput in) throws IOException {
        // Check null value.
        if (in.readBoolean())
            return null;

        // Read string length.
        int sLen = in.readInt();

        StringBuilder strBuilder = new StringBuilder(sLen);

        // Read byte array.
        for (int i = 0, b0, b1, b2; i < sLen; i++) {
            b0 = in.readByte() & 0xff;

            switch (b0 >> 4) {
                case 0:
                case 1:
                case 2:
                case 3:
                case 4:
                case 5:
                case 6:
                case 7:   // 1 byte format: 0xxxxxxx
                    strBuilder.append((char)b0);
                    break;

                case 12:
                case 13:  // 2 byte format: 110xxxxx 10xxxxxx
                    b1 = in.readByte();

                    if ((b1 & 0xC0) != 0x80)
                        throw new UTFDataFormatException();

                    strBuilder.append((char)(((b0 & 0x1F) << 6) | (b1 & 0x3F)));
                    break;

                case 14:  // 3 byte format: 1110xxxx 10xxxxxx 10xxxxxx
                    b1 = in.readByte();
                    b2 = in.readByte();

                    if ((b1 & 0xC0) != 0x80 || (b2 & 0xC0) != 0x80)
                        throw new UTFDataFormatException();

                    strBuilder.append((char)(((b0 & 0x0F) << 12) | ((b1 & 0x3F) << 6) | (b2 & 0x3F)));
                    break;

                default:  // 10xx xxxx, 1111 xxxx
                    throw new UTFDataFormatException();
            }
        }

        return strBuilder.toString();
    }

    /**
     * Writes string to output stream accounting for {@code null} values. <br/>
     *
     * <p>
     *     Uses {@link ObjectOutputStream#writeUTF(String)} to write the string under the hood
     *     but cuts strings longer than {@link #UTF_BYTE_LIMIT} to the limit to avoid {@link UTFDataFormatException}.
     * </p>
     *
     * <p>
     *     Strings written by the method can be read by {@link #readString(DataInput)}.
     * </p>
     *
     * @see #writeString(DataOutput, String) for more information.
     *
     * @param out Output stream to write to.
     * @param s String to write, possibly {@code null}.
     * @throws IOException If write failed.
     */
    public static void writeCutString(DataOutput out, @Nullable String s) throws IOException {
        // Write null flag.
        out.writeBoolean(isNull(s));

        if (isNull(s))
            return;

        //Conversion of string to limit.
        for (int i = 0, bs = 0; i < s.length(); i++) {
            if ((bs += utfBytes(s.charAt(i))) > UTF_BYTE_LIMIT) {
                s = s.substring(0, i);
                break;
            }
        }

        out.writeUTF(s);
    }

    /**
     * Get number of bytes for {@link DataOutput#writeUTF},
     * depending on character: <br/>
     *
     * One byte - If a character <code>c</code> is in the range
     * <code>&#92;u0001</code> through <code>&#92;u007f</code>.<br/>
     *
     * Two bytes - If a character <code>c</code> is <code>&#92;u0000</code> or
     * is in the range <code>&#92;u0080</code> through <code>&#92;u07ff</code>.
     * <br/>
     *
     * Three bytes - If a character <code>c</code> is in the range
     * <code>&#92;u0800</code> through <code>uffff</code>.
     *
     * @param c Character.
     * @return Number of bytes.
     */
    public static int utfBytes(char c) {
        return (c >= 0x0001 && c <= 0x007F) ? 1 : (c > 0x07FF) ? MAX_UTF_BYTES : 2;
    }

    /**
     * Broadcasts given job to nodes that match filter.
     *
     * @param kctx Kernal context.
     * @param job Ignite job.
     * @param srvrsOnly Broadcast only on server nodes.
     * @param nodeFilter Node filter.
     */
    public static IgniteFuture<Void> broadcastToNodesWithFilterAsync(
        GridKernalContext kctx,
        IgniteRunnable job,
        boolean srvrsOnly,
        IgnitePredicate<ClusterNode> nodeFilter
    ) {
        ClusterGroup cl = kctx.grid().cluster();

        if (srvrsOnly)
            cl = cl.forServers();

        ClusterGroup grp = nodeFilter != null ? cl.forPredicate(nodeFilter) : cl;

        if (grp.nodes().isEmpty())
            return new IgniteFinishedFutureImpl<>();

        IgniteCompute compute = kctx.grid().compute(grp);

        return compute.broadcastAsync(job);
    }

    /**
     * Reads string-to-string map written by {@link #writeStringMap(DataOutput, Map)}.
     *
     * @param in Data input.
     * @throws IOException If write failed.
     * @return Read result.
     */
    public static Map<String, String> readStringMap(DataInput in) throws IOException {
        int size = in.readInt();

        if (size == -1)
            return null;
        else {
            Map<String, String> map = U.newHashMap(size);

            for (int i = 0; i < size; i++)
                map.put(readUTF(in), readUTF(in));

            return map;
        }
    }

    /**
     * Writes string-to-string map to given data output.
     *
     * @param out Data output.
     * @param map Map.
     * @throws IOException If write failed.
     */
    public static void writeStringMap(DataOutput out, @Nullable Map<String, String> map) throws IOException {
        if (map != null) {
            out.writeInt(map.size());

            for (Map.Entry<String, String> e : map.entrySet()) {
                writeUTF(out, e.getKey());
                writeUTF(out, e.getValue());
            }
        }
        else
            out.writeInt(-1);
    }

    /** Maximum string length to be written at once. */
    private static final int MAX_STR_LEN = 0xFFFF / 4;

    /**
     * Write UTF string which can be {@code null}.
     *
     * @param out Output stream.
     * @param val Value.
     * @throws IOException If failed.
     */
    public static void writeUTF(DataOutput out, @Nullable String val) throws IOException {
        if (val == null)
            out.writeInt(-1);
        else {
            out.writeInt(val.length());

            if (val.length() <= MAX_STR_LEN)
                out.writeUTF(val); // Optimized write in 1 chunk.
            else {
                int written = 0;

                while (written < val.length()) {
                    int partLen = Math.min(val.length() - written, MAX_STR_LEN);

                    String part = val.substring(written, written + partLen);

                    out.writeUTF(part);

                    written += partLen;
                }
            }
        }
    }

    /**
     * Read UTF string which can be {@code null}.
     *
     * @param in Input stream.
     * @return Value.
     * @throws IOException If failed.
     */
    public static String readUTF(DataInput in) throws IOException {
        int len = in.readInt(); // May be zero.

        if (len < 0)
            return null;
        else {
            if (len <= MAX_STR_LEN)
                return in.readUTF();

            StringBuilder sb = new StringBuilder(len);

            do {
                sb.append(in.readUTF());
            }
            while (sb.length() < len);

            assert sb.length() == len;

            return sb.toString();
        }
    }

    /** Explicit class for {@code Supplier<Enumeration<NetworkInterface>>}. */
    @FunctionalInterface
    public interface InterfaceSupplier {
        /** Return collection of local network interfaces. */
        Enumeration<NetworkInterface> getInterfaces() throws SocketException;
    }

    /**
     * @param configuration Ignite configuration.
     * @return Whether persistence is enabled.
     */
    public static boolean persistenceEnabled(IgniteConfiguration configuration) {
        if (configuration.getDataStorageConfiguration() != null) {
            DataStorageConfiguration dsCfg = configuration.getDataStorageConfiguration();
            if (dsCfg.getDefaultDataRegionConfiguration() != null && dsCfg.getDefaultDataRegionConfiguration().isPersistenceEnabled())
                return true;

            DataRegionConfiguration[] regions = dsCfg.getDataRegionConfigurations();

            if (regions != null) {
                for (DataRegionConfiguration region : regions) {
                    if (region.isPersistenceEnabled())
                        return true;
                }
            }
        }

        return false;
    }

    /**
     * Converts count of bytes to a human-readable format.
     * Examples: 10 -> 10.0 B, 2048 -> 2.0 KB, etc.
     *
     * @param bytes Byte count.
     * @return Human readable format for count of bytes.
     */
    public static String humanReadableByteCount(long bytes) {
        long base = 1024L;

        int exponent = max((int)(Math.log(bytes) / Math.log(base)), 0);
        String unit = String.valueOf(BYTE_CNT_PREFIXES.charAt(exponent)).trim();

        return String.format((Locale)null, "%.1f %sB", (bytes / Math.pow(base, exponent)), unit);
    }

    /**
     * Converts duration to a human-readable format.
     * Examples: 10 -> 10ms, 6_0000 -> 6s, 65_000 -> 1m5s, (65 * 60_000 + 32_000) -> 1h5m32s, etc.
     *
     * @param millis Duration in milliseconds.
     * @return Human readable format for duration.
     */
    public static String humanReadableDuration(long millis) {
        StringBuilder sb = new StringBuilder();

        if (millis < 0) {
            sb.append('-');

            millis = -millis;
        }

        if (millis < 1_000)
            sb.append(millis).append("ms");
        else {
            long days = TimeUnit.MILLISECONDS.toDays(millis);

            if (days > 0) {
                sb.append(days).append('d');

                millis -= TimeUnit.DAYS.toMillis(days);
            }

            long hours = TimeUnit.MILLISECONDS.toHours(millis);

            if (hours > 0) {
                sb.append(hours).append('h');

                millis -= TimeUnit.HOURS.toMillis(hours);
            }

            long minutes = TimeUnit.MILLISECONDS.toMinutes(millis);

            if (minutes > 0) {
                sb.append(minutes).append('m');

                millis -= TimeUnit.MINUTES.toMillis(minutes);
            }

            long seconds = TimeUnit.MILLISECONDS.toSeconds(millis);

            if (seconds > 0)
                sb.append(seconds).append('s');
        }

        return sb.toString();
    }

    /**
     * @param ctx Kernel context.
     * @param depInfo Deployment info.
     * @param clsName Class name.
     * @return Whether the {@code depObj} is valid for the {@code clsName} in the current context.
     */
    public static boolean validateDeploymentInfo(GridKernalContext ctx, GridDeploymentInfo depInfo, String clsName) {
        GridDeployment dep = ctx.deploy().getDeployment(clsName);

        return dep == null || dep.classLoaderId().equals(depInfo.classLoaderId());
    }

    /**
     * Getting the total size of uncompressed data in zip.
     *
     * @param zip Zip file.
     * @return Total uncompressed size.
     * @throws IOException If failed.
     */
    public static long uncompressedSize(File zip) throws IOException {
        try (ZipFile zipFile = new ZipFile(zip)) {
            long size = 0;

            Enumeration<? extends ZipEntry> entries = zipFile.entries();

            while (entries.hasMoreElements())
                size += entries.nextElement().getSize();

            return size;
        }
    }

    /**
     * @param enabled Parameter.
     * @return Returns "enabled" or "disabled" string.
     */
    public static String enabledString(boolean enabled) {
        return enabled ? "enabled" : "disabled";
    }

    /**
     * Maps object hash to some index between 0 and specified size via modulo operation.
     *
     * @param hash Object hash.
     * @param size Size greater than 0.
     * @return Calculated index in range [0..size).
     */
    public static int hashToIndex(int hash, int size) {
        return safeAbs(hash % size);
    }

    /**
     * Invokes {@link ServerSocket#accept()} method on the passed server socked, working around the
     * https://bugs.openjdk.java.net/browse/JDK-8247750 in the process.
     *
     * @param srvrSock Server socket.
     * @return New socket.
     * @throws IOException If an I/O error occurs when waiting for a connection.
     * @see ServerSocket#accept()
     */
    public static Socket acceptServerSocket(ServerSocket srvrSock) throws IOException {
        while (true) {
            try {
                return srvrSock.accept();
            }
            catch (SocketTimeoutException e) {
                if (srvrSock.getSoTimeout() > 0)
                    throw e;
            }
        }
    }

    /**
     * Returns {@code true} if class is a lambda.
     *
     * @param objectClass Class.
     * @return {@code true} if class is a lambda, {@code false} otherwise.
     */
    public static boolean isLambda(Class<?> objectClass) {
        return !objectClass.isPrimitive() && !objectClass.isArray()
            // Order is crucial here, isAnonymousClass and isLocalClass may fail if
            // class' outer class was loaded with different classloader.
            && objectClass.isSynthetic()
            && !objectClass.isAnonymousClass() && !objectClass.isLocalClass()
            && classCannotBeLoadedByName(objectClass);
    }

    /**
     * Returns {@code true} if class can not be loaded by name.
     *
     * @param objectClass Class.
     * @return {@code true} if class can not be loaded by name, {@code false} otherwise.
     */
    public static boolean classCannotBeLoadedByName(Class<?> objectClass) {
        try {
            Class.forName(objectClass.getName());
            return false;
        }
        catch (ClassNotFoundException e) {
            return true;
        }
    }

    /**
     * Resets cached local addresses and host names.
     */
    @TestOnly
    public static void resetCachedLocalAddressAndHostNames() {
        cachedLocalAddr = null;
        cachedLocalAddrAllHostNames = null;
    }

    /**
     * Key value for logging.
     */
    public static String resolveKey(GridCacheContext cctx, KeyCacheObject key) {
        GridToStringBuilder.SensitiveDataLogging sensitiveDataLogging = S.getSensitiveDataLogging();
        String keyValue = "nil";

        if (sensitiveDataLogging == GridToStringBuilder.SensitiveDataLogging.PLAIN) {
            keyValue = key.value(cctx.cacheObjectContext(), false);
        } else if (sensitiveDataLogging == GridToStringBuilder.SensitiveDataLogging.HASH) {
            keyValue = String.valueOf(IgniteUtils.hash(key));
        }
        return keyValue;
    }

    /**
     * Key values for logging from conflict map.
     */
    public static String resolveKey(GridCacheContext cctx, Set<KeyCacheObject> keySet) {
        GridToStringBuilder.SensitiveDataLogging sensitiveDataLogging = S.getSensitiveDataLogging();
        String keyValue = "nil";

        if (sensitiveDataLogging == GridToStringBuilder.SensitiveDataLogging.PLAIN) {
            List<String> collect = keySet.stream()
                    .map(x -> Objects.requireNonNull(x.value(cctx.cacheObjectContext(), false)).toString())
                    .collect(Collectors.toList());
            keyValue = String.join(", ",collect);
        } else if (sensitiveDataLogging == GridToStringBuilder.SensitiveDataLogging.HASH) {
            List<String> collect = keySet.stream()
                    .map(x -> Objects.requireNonNull(String.valueOf(IgniteUtils.hash(x))))
                    .collect(Collectors.toList());
            keyValue = String.join(", ",collect);;
        }
        return keyValue;
    }
}
