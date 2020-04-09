/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.spi.discovery.tcp.ipfinder.vm;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**  */
public class TcpDiscoveryVmIpFinderDnsResolveTest extends GridCommonAbstractTest {

    /** Fqnd */
    private static String FQDN = "test.domain";

    /** Multiple Fqnd */
    private static String MULTI_FQDN = "multi.test.domain";

    /** Incorrect fqnd */
    private static String BAD_FQDN = "bad.domain";

    /** local host address */
    private static String LOCAL_HOST = "localhost";

    /** Ip1 */
    private static String IP1 = "10.0.0.1";

    /** Ip2 */
    private static String IP2 = "10.0.0.2";

    /** DNS service */
    private static TwoIpRoundRobinDnsService hostNameSvc;

    /** original DNS */
    private static Object nameSvc;

    /**
     */
    @BeforeClass
    public static void before() throws Exception {
        hostNameSvc = new TwoIpRoundRobinDnsService(FQDN, MULTI_FQDN, IP1, IP2);

        INameService.install(hostNameSvc);
    }

    /**
     */
    @AfterClass
    public static void cleanup() throws Exception {
        INameService.uninstall();
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testFqdnResolveWhenDnsCantResolveHostName() throws Exception {
        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();

        Set<String> addrs = new HashSet<>();

        addrs.add(BAD_FQDN);

        ipFinder.setAddresses(addrs);

        Collection<InetSocketAddress> resolved1 = ipFinder.getRegisteredAddresses();

        assertNotNull(resolved1);

        InetSocketAddress addr1 = resolved1.iterator().next();

        assertNull(addr1.getAddress());
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testFqdnResolveAfterDnsHostChange() throws Exception {
        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();

        Set<String> addrs = new HashSet<>();

        addrs.add(FQDN);

        ipFinder.setAddresses(addrs);

        Collection<InetSocketAddress> resolved1 = ipFinder.getRegisteredAddresses();

        InetSocketAddress addr1 = resolved1.iterator().next();

        //because of JAVA networkaddress cache ttl can be turn on.
        //will be great to change current test and run it in separate JVM.
        //and set there -Dsun.net.inetaddr.ttl=0 -Dsun.net.inetaddr.negative.ttl=0
        Thread.sleep(50_000);

        Collection<InetSocketAddress> resolved2 = ipFinder.getRegisteredAddresses();

        InetSocketAddress addr2 = resolved2.iterator().next();

        log.info("Adrrs1 - " + addr1.getAddress() + " Adrrs2 - " + addr2.getAddress());

        assertFalse("Addresses weren't resolved second time. Probably DNS cache has TTL more then 1 min, if yes " +
            "then please mute this test. Adrrs1 - " + addr1.getAddress() + " Adrrs2 - " + addr2.getAddress(),
            addr1.equals(addr2));
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testFqdnWithPortRangeResolveWithTwoIpRoundRobinDns() throws Exception {
        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();

        Set<String> addrs = new HashSet<>();

        addrs.add(FQDN + ":47500..47509");

        ipFinder.setAddresses(addrs);

        Collection<InetSocketAddress> resolved = ipFinder.getRegisteredAddresses();

        log.info("Resolved addresses are " + resolved);

        assertTrue(resolved.size() == 10);

        Iterator<InetSocketAddress> it = resolved.iterator();

        InetSocketAddress first = it.next();

        String ip = first.getAddress().getHostAddress();

        String host = first.getHostName();

        while (it.hasNext()) {
            InetSocketAddress curr = it.next();

            assertTrue("IP address isn't the same. ip - " + curr.getAddress().getHostAddress() + " expected " + ip,
                ip.equals(curr.getAddress().getHostAddress()));

            assertTrue("FQDN isn't the same. cur - " + curr.getHostName() + " expected " + host,
                host.equals(curr.getHostName()));
        }
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testFqdnResolveAfterDnsHostChangeWithRegisteredAddrs() throws Exception {
        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();

        Set<String> addrs = new HashSet<>();

        addrs.add(FQDN);

        ipFinder.setAddresses(addrs);

        Collection<InetSocketAddress> registerAddrs = new HashSet<>();

        registerAddrs.add(new InetSocketAddress(LOCAL_HOST, 0));

        ipFinder.registerAddresses(registerAddrs);

        Collection<InetSocketAddress> resolved = ipFinder.getRegisteredAddresses();

        assertTrue(resolved.size() == 2);

        log.info("Resolved addresses are " + resolved);

        Iterator<InetSocketAddress> it = resolved.iterator();

        InetSocketAddress addr1 = it.next();

        InetSocketAddress addr2 = it.next();

        assertNotNull(addr1);

        assertNotNull(addr2);

        assertTrue(FQDN.equals(addr1.getHostName()) || LOCAL_HOST.equals(addr1.getHostName()));

        assertTrue(FQDN.equals(addr2.getHostName()) || LOCAL_HOST.equals(addr2.getHostName()));

        assertFalse("Addresses are the same. Adrrs1 - " + addr1.getAddress() +
            " Adrrs2 - " + addr2.getAddress(), addr1.equals(addr2));
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testMultiFqdnResolveWithPortRange() throws Exception {
        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();

        Collection<String> addrs = new ArrayList<>();

        addrs.add(MULTI_FQDN + ":47500..47509");

        ipFinder.setAddresses(addrs);

        Collection<InetSocketAddress> resolved = ipFinder.getRegisteredAddresses();

        assertTrue(resolved.size() == 20);

        log.info("Resolved addresses are " + resolved);

        Iterator<InetSocketAddress> it = resolved.iterator();

        int cntIp1 = 0;

        int cntIp2 = 0;

        while (it.hasNext()) {
            InetSocketAddress curr = it.next();

            assertTrue(MULTI_FQDN.equals(curr.getHostName()));

            if (IP1.equals(curr.getAddress().getHostAddress()))
                cntIp1++;
            if (IP2.equals(curr.getAddress().getHostAddress()))
                cntIp2++;
        }

        assertTrue(cntIp1 == 10);

        assertTrue(cntIp2 == 10);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testMultiFqdnResolve() throws Exception {
        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();

        Collection<String> addrs = new ArrayList<>();

        addrs.add(MULTI_FQDN);

        ipFinder.setAddresses(addrs);

        Collection<InetSocketAddress> resolved = ipFinder.getRegisteredAddresses();

        assertTrue(resolved.size() == 2);

        log.info("Resolved addresses are " + resolved);

        Iterator<InetSocketAddress> it = resolved.iterator();

        InetSocketAddress addr1 = it.next();

        InetSocketAddress addr2 = it.next();

        assertNotNull(addr1);

        assertNotNull(addr2);

        assertTrue(MULTI_FQDN.equals(addr1.getHostName()) && MULTI_FQDN.equals(addr2.getHostName()));

        assertFalse("Addresses are the same. Adrrs1 - " + addr1.getAddress() +
            " Adrrs2 - " + addr2.getAddress(), addr1.equals(addr2));
    }

    /**
     * Custom hostname service.
     */
    @SuppressWarnings("restriction")
    public static class TwoIpRoundRobinDnsService implements INameService {
        /** ip1 */
        private final String ip1;

        /** ip2 */
        private final String ip2;

        /** fqdn */
        private final String fqdn;

        /** multiple fqdn */
        private final String multipleFqdn;

        /** change flag */
        private boolean needReturnIp1 = false;

        /** */
        public TwoIpRoundRobinDnsService(String fqdn, String multipleFqdn, String ip1, String ip2) {
            this.multipleFqdn = multipleFqdn;
            this.ip1 = ip1;
            this.ip2 = ip2;
            this.fqdn = fqdn;
        }

        /** {@inheritDoc} */
        @Override
        public InetAddress[] lookupAllHostAddr(String paramStr) throws UnknownHostException {
            if (fqdn.equals(paramStr)) {
                String ip = needReturnIp1 ? ip1 : ip2;

                needReturnIp1 = !needReturnIp1;

                final byte[] arrOfByte = sun.net.util.IPAddressUtil.textToNumericFormatV4(ip);

                final InetAddress addr = InetAddress.getByAddress(paramStr, arrOfByte);

                return new InetAddress[] {addr};
            }
            else if (multipleFqdn.equals(paramStr)) {
                final byte[] arrOfByte1 = sun.net.util.IPAddressUtil.textToNumericFormatV4(ip1);

                final byte[] arrOfByte2 = sun.net.util.IPAddressUtil.textToNumericFormatV4(ip2);

                final InetAddress addr1 = InetAddress.getByAddress(paramStr, arrOfByte1);

                final InetAddress addr2 = InetAddress.getByAddress(paramStr, arrOfByte2);

                return new InetAddress[] {addr1, addr2};
            }
            else
                throw new UnknownHostException();
        }

        /** {@inheritDoc} */
        @Override
        public String getHostByAddr(byte[] paramArrOfByte) throws UnknownHostException {
            throw new UnknownHostException();
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            TwoIpRoundRobinDnsService svc = (TwoIpRoundRobinDnsService)o;
            return needReturnIp1 == svc.needReturnIp1 &&
                Objects.equals(ip1, svc.ip1) &&
                Objects.equals(ip2, svc.ip2) &&
                Objects.equals(fqdn, svc.fqdn) &&
                Objects.equals(multipleFqdn, svc.multipleFqdn);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {

            return Objects.hash(ip1, ip2, fqdn, multipleFqdn, needReturnIp1);
        }
    }

    /** */
    public interface INameService extends InvocationHandler {
        /** */
        static void install(
            INameService dns) throws IllegalArgumentException, IllegalAccessException, NoSuchFieldException, SecurityException, ClassNotFoundException {
            final Class<?> inetAddrCls = InetAddress.class;

            Object neu;

            Field nameSvcField;

            try {
                //JAVA 9+ class
                final Class<?> iface = Class.forName("java.net.InetAddress$NameService");

                nameSvcField = inetAddrCls.getDeclaredField("nameService");

                neu = Proxy.newProxyInstance(iface.getClassLoader(), new Class<?>[] {iface}, dns);
            }
            catch (final ClassNotFoundException | NoSuchFieldException e) {
                //JAVA <8 class
                nameSvcField = inetAddrCls.getDeclaredField("nameServices");

                final Class<?> iface = Class.forName("sun.net.spi.nameservice.NameService");

                neu = Collections.singletonList(Proxy.newProxyInstance(iface.getClassLoader(), new Class<?>[] {iface}, dns));
            }

            nameSvcField.setAccessible(true);

            nameSvc = nameSvcField.get(inetAddrCls);

            nameSvcField.set(inetAddrCls, neu);
        }

        /** */
        static void uninstall() throws IllegalArgumentException, IllegalAccessException, NoSuchFieldException, SecurityException {
            final Class<?> inetAddrCls = InetAddress.class;

            Field nameSvcField;

            try {
                //JAVA 9+ class
                Class.forName("java.net.InetAddress$NameService");

                nameSvcField = inetAddrCls.getDeclaredField("nameService");
            }
            catch (final ClassNotFoundException | NoSuchFieldException e) {
                //JAVA <8 class
                nameSvcField = inetAddrCls.getDeclaredField("nameServices");
            }

            nameSvcField.setAccessible(true);

            nameSvcField.set(inetAddrCls, nameSvc);
        }

        /**
         * Lookup a host mapping by name. Retrieve the IP addresses associated with a host
         *
         * @param host the specified hostname
         * @return array of IP addresses for the requested host
         * @throws UnknownHostException if no IP address for the {@code host} could be found
         */
        InetAddress[] lookupAllHostAddr(final String host) throws UnknownHostException;

        /**
         * Lookup the host corresponding to the IP address provided
         *
         * @param addr byte array representing an IP address
         * @return {@code String} representing the host name mapping
         * @throws UnknownHostException if no host found for the specified IP address
         */
        String getHostByAddr(final byte[] addr) throws UnknownHostException;

        /** */
        @Override default Object invoke(final Object proxy, final Method method,
            final Object[] args) throws Throwable {
            switch (method.getName()) {
                case "lookupAllHostAddr":
                    return lookupAllHostAddr((String)args[0]);
                case "getHostByAddr":
                    return getHostByAddr((byte[])args[0]);
                default:
                    final StringBuilder o = new StringBuilder();

                    o.append(method.getReturnType().getCanonicalName() + " " + method.getName() + "(");

                    final Class<?>[] ps = method.getParameterTypes();

                    for (int i = 0; i < ps.length; ++i) {
                        if (i > 0)
                            o.append(", ");

                        o.append(ps[i].getCanonicalName()).append(" p").append(i);
                    }

                    o.append(")");

                    throw new UnsupportedOperationException(o.toString());
            }
        }
    }
}