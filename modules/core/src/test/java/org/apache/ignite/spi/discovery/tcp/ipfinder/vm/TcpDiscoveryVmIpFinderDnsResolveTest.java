package org.apache.ignite.spi.discovery.tcp.ipfinder.vm;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

public class TcpDiscoveryVmIpFinderDnsResolveTest extends GridCommonAbstractTest {

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        System.setProperty("networkaddress.cache.ttl", "0");
        System.setProperty("networkaddress.cache.negative.ttl", "0");

        System.setProperty("sun.net.inetaddr.ttl", "0");
        System.setProperty("sun.net.inetaddr.negative.ttl", "0");
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testPortReleasedAfterFailure() throws Exception {
        MyHostNameService hostNameService = new MyHostNameService("10.0.0.1", "test.domain");

        INameService.install(hostNameService);

        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();

        Set<String> addresses = new HashSet<>();

        addresses.add("test.domain");

        ipFinder.setAddresses(addresses);

        Collection<InetSocketAddress> resolved1 =  ipFinder.getRegisteredAddresses();

        InetSocketAddress addr1 = resolved1.iterator().next();

        hostNameService.setIp("10.0.0.2");

        INameService.install(hostNameService);

        Collection<InetSocketAddress> resolved2 =  ipFinder.getRegisteredAddresses();

        InetSocketAddress addr2 = resolved2.iterator().next();

        log.info("Adrrs1 - " + addr1.getAddress() + " Adrrs2 - " + addr2.getAddress());

        assertFalse("Addresses weren't resolved second time. Adrrs1 - " + addr1.getAddress() +
            " Adrrs2 - " + addr2.getAddress(), addr1.equals(addr2));

        assertEquals(addr1.getAddress().getHostAddress(),"10.0.0.1");

        assertEquals(addr2.getAddress().getHostAddress(),"10.0.0.2");

    }

    /**
     * Custom hostname service.
     */
    @SuppressWarnings("restriction")
    public static class MyHostNameService implements INameService {
        /** ip */
        private String ip;

        /** fqdn */
        private String fqdn;

        /** */
        public void setIp(String ip) {
            this.ip = ip;
        }

        /** */
        public void setFqdn(String fqdn) {
            this.fqdn = fqdn;
        }

        /** */
        public MyHostNameService(String ip, String fqdn) {
            this.ip = ip;
            this.fqdn = fqdn;
        }

        /** {@inheritDoc} */
        @Override
        public InetAddress[] lookupAllHostAddr(String paramString) throws UnknownHostException {
            if (fqdn.equals(paramString)) {
                final byte[] arrayOfByte = sun.net.util.IPAddressUtil.textToNumericFormatV4(ip);
                final InetAddress address = InetAddress.getByAddress(paramString, arrayOfByte);
                return new InetAddress[] { address };
            } else {
                throw new UnknownHostException();
            }
        }

        /** {@inheritDoc} */
        @Override
        public String getHostByAddr(byte[] paramArrayOfByte) throws UnknownHostException {
            throw new UnknownHostException();
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            MyHostNameService that = (MyHostNameService)o;
            return Objects.equals(ip, that.ip) &&
                Objects.equals(fqdn, that.fqdn);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {

            return Objects.hash(ip, fqdn);
        }
    }

    /** {@inheritDoc} */
    public interface INameService extends InvocationHandler {
        /** {@inheritDoc} */
        static void install(INameService dns) throws IllegalArgumentException, IllegalAccessException, NoSuchFieldException, SecurityException, ClassNotFoundException {
            final Class<?> inetAddressClass = InetAddress.class;

            Object neu;

            Field nameServiceField;

            try {
                //JAVA 9+ class
                final Class<?> iface = Class.forName("java.net.InetAddress$NameService");

                nameServiceField = inetAddressClass.getDeclaredField("nameService");

                neu = Proxy.newProxyInstance(iface.getClassLoader(), new Class<?>[] { iface }, dns);
            } catch(final ClassNotFoundException|NoSuchFieldException e) {
                //JAVA <8 class
                nameServiceField = inetAddressClass.getDeclaredField("nameServices");

                final Class<?> iface = Class.forName("sun.net.spi.nameservice.NameService");

                neu = Arrays.asList(Proxy.newProxyInstance(iface.getClassLoader(), new Class<?>[] { iface }, dns));
            }

            nameServiceField.setAccessible(true);

            nameServiceField.set(inetAddressClass, neu);
        }

        /**
         * Lookup a host mapping by name. Retrieve the IP addresses associated with a host
         *
         * @param host the specified hostname
         * @return array of IP addresses for the requested host
         * @throws UnknownHostException  if no IP address for the {@code host} could be found
         */
        InetAddress[] lookupAllHostAddr(final String host) throws UnknownHostException;

        /**
         * Lookup the host corresponding to the IP address provided
         *
         * @param addr byte array representing an IP address
         * @return {@code String} representing the host name mapping
         * @throws UnknownHostException
         *             if no host found for the specified IP address
         */
        String getHostByAddr(final byte[] addr) throws UnknownHostException;

        /** */
        @Override default public Object invoke(final Object proxy, final Method method, final Object[] args) throws Throwable {
            switch(method.getName()) {
                case "lookupAllHostAddr": return lookupAllHostAddr((String)args[0]);
                case "getHostByAddr"    : return getHostByAddr    ((byte[])args[0]);
                default                 :
                    final StringBuilder o = new StringBuilder();

                    o.append(method.getReturnType().getCanonicalName()+" "+method.getName()+"(");

                    final Class<?>[] ps = method.getParameterTypes();

                    for(int i=0;i<ps.length;++i) {
                        if(i>0)
                            o.append(", ");

                        o.append(ps[i].getCanonicalName()).append(" p").append(i);
                    }

                    o.append(")");

                    throw new UnsupportedOperationException(o.toString());
            }
        }
    }
}