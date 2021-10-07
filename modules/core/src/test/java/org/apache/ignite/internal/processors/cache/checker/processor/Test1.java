package org.apache.ignite.internal.processors.cache.checker.processor;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteClientDisconnectedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLock;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.discovery.IgniteDiscoverySpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

public class Test1 extends GridCommonAbstractTest {

    @Test
    public void testLock() throws Exception {
        IgniteEx srv = startGrid(0);

        IgniteEx client = startClientGrid(5);

        IgniteLock lock = client.reentrantLock("lock", true, false, true);

        lock.lock();
        lock.unlock();
        System.out.println("nodeid0 " + client.localNode().id());

        reconnectClient(client);
//        IgniteDiscoverySpi clientDiscSpi = (IgniteDiscoverySpi) client.configuration().getDiscoverySpi();
//        clientDiscSpi.clientReconnect();

//        try {
            lock.lock();
//        }
//        catch (IgniteClientDisconnectedException e) {
//            e.reconnectFuture().get();
//            lock.lock();
//        }
        lock.unlock();

        System.out.println("nodeid1 " + client.localNode().id());//после реконнекта меняется

        Thread thread0 = new Thread(() -> {
            lock.lock();
            System.out.println("T1 acquire lock");

//            try {
//                Thread.sleep(1000);
//            }
//            catch (Exception e) {
//                System.out.println("T1 sleep exception " + e.getClass());
//            }

            try {
                Thread.sleep(5000);

                lock.unlock();

                System.out.println("T1 unlock");
            }
            catch (Exception e) {
                System.out.println("T1 unlock exception");
                e.printStackTrace();
            }
        });

        Thread thread1 = new Thread(() -> {
            try {
                Thread.sleep(1000);
            }
            catch (Exception e) {
                System.out.println("T2 sleep exception " + e.getClass());
            }

            boolean locked = lock.tryLock();
            System.out.println("T2 acquire lock " + locked);

            try {
                Thread.sleep(1000);
            }
            catch (Exception e) {
                System.out.println("T2 sleep exception " + e.getClass());
            }

            try {
                if (locked) {
                    lock.unlock();
                    System.out.println("T2 lock success");
                }

                System.out.println("T2 unlock");
            }
            catch (Exception e) {
                System.out.println("T2 unlock exception");
                e.printStackTrace();
            }
        });

        thread0.start();
        thread1.start();
        thread0.join();
        thread1.join();
    }

    @Test
    public void testCache() throws Exception {
        IgniteEx server = startGrid(0);
        IgniteEx client = startClientGrid(5);

        IgniteCache<Object, Object> cache = client.createCache("cache0");

        cache.put(1, 1);

//        reconnectClient(client);
        IgniteDiscoverySpi clientDiscSpi = (IgniteDiscoverySpi) client.configuration().getDiscoverySpi();
        clientDiscSpi.clientReconnect();

        cache.put(2, 2);

        log.warning("qqq " + cache.get(1));
        log.warning("qqq " + cache.get(2));
    }

    private static void reconnectClient(Ignite node) throws Exception {
        long clientStartedTopology = node.cluster().topologyVersion();

        IgniteDiscoverySpi clientDiscSpi = (IgniteDiscoverySpi) node.configuration().getDiscoverySpi();
        clientDiscSpi.clientReconnect();

        for (;;) {
            Thread.sleep(500);

            try {
                if(node.cluster().topologyVersion() == clientStartedTopology + 2)
                    break;
                else
                    System.out.println("client still is not in topology");
            }
            catch (Exception e) {
                System.out.println("Client still offline");
            }
        }
    }

    @Test
    public void testLockReproducer() throws Exception {
        IgniteEx srv = startGrid(0);

        IgniteEx client = startClientGrid(5);

        IgniteLock lock = client.reentrantLock("lock", true, false, true);

        lock.lock();
        lock.unlock();
        System.out.println("nodeid0 " + client.localNode().id());

        reconnectClient(client);

        System.out.println("nodeid1 " + client.localNode().id());//после реконнекта меняется

        Thread thread0 = new Thread(() -> {
            lock.lock();
            System.out.println("T1 acquire lock");

//            try {
//                Thread.sleep(1000);
//            }
//            catch (Exception e) {
//                System.out.println("T1 sleep exception " + e.getClass());
//            }

            try {
                Thread.sleep(5000);

                lock.unlock();

                System.out.println("T1 unlock");
            }
            catch (Exception e) {
                System.out.println("T1 unlock exception");
                e.printStackTrace();
            }
        });

        Thread thread1 = new Thread(() -> {
            try {
                Thread.sleep(1000);
            }
            catch (Exception e) {
                System.out.println("T2 sleep exception " + e.getClass());
            }

            lock.lock();
            System.out.println("T2 acquire lock");

            try {
                Thread.sleep(1000);
            }
            catch (Exception e) {
                System.out.println("T2 sleep exception " + e.getClass());
            }

            try {
                lock.unlock();

                System.out.println("T2 unlock");
            }
            catch (Exception e) {
                System.out.println("T2 unlock exception");
                e.printStackTrace();
            }
        });

        thread0.start();
        thread1.start();
        thread0.join();
        thread1.join();
    }
}
