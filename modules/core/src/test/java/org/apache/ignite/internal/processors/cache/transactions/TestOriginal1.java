package org.apache.ignite.internal.processors.cache.transactions;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLock;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.discovery.IgniteDiscoverySpi;

public class TestOriginal1 {
    public static void main(String[] args) throws Exception {
        Ignition.start(new IgniteConfiguration().setIgniteInstanceName("server"));

        IgniteEx client = (IgniteEx)Ignition.start(new IgniteConfiguration().setIgniteInstanceName("client").setClientMode(true));

        IgniteLock lock = client.reentrantLock("lock", true, false, true);

        lock.lock();
        lock.unlock();



        System.out.println("nodeid0 " + client.localNode().id());

        reconnectClient(client);

        System.out.println("nodeid1 " + client.localNode().id());

        lock.lock();
        lock.unlock();

//        new Thread(() -> {
//            lock.lock();
//            System.out.println("T1 acquire lock");
//
//            try { Thread.sleep(1000); }
//            catch (Exception e) {
//                System.out.println("T1 sleep exception " + e.getClass());
//            }
//
//            try {
//                Thread.sleep(1000000000);
//
//                lock.unlock();
//
//                System.out.println("T1 unlock");
//            }
//            catch (Exception e) {
//                System.out.println("T1 unlock exception");
//                e.printStackTrace();
//            }
//        }).start();
//
//        new Thread(() -> {
//            try { Thread.sleep(3000); }
//            catch (Exception e) {
//                System.out.println("T2 sleep exception " + e.getClass());
//            }
//
//            lock.lock();
//            System.out.println("T2 acquire lock");
//
//            try { Thread.sleep(1000); }
//            catch (Exception e) {
//                System.out.println("T2 sleep exception " + e.getClass());
//            }
//
//            try {
//                lock.unlock();
//
//                System.out.println("T2 unlock");
//            }
//            catch (Exception e) {
//                System.out.println("T2 unlock exception");
//                e.printStackTrace();
//            }
//        }).start();
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
}
