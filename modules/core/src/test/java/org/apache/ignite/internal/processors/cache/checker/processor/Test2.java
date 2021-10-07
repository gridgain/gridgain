package org.apache.ignite.internal.processors.cache.checker.processor;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteClientDisconnectedException;
import org.apache.ignite.IgniteLock;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.discovery.IgniteDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

public class Test2 extends GridCommonAbstractTest {
    @Test
    public void testOriginal2() throws Exception {
        IgniteEx srv = startGrid(0);

        IgniteEx client = startClientGrid(5);

        IgniteLock lock = client.reentrantLock("lock", true, true, true);

        lock.lock();
        lock.unlock();

        GridTestUtils.runAsync(() -> {
            for (int i = 0; i < 5; i++) {
                stopGrid(0);
                try {
                    startGrid(0);
                }
                catch (Exception e) {
                    throw new RuntimeException(e);
                }
                doSleep(1000);
                System.out.println("restarted");
            }
        });

        tryClientReconnect(client, lock);

    }
    private static void tryClientReconnect(Ignite client, IgniteLock lock) throws Exception {
        Thread thread0 = new Thread(() -> {
            while (true) {
                if (!lock(client, lock))
                    System.out.println("error locking T1");
                else {
                    System.out.println("T1 acquire lock");
                }

                try {
                    Thread.sleep(1000);
                }
                catch (Exception e) { /* no-op */ }

                if (!unlock(client, lock))
                    System.out.println("error unlocking T1");
                else {
                    System.out.println("T1 unlock");
                }
                // proceed)
                try {
                    Thread.sleep(1000);
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }
        });

        Thread thread1 = new Thread(() -> {
            while (true) {
                if (!lock(client, lock))
                    System.out.println("error locking T2");
                else {
                    System.out.println("T2 acquire lock");
                }

                try {
                    Thread.sleep(1000);
                }
                catch (Exception e) { /* no-op */ }

                if (!unlock(client, lock))
                    System.out.println("error unlocking T1");
                else {
                    System.out.println("T2 unlock");
                }

                try {
                    Thread.sleep(1000);
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        thread0.start();
        thread1.start();
        thread0.join();
        thread1.join();
    }

    private static boolean lock(Ignite client, IgniteLock lock) {
        boolean operationFinished = false;
        while (!operationFinished) {
            try {
                lock.lock();
                operationFinished = true;
            } catch (Exception e) {
                if (e instanceof IgniteClientDisconnectedException) {
                    IgniteClientDisconnectedException cause = (IgniteClientDisconnectedException) e;

                    cause.reconnectFuture().get(); // Wait until the client is reconnected.

                    try { //for persistent clusters.
                        while (!client.cluster().active()) {
                            System.out.println("Waiting for activation");
                            try {
                                Thread.sleep(5000);
                            } catch (InterruptedException interruptedException) {
                                interruptedException.printStackTrace();
                            }
                        }
                    } catch (Exception exception) {
                        exception.printStackTrace();
                    }

                    lock = client.reentrantLock("lock", true, true, true);


                } else {
                    e.printStackTrace();
                    return false;

                }
            }
        }
        return true;
    }

    private static void reconnectClient(Ignite node) throws Exception {
        long clientStartedTopology = node.cluster().topologyVersion();

        IgniteDiscoverySpi clientDiscSpi = (IgniteDiscoverySpi) node.configuration().getDiscoverySpi();
        clientDiscSpi.clientReconnect();

        for (; ; ) {
            Thread.sleep(500);

            try {
                if (node.cluster().topologyVersion() == clientStartedTopology + 2)
                    break;
                else
                    System.out.println("client still is not in topology");
            } catch (Exception e) {
                System.out.println("Client still offline");
            }
        }
    }

    private static boolean unlock(Ignite client, IgniteLock lock) {
        boolean operationFinished = false;
        while (!operationFinished) {
            try {
                lock.unlock();
                operationFinished = true;
            } catch (Exception e) {
                if (e instanceof IgniteClientDisconnectedException) {
                    IgniteClientDisconnectedException cause = (IgniteClientDisconnectedException) e;

                    cause.reconnectFuture().get(); // Wait until the client is reconnected.

                    try { //for persistent clusters
                        while (!client.cluster().active()) {
                            System.out.println("Waiting for activation");
                            try {
                                Thread.sleep(5000);
                            } catch (InterruptedException interruptedException) {
                                interruptedException.printStackTrace();
                            }
                        }
                    } catch (Exception exception) {
                        exception.printStackTrace();
                    }

                    lock = client.reentrantLock("lock", true, true, true);

                } else {
                    e.printStackTrace();
                    return false;

                }


            }
        }

        return true;
    }

}
