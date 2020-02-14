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

package org.apache.ignite.internal.processors.cache;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.ignite.IgniteCheckedException;

/**
 * The {@code CycleDetectingLockFactory} creates {@link ReentrantLock} instances and {@link
 * ReentrantReadWriteLock} instances that detect potential deadlock by checking for cycles in lock
 * acquisition order.
 *
 * <p>Potential deadlocks detected when calling the {@code lock()}, {@code lockInterruptibly()}, or
 * {@code tryLock()} methods will result in the execution of the {@link Policy} specified when
 * creating the factory. The currently available policies are:
 *
 * <ul>
 *   <li>DISABLED
 *   <li>WARN
 *   <li>THROW
 * </ul>
 *
 * <p>The locks created by a factory instance will detect lock acquisition cycles with locks created
 * by other {@code CycleDetectingLockFactory} instances (except those with {@code Policy.DISABLED}).
 * A lock's behavior when a cycle is detected, however, is defined by the {@code Policy} of the
 * factory that created it. This allows detection of cycles across components while delegating
 * control over lock behavior to individual components.
 *
 * <p>Applications are encouraged to use a {@code CycleDetectingLockFactory} to create any locks for
 * which external/unmanaged code is executed while the lock is held. (See caveats under
 * <strong>Performance</strong>).
 *
 * <p><strong>Cycle Detection</strong>
 *
 * <p>Deadlocks can arise when locks are acquired in an order that forms a cycle. In a simple
 * example involving two locks and two threads, deadlock occurs when one thread acquires Lock A, and
 * then Lock B, while another thread acquires Lock B, and then Lock A:
 *
 * <pre>
 * Thread1: acquire(LockA) --X acquire(LockB)
 * Thread2: acquire(LockB) --X acquire(LockA)
 * </pre>
 *
 * <p>Neither thread will progress because each is waiting for the other. In more complex
 * applications, cycles can arise from interactions among more than 2 locks:
 *
 * <pre>
 * Thread1: acquire(LockA) --X acquire(LockB)
 * Thread2: acquire(LockB) --X acquire(LockC)
 * ...
 * ThreadN: acquire(LockN) --X acquire(LockA)
 * </pre>
 *
 * <p>The implementation detects cycles by constructing a directed graph in which each lock
 * represents a node and each edge represents an acquisition ordering between two locks.
 *
 * <ul>
 *   <li>Each lock adds (and removes) itself to/from a ThreadLocal Set of acquired locks when the
 *       Thread acquires its first hold (and releases its last remaining hold).
 *   <li>Before the lock is acquired, the lock is checked against the current set of acquired
 *       locks---to each of the acquired locks, an edge from the soon-to-be-acquired lock is either
 *       verified or created.
 *   <li>If a new edge needs to be created, the outgoing edges of the acquired locks are traversed
 *       to check for a cycle that reaches the lock to be acquired. If no cycle is detected, a new
 *       "safe" edge is created.
 *   <li>If a cycle is detected, an "unsafe" (cyclic) edge is created to represent a potential
 *       deadlock situation, and the appropriate Policy is executed.
 * </ul>
 *
 * <p>Note that detection of potential deadlock does not necessarily indicate that deadlock will
 * happen, as it is possible that higher level application logic prevents the cyclic lock
 * acquisition from occurring. One example of a false positive is:
 *
 * <pre>
 * LockA -&gt; LockB -&gt; LockC
 * LockA -&gt; LockC -&gt; LockB
 * </pre>
 *
 * <p><strong>ReadWriteLocks</strong>
 *
 * <p>While {@code ReadWriteLock} instances have different properties and can form cycles without
 * potential deadlock, this class treats {@code ReadWriteLock} instances as equivalent to
 * traditional exclusive locks. Although this increases the false positives that the locks detect
 * (i.e. cycles that will not actually result in deadlock), it simplifies the algorithm and
 * implementation considerably. The assumption is that a user of this factory wishes to eliminate
 * any cyclic acquisition ordering.
 *
 * <p><strong>Explicit Lock Acquisition Ordering</strong>
 *
 * <p>The {@link CycleDetectingLockFactory.WithExplicitOrdering} class can be used to enforce an
 * application-specific ordering in addition to performing general cycle detection.
 *
 * <p><strong>Garbage Collection</strong>
 *
 * <p>In order to allow proper garbage collection of unused locks, the edges of the lock graph are
 * weak references.
 *
 * <p><strong>Performance</strong>
 *
 * <p>The extra bookkeeping done by cycle detecting locks comes at some cost to performance.
 * Benchmarks (as of December 2011) show that:
 *
 * <ul>
 *   <li>for an unnested {@code lock()} and {@code unlock()}, a cycle detecting lock takes 38ns as
 *       opposed to the 24ns taken by a plain lock.
 *   <li>for nested locking, the cost increases with the depth of the nesting:
 *       <ul>
 *         <li>2 levels: average of 64ns per lock()/unlock()
 *         <li>3 levels: average of 77ns per lock()/unlock()
 *         <li>4 levels: average of 99ns per lock()/unlock()
 *         <li>5 levels: average of 103ns per lock()/unlock()
 *         <li>10 levels: average of 184ns per lock()/unlock()
 *         <li>20 levels: average of 393ns per lock()/unlock()
 *       </ul>
 * </ul>
 *
 * <p>As such, the CycleDetectingLockFactory may not be suitable for performance-critical
 * applications which involve tightly-looped or deeply-nested locking algorithms.
 *
 * @author Darick Tong
 * @since 13.0
 */
public class CycleDetectingLockFactory {

    /**
     * Encapsulates the action to be taken when a potential deadlock is encountered. Clients can use
     * one of the predefined {@link Policies} or specify a custom implementation. Implementations must
     * be thread-safe.
     *
     * @since 13.0
     */
    public interface Policy {

        /**
         * Called when a potential deadlock is encountered. Implementations can throw the given {@code
         * exception} and/or execute other desired logic.
         *
         * <p>Note that the method will be called even upon an invocation of {@code tryLock()}. Although
         * {@code tryLock()} technically recovers from deadlock by eventually timing out, this behavior
         * is chosen based on the assumption that it is the application's wish to prohibit any cyclical
         * lock acquisitions.
         */
        void handlePotentialDeadlock(PotentialDeadlockException exception) throws IgniteCheckedException;
    }

    /**
     * Pre-defined {@link Policy} implementations.
     *
     * @since 13.0
     */
    public enum Policies implements Policy {
        /**
         * When potential deadlock is detected, this policy results in the throwing of the {@code
         * PotentialDeadlockException} indicating the potential deadlock, which includes stack traces
         * illustrating the cycle in lock acquisition order.
         */
        THROW {
            @Override public void handlePotentialDeadlock(PotentialDeadlockException e) throws IgniteCheckedException {
                throw new IgniteCheckedException(e);
            }
        },

        /**
         * When potential deadlock is detected, this policy results in the logging of a {@link
         * Level#SEVERE} message indicating the potential deadlock, which includes stack traces
         * illustrating the cycle in lock acquisition order.
         */
        WARN {
            @Override public void handlePotentialDeadlock(PotentialDeadlockException e) {
                logger.log(Level.SEVERE, "Detected potential deadlock", e);
            }
        },

        /**
         * Disables cycle detection. This option causes the factory to return unmodified lock
         * implementations provided by the JDK, and is provided to allow applications to easily
         * parameterize when cycle detection is enabled.
         *
         * <p>Note that locks created by a factory with this policy will <em>not</em> participate the
         * cycle detection performed by locks created by other factories.
         */
        DISABLED {
            @Override public void handlePotentialDeadlock(PotentialDeadlockException e) {}
        };
    }

    /** Creates a new factory with the specified policy. */
    public static CycleDetectingLockFactory newInstance(Policy policy) {
        return new CycleDetectingLockFactory(policy);
    }

    /** Equivalent to {@code newReentrantLock(lockName, false)}. */
    public ReentrantLock newReentrantLock(String lockName) {
        return newReentrantLock(lockName, false);
    }

    /**
     * Creates a {@link ReentrantLock} with the given fairness policy. The {@code lockName} is used in
     * the warning or exception output to help identify the locks involved in the detected deadlock.
     */
    public ReentrantLock newReentrantLock(String lockName, boolean fair) {
        return policy == Policies.DISABLED
            ? new ReentrantLock(fair)
            : new CycleDetectingReentrantLock(new LockGraphNode(lockName), fair);
    }

    //////// Implementation /////////

    private static final Logger logger = Logger.getLogger(CycleDetectingLockFactory.class.getName());

    /** Policy. */
    final Policy policy;

    /**
     * @param policy Policy.
     */
    private CycleDetectingLockFactory(Policy policy) {
        this.policy = (Objects.requireNonNull(policy));
    }

    /**
     * Tracks the currently acquired locks for each Thread, kept up to date by calls to {@link
     * #aboutToAcquire(CycleDetectingLock)} and {@link #lockStateChanged(CycleDetectingLock)}.
     */
    // This is logically a Set, but an ArrayList is used to minimize the amount
    // of allocation done on lock()/unlock().
    private static final ThreadLocal<ArrayList<LockGraphNode>> acquiredLocks =
        ThreadLocal.withInitial(() -> new ArrayList<>(3));

    /**
     * A Throwable used to record a stack trace that illustrates an example of a specific lock
     * acquisition ordering. The top of the stack trace is truncated such that it starts with the
     * acquisition of the lock in question, e.g.
     *
     * <pre>
     * com...ExampleStackTrace: LockB -&gt; LockC
     *   at com...CycleDetectingReentrantLock.lock(CycleDetectingLockFactory.java:443)
     *   at ...
     *   at ...
     *   at com...MyClass.someMethodThatAcquiresLockB(MyClass.java:123)
     * </pre>
     */
    private static class ExampleStackTrace extends IllegalStateException {

        /** */
        private static final long serialVersionUID = 0L;

        /** Excluded class names. */
        static final Set<String> EXCLUDED_CLASS_NAMES =
            new HashSet<>(Arrays.asList(
                CycleDetectingLockFactory.class.getName(),
                ExampleStackTrace.class.getName(),
                LockGraphNode.class.getName()));

        /**
         * @param node1 Node 1.
         * @param node2 Node 2.
         */
        ExampleStackTrace(LockGraphNode node1, LockGraphNode node2) {
            super(node1.getLockName() + " -> " + node2.getLockName());
            StackTraceElement[] origStackTrace = getStackTrace();
            for (int i = 0, n = origStackTrace.length; i < n; i++) {
                if (!EXCLUDED_CLASS_NAMES.contains(origStackTrace[i].getClassName())) {
                    setStackTrace(Arrays.copyOfRange(origStackTrace, i, n));
                    break;
                }
            }
        }
    }

    /**
     * Represents a detected cycle in lock acquisition ordering. The exception includes a causal chain
     * of {@code ExampleStackTrace} instances to illustrate the cycle, e.g.
     *
     * <pre>
     * com....PotentialDeadlockException: Potential Deadlock from LockC -&gt; ReadWriteA
     *   at ...
     *   at ...
     * Caused by: com...ExampleStackTrace: LockB -&gt; LockC
     *   at ...
     *   at ...
     * Caused by: com...ExampleStackTrace: ReadWriteA -&gt; LockB
     *   at ...
     *   at ...
     * </pre>
     *
     * <p>Instances are logged for the {@code Policies.WARN}, and thrown for {@code Policies.THROW}.
     *
     * @since 13.0
     */
    public static final class PotentialDeadlockException extends ExampleStackTrace {

        /** */
        private static final long serialVersionUID = 0L;

        /** Conflicting stack trace. */
        private final ExampleStackTrace conflictingStackTrace;

        /**
         * @param node1 Node 1.
         * @param node2 Node 2.
         * @param conflictingStackTrace Conflicting stack trace.
         */
        private PotentialDeadlockException(
            LockGraphNode node1, LockGraphNode node2, ExampleStackTrace conflictingStackTrace) {
            super(node1, node2);
            this.conflictingStackTrace = conflictingStackTrace;
            initCause(conflictingStackTrace);
        }

        /**
         *
         */
        public ExampleStackTrace getConflictingStackTrace() {
            return conflictingStackTrace;
        }

        /**
         * Appends the chain of messages from the {@code conflictingStackTrace} to the original {@code
         * message}.
         */
        @Override public String getMessage() {

            StringBuilder message = new StringBuilder(super.getMessage());

            for (Throwable t = conflictingStackTrace; t != null; t = t.getCause())
                message.append(", ").append(t.getMessage());

            return message.toString();
        }
    }

    /**
     * Internal Lock implementations implement the {@code CycleDetectingLock} interface, allowing the
     * detection logic to treat all locks in the same manner.
     */
    private interface CycleDetectingLock {

        /** @return the {@link LockGraphNode} associated with this lock. */
        LockGraphNode getLockGraphNode();

        /** @return {@code true} if the current thread has acquired this lock. */
        boolean isAcquiredByCurrentThread();
    }

    /**
     * A {@code LockGraphNode} associated with each lock instance keeps track of the directed edges in
     * the lock acquisition graph.
     */
    private static class LockGraphNode {

        /**
         * The map tracking the locks that are known to be acquired before this lock, each associated
         * with an example stack trace. Locks are weakly keyed to allow proper garbage collection when
         * they are no longer referenced.
         */
        final Map<LockGraphNode, ExampleStackTrace> allowedPriorLocks =
            // originally here was new MapMaker().weakKeys().makeMap();
            new ConcurrentHashMap<>();

        /**
         * The map tracking lock nodes that can cause a lock acquisition cycle if acquired before this
         * node.
         */
        final Map<LockGraphNode, PotentialDeadlockException> disallowedPriorLocks =
            // originally here was new MapMaker().weakKeys().makeMap();
            new ConcurrentHashMap<>();

        /** Lock name. */
        final String lockName;

        /**
         * @param lockName Lock name.
         */
        LockGraphNode(String lockName) {
            this.lockName = Objects.requireNonNull(lockName);
        }

        /**
         *
         */
        String getLockName() {
            return lockName;
        }

        /**
         * @param policy Policy.
         * @param acquiredLocks Acquired locks.
         */
        void checkAcquiredLocks(Policy policy, List<LockGraphNode> acquiredLocks) throws IgniteCheckedException {
            for (int i = 0, size = acquiredLocks.size(); i < size; i++)
                checkAcquiredLock(policy, acquiredLocks.get(i));
            System.out.println("allowedPriorLocks.size() = " + allowedPriorLocks.size());
        }


        /**
         * Checks the acquisition-ordering between {@code this}, which is about to be acquired, and the
         * specified {@code acquiredLock}.
         *
         * <p>When this method returns, the {@code acquiredLock} should be in either the {@code
         * preAcquireLocks} map, for the case in which it is safe to acquire {@code this} after the
         * {@code acquiredLock}, or in the {@code disallowedPriorLocks} map, in which case it is not
         * safe.
         */
        void checkAcquiredLock(Policy policy, LockGraphNode acquiredLock) throws IgniteCheckedException {
            // checkAcquiredLock() should never be invoked by a lock that has already
            // been acquired. For unordered locks, aboutToAcquire() ensures this by
            // checking isAcquiredByCurrentThread(). For ordered locks, however, this
            // can happen because multiple locks may share the same LockGraphNode. In
            // this situation, throw an IllegalStateException as defined by contract
            // described in the documentation of WithExplicitOrdering.
            if (this == acquiredLock)
                throw new IllegalStateException("Attempted to acquire multiple locks with the same rank");

            if (allowedPriorLocks.containsKey(acquiredLock)) {
                // The acquisition ordering from "acquiredLock" to "this" has already
                // been verified as safe. In a properly written application, this is
                // the common case.
                return;
            }

            PotentialDeadlockException previousDeadlockException = disallowedPriorLocks.get(acquiredLock);

            if (previousDeadlockException != null) {
                // Previously determined to be an unsafe lock acquisition.
                // Create a new PotentialDeadlockException with the same causal chain
                // (the example cycle) as that of the cached exception.
                PotentialDeadlockException exception =
                    new PotentialDeadlockException(
                        acquiredLock, this, previousDeadlockException.getConflictingStackTrace());
                policy.handlePotentialDeadlock(exception);
                return;
            }

            // Otherwise, it's the first time seeing this lock relationship. Look for
            // a path from the acquiredLock to this.
            Set<LockGraphNode> seen = Collections.newSetFromMap(new IdentityHashMap<>());

            ExampleStackTrace path = acquiredLock.findPathTo(this, seen);

            if (path == null) {
                // this can be safely acquired after the acquiredLock.
                //
                // Note that there is a race condition here which can result in missing
                // a cyclic edge: it's possible for two threads to simultaneous find
                // "safe" edges which together form a cycle. Preventing this race
                // condition efficiently without _introducing_ deadlock is probably
                // tricky. For now, just accept the race condition---missing a warning
                // now and then is still better than having no deadlock detection.
                allowedPriorLocks.put(acquiredLock, new ExampleStackTrace(acquiredLock, this));
            } else {
                // Unsafe acquisition order detected. Create and cache a
                // PotentialDeadlockException.
                PotentialDeadlockException exception =
                    new PotentialDeadlockException(acquiredLock, this, path);
                disallowedPriorLocks.put(acquiredLock, exception);
                policy.handlePotentialDeadlock(exception);
            }
        }

        /**
         * Performs a depth-first traversal of the graph edges defined by each node's {@code
         * allowedPriorLocks} to find a path between {@code this} and the specified {@code lock}.
         *
         * @return If a path was found, a chained {@link ExampleStackTrace} illustrating the path to the
         *     {@code lock}, or {@code null} if no path was found.
         */
        private ExampleStackTrace findPathTo(LockGraphNode node, Set<LockGraphNode> seen) {
            if (!seen.add(this))
                return null; // Already traversed this node.

            ExampleStackTrace found = allowedPriorLocks.get(node);

            if (found != null)
                return found; // Found a path ending at the node!

            // Recurse the edges.
            for (Entry<LockGraphNode, ExampleStackTrace> entry : allowedPriorLocks.entrySet()) {
                LockGraphNode preAcquiredLock = entry.getKey();
                found = preAcquiredLock.findPathTo(node, seen);
                if (found != null) {
                    // One of this node's allowedPriorLocks found a path. Prepend an
                    // ExampleStackTrace(preAcquiredLock, this) to the returned chain of
                    // ExampleStackTraces.
                    ExampleStackTrace path = new ExampleStackTrace(preAcquiredLock, this);
                    path.setStackTrace(entry.getValue().getStackTrace());
                    path.initCause(found);
                    return path;
                }
            }

            return null;
        }
    }

    /**
     * CycleDetectingLock implementations must call this method before attempting to acquire the lock.
     */
    private void aboutToAcquire(CycleDetectingLock lock) throws IgniteCheckedException {
        if (!lock.isAcquiredByCurrentThread()) {
            ArrayList<LockGraphNode> acquiredLockList = acquiredLocks.get();

            LockGraphNode node = lock.getLockGraphNode();

            node.checkAcquiredLocks(policy, acquiredLockList);

            acquiredLockList.add(node);
        }
    }

    /**
     * CycleDetectingLock implementations must call this method in a {@code finally} clause after any
     * attempt to change the lock state, including both lock and unlock attempts. Failure to do so can
     * result in corrupting the acquireLocks set.
     */
    private static void lockStateChanged(CycleDetectingLock lock) {
        if (!lock.isAcquiredByCurrentThread()) {
            ArrayList<LockGraphNode> acquiredLockList = acquiredLocks.get();

            LockGraphNode node = lock.getLockGraphNode();

            // Iterate in reverse because locks are usually locked/unlocked in a
            // LIFO order.
            for (int i = acquiredLockList.size() - 1; i >= 0; i--) {
                if (acquiredLockList.get(i) == node) {
                    acquiredLockList.remove(i);
                    break;
                }
            }
        }
    }

    private static void acquire(CycleDetectingLock lock) {
        if (lock.isAcquiredByCurrentThread()) {
            ArrayList<LockGraphNode> acquiredLockList = acquiredLocks.get();

            LockGraphNode node = lock.getLockGraphNode();

            acquiredLockList.add(node);
        }
    }

    final class CycleDetectingReentrantLock extends ReentrantLock implements CycleDetectingLock {

        /** */
        private static final long serialVersionUID = 0L;

        /** Lock graph node. */
        private final LockGraphNode lockGraphNode;

        /**
         * @param lockGraphNode Lock graph node.
         * @param fair Fair.
         */
        private CycleDetectingReentrantLock(LockGraphNode lockGraphNode, boolean fair) {
            super(fair);
            this.lockGraphNode = Objects.requireNonNull(lockGraphNode);
        }

        ///// CycleDetectingLock methods. /////

        /** {@inheritDoc} */
        @Override public LockGraphNode getLockGraphNode() {
            return lockGraphNode;
        }

        /** {@inheritDoc} */
        @Override public boolean isAcquiredByCurrentThread() {
            return isHeldByCurrentThread();
        }

        ///// Overridden ReentrantLock methods. /////

        /** {@inheritDoc} */
        @Override public void lock() {
            super.lock();
        }

        public void lockWithDetection() throws IgniteCheckedException {


            aboutToAcquire(this);
            try {
                super.lock();
            } finally {
                lockStateChanged(this);
            }

        }

/*        public void lockWithDetection() throws IgniteCheckedException {
            try {
                if (super.tryLock(10, TimeUnit.MILLISECONDS)) {
                    aboutToAcquire(this);
                }
                else {
                    aboutToAcquire(this);
                    super.lock();
                }
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
            finally {
                lockStateChanged(this);
            }
        }*/

        /** {@inheritDoc} */
        @Override public void lockInterruptibly() throws InterruptedException {
            try {
                aboutToAcquire(this);
            }
            catch (IgniteCheckedException e) {
                e.printStackTrace();
            }
            try {
                super.lockInterruptibly();
            } finally {
                lockStateChanged(this);
            }
        }

        /** {@inheritDoc} */
        @Override public boolean tryLock() {
            try {
                aboutToAcquire(this);
            }
            catch (IgniteCheckedException e) {
                e.printStackTrace();
            }
            try {
                return super.tryLock();
            }
            finally {
                lockStateChanged(this);
            }
        }

        /** {@inheritDoc} */
        @Override public boolean tryLock(long timeout, TimeUnit unit) throws InterruptedException {
            try {
                aboutToAcquire(this);
            }
            catch (IgniteCheckedException e) {
                e.printStackTrace();
            }
            try {
                return super.tryLock(timeout, unit);
            }
            finally {
                lockStateChanged(this);
            }
        }

        /** {@inheritDoc} */
        @Override public void unlock() {
            try {
                super.unlock();
            }
            finally {
                lockStateChanged(this);
            }
        }
    }
}
