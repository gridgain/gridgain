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

package org.apache.ignite.thread;

import java.lang.ref.WeakReference;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.worker.GridWorker;

/**
 * This class adds some necessary plumbing on top of the {@link Thread} class.
 * Specifically, it adds:
 * <ul>
 *      <li>Consistent naming of threads</li>
 *      <li>Dedicated parent thread group</li>
 *      <li>Backing interrupted flag</li>
 *      <li>Name of the grid this thread belongs to</li>
 * </ul>
 * <b>Note</b>: this class is intended for internal use only.
 */
public class IgniteThread extends Thread {
    /** Index for unassigned thread. */
    public static final int GRP_IDX_UNASSIGNED = -1;

    /** Default thread's group. */
    private static final ThreadGroup DFLT_GRP = new ThreadGroup("ignite");

    /** Number of all grid threads in the system. */
    private static final AtomicLong cntr = new AtomicLong();

    /** Per thread current waiting operations */
    private static final ConcurrentHashMap<Long, OperationStackEntry> curOp = new ConcurrentHashMap<>();

    /** The name of the Ignite instance this thread belongs to. */
    protected final String igniteInstanceName;

    /** */
    private int compositeRwLockIdx;

    /** */
    private final int stripe;

    /** */
    private final byte plc;

    /** */
    private boolean holdsTopLock;

    /** */
    private boolean forbiddenToRequestBinaryMetadata;

    /**
     * Idle state change timestamp. Negative if busy, positive if idle.
     */
    private volatile long idleTs = -1;

    /**
     * Creates thread with given worker.
     *
     * @param worker Runnable to create thread with.
     */
    public IgniteThread(GridWorker worker) {
        this(worker.igniteInstanceName(), worker.name(), worker, GRP_IDX_UNASSIGNED, -1, GridIoPolicy.UNDEFINED, false);
    }

    /**
     * Creates grid thread with given name for a given Ignite instance.
     *
     * @param igniteInstanceName Name of the Ignite instance this thread is created for.
     * @param threadName Name of thread.
     * @param r Runnable to execute.
     */
    public IgniteThread(String igniteInstanceName, String threadName, Runnable r) {
        this(igniteInstanceName, threadName, r, GRP_IDX_UNASSIGNED, -1, GridIoPolicy.UNDEFINED, false);
    }

    /**
     * Creates grid thread with given name for a given Ignite instance with specified
     * thread group.
     *  @param igniteInstanceName Name of the Ignite instance this thread is created for.
     * @param threadName Name of thread.
     * @param r Runnable to execute.
     * @param grpIdx Thread index within a group.
     * @param stripe Non-negative stripe number if this thread is striped pool thread.
     * @param idle
     */
    public IgniteThread(String igniteInstanceName, String threadName, Runnable r, int grpIdx, int stripe, byte plc,
        boolean idle) {
        super(DFLT_GRP, r, createName(cntr.incrementAndGet(), threadName, igniteInstanceName));

        A.ensure(grpIdx >= -1, "grpIdx >= -1");

        this.igniteInstanceName = igniteInstanceName;
        this.compositeRwLockIdx = grpIdx;
        this.stripe = stripe;
        this.plc = plc;
        this.idleTs = idle ? 1 : -1;
    }

    /**
     * @param igniteInstanceName Name of the Ignite instance this thread is created for.
     * @param threadGrp Thread group.
     * @param threadName Name of thread.
     */
    protected IgniteThread(String igniteInstanceName, ThreadGroup threadGrp, String threadName) {
        super(threadGrp, threadName);

        this.igniteInstanceName = igniteInstanceName;
        this.compositeRwLockIdx = GRP_IDX_UNASSIGNED;
        this.stripe = -1;
        this.plc = GridIoPolicy.UNDEFINED;
    }

    /**
     * @return Related {@link GridIoPolicy} for internal Ignite pools.
     */
    public byte policy() {
        return plc;
    }

    /**
     * @return Non-negative stripe number if this thread is striped pool thread.
     */
    public int stripe() {
        return stripe;
    }

    /**
     * @return {@code True} if thread belongs to pool processing cache operations.
     */
    public boolean cachePoolThread() {
        return stripe >= 0 ||
            plc == GridIoPolicy.SYSTEM_POOL ||
            plc == GridIoPolicy.UTILITY_CACHE_POOL;
    }

    /**
     * Gets name of the Ignite instance this thread belongs to.
     *
     * @return Name of the Ignite instance this thread belongs to.
     */
    public String getIgniteInstanceName() {
        return igniteInstanceName;
    }

    /**
     * @return Composite RW lock index.
     */
    public int compositeRwLockIndex() {
        return compositeRwLockIdx;
    }

    /**
     * @param compositeRwLockIdx Composite RW lock index.
     */
    public void compositeRwLockIndex(int compositeRwLockIdx) {
        this.compositeRwLockIdx = compositeRwLockIdx;
    }

    /**
     * @return {@code True} if thread is not allowed to request binary metadata to avoid potential deadlock.
     */
    public boolean isForbiddenToRequestBinaryMetadata() {
        return holdsTopLock || forbiddenToRequestBinaryMetadata;
    }

    /**
     * @return {@code True} if thread is not allowed to request binary metadata to avoid potential deadlock.
     */
    public static boolean currentThreadCanRequestBinaryMetadata() {
        IgniteThread curThread = current();

        return curThread == null || !curThread.isForbiddenToRequestBinaryMetadata();
    }

    /**
     * Callback before entry processor execution is started.
     */
    public static void onEntryProcessorEntered(boolean holdsTopLock) {
        Thread curThread = Thread.currentThread();

        if (curThread instanceof IgniteThread)
            ((IgniteThread)curThread).holdsTopLock = holdsTopLock;
    }

    /**
     * Callback after entry processor execution is finished.
     */
    public static void onEntryProcessorLeft() {
        Thread curThread = Thread.currentThread();

        if (curThread instanceof IgniteThread)
            ((IgniteThread)curThread).holdsTopLock = false;
    }

    /**
     * Callback on entering critical section where binary metadata requests are forbidden.
     */
    public static void onForbidBinaryMetadataRequestSectionEntered() {
        Thread curThread = Thread.currentThread();

        if (curThread instanceof IgniteThread)
            ((IgniteThread)curThread).forbiddenToRequestBinaryMetadata = true;
    }

    /**
     * Callback on leaving critical section where binary metadata requests are forbidden.
     */
    public static void onForbidBinaryMetadataRequestSectionLeft() {
        Thread curThread = Thread.currentThread();

        if (curThread instanceof IgniteThread)
            ((IgniteThread)curThread).forbiddenToRequestBinaryMetadata = false;
    }

    /**
     * @return IgniteThread or {@code null} if current thread is not an instance of IgniteThread.
     */
    public static IgniteThread current() {
        Thread thread = Thread.currentThread();

        return thread.getClass() == IgniteThread.class || thread instanceof IgniteThread ?
            ((IgniteThread)thread) : null;
    }

    /**
     * Sets current thread state as idle for stack collection purposes.
     *
     * Note there is assertion that state is changed. Invoke this method in pair with {@link #nowBusy()}.
     */
    public static void nowIdle() {
        IgniteThread current = current();

        if (current != null)
            current.idle(true);
    }

    /**
     * Sets current thread state as busy for stack collection purposes.
     *
     * Note there is assertion that state is changed. Invoke this method in pair with {@link #nowIdle()}.
     */
    public static void nowBusy() {
        IgniteThread current = current();

        if (current != null)
            current.idle(false);
    }

    public long idleTs() {
        return idleTs;
    }

    /** */
    void idle(boolean isIdle) {
        assert Thread.currentThread() == this;

        assert isIdle ? idleTs < 0 : idleTs > 0;

        idleTs = isIdle ? System.currentTimeMillis() : -System.currentTimeMillis();
    }

    /**
     * Creates new thread name.
     *
     * @param num Thread number.
     * @param threadName Thread name.
     * @param igniteInstanceName Ignite instance name.
     * @return New thread name.
     */
    protected static String createName(long num, String threadName, String igniteInstanceName) {
        return threadName + "-#" + num + (igniteInstanceName != null ? '%' + igniteInstanceName + '%' : "");
    }

    public static void pushOp(Object op) {
        curOp.compute(Thread.currentThread().getId(), (k, v) -> new OperationStackEntry(op, v));
    }

    public static void popOp() {
        curOp.compute(Thread.currentThread().getId(), (k, v) -> v == null ? null : v.prev);
    }

    public static OperationStackEntry peekOps(long threadId) {
        return curOp.get(threadId);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgniteThread.class, this, "name", getName());
    }

    public static class OperationStackEntry {
        private final WeakReference<Object> op;

        private final OperationStackEntry prev;

        private OperationStackEntry(Object op, OperationStackEntry prev) {
            this.op = new WeakReference<>(op);

            this.prev = prev;
        }

        public Object op() {
            return op.get();
        }

        public OperationStackEntry prev() {
            return prev;
        }
    }
}
