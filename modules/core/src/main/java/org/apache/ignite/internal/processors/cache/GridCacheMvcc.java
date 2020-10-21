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
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionManager;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.IgnitePair;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * Replicated lock based on MVCC paradigm. This class ensures that locks are acquired
 * in proper order and that there is no more than only one active lock present at all
 * times. It also ensures that new generated lock candidates will appear after
 * old ones in the pending set, hence preventing lock starvation.
 * See {@link GridCacheVersionManager#next(long topVer)} for information on how lock IDs are
 * generated to prevent starvation.
 */
public final class GridCacheMvcc {
    /** Logger reference. */
    private static final AtomicReference<IgniteLogger> logRef = new AtomicReference<>();

    /** Logger. */
    private static volatile IgniteLogger log;

    /** */
    private static final Comparator<GridCacheVersion> SER_VER_COMPARATOR = new Comparator<GridCacheVersion>() {
        @Override public int compare(GridCacheVersion ver1, GridCacheVersion ver2) {
            int nodeOrder1 = ver1.nodeOrder();
            int nodeOrder2 = ver2.nodeOrder();

            if (nodeOrder1 == nodeOrder2) {
                long order1 = ver1.order();
                long order2 = ver2.order();

                assert order1 != order2;

                return order1 > order2 ? 1 : -1;
            }
            else
                return nodeOrder1 > nodeOrder2 ? 1 : -1;
        }
    };

    /** Cache context. */
    @GridToStringExclude
    private final GridCacheContext<?, ?> cctx;

    /** Local queue. */
    @GridToStringInclude
    private LinkedList<GridCacheMvccCandidate> locs;

    /** Remote queue. */
    @GridToStringInclude
    private IgnitePair<GridCacheMvccCandidate> rmts;

    /**
     * @param cctx Cache context.
     */
    public GridCacheMvcc(GridCacheContext<?, ?> cctx) {
        assert cctx != null;

        this.cctx = cctx;

        if (log == null)
            log = U.logger(cctx.kernalContext(), logRef, GridCacheMvcc.class);
    }

    /**
     * @return Any owner.
     */
    @Nullable GridCacheMvccCandidate anyOwner() {
        GridCacheMvccCandidate owner = localOwner();

        if (owner == null)
            owner = remoteOwner();

        return owner;
    }

    /**
     * @return All owners.
     */
    @Nullable public CacheLockCandidates allOwners() {
        CacheLockCandidates owners = localOwners();

        if (owners == null)
            owners = remoteOwner();

        return owners;
    }

    /**
     * @return Remote candidate only if it's first in the list and is marked as <tt>'used'</tt>.
     */
    @Nullable private GridCacheMvccCandidate remoteOwner() {
        if (rmts != null) {
            assert !rmts.isEmpty();

            GridCacheMvccCandidate firstCandidate = rmts.get1();

            // TODO: 28.09.20 fisrtCandidate.used() returns false. Investigate.
//            assert firstCandidate.used() && firstCandidate.owner();
            assert firstCandidate.owner();

            return firstCandidate;
        }

        return null;
    }

    /**
     * @return All local owners.
     */
    @Nullable public CacheLockCandidates localOwners() {
        if (locs != null) {
            assert !locs.isEmpty();

            CacheLockCandidates owners = null;

            GridCacheMvccCandidate first = locs.getFirst();

            if (first.read()) {
                for (GridCacheMvccCandidate cand : locs) {
                    if (cand.owner()) {
                        assert cand.read() : this;

                        if (owners != null) {
                            CacheLockCandidatesList list;

                            if (owners.size() == 1) {
                                GridCacheMvccCandidate owner = owners.candidate(0);

                                owners = list = new CacheLockCandidatesList();

                                ((CacheLockCandidatesList)owners).add(owner);
                            }
                            else
                                list = ((CacheLockCandidatesList)owners);

                            list.add(cand);
                        }
                        else
                            owners = cand;
                    }

                    if (!cand.read())
                        break;
                }
            }
            else if (first.owner())
                owners = first;

            return owners;
        }

        return null;
    }

    /**
     * @return Local candidate only if it's first in the list and is marked
     *      as <tt>'owner'</tt>.
     */
    @Nullable GridCacheMvccCandidate localOwner() {
        if (locs != null) {
            assert !locs.isEmpty();

            GridCacheMvccCandidate first = locs.getFirst();

            return first.owner() ? first : null;
        }

        return null;
    }

    /**
     * @param cands Candidates to search.
     * @param ver Version.
     * @return Candidate for the version.
     */
    @Nullable private GridCacheMvccCandidate candidate(Iterable<GridCacheMvccCandidate> cands,
        GridCacheVersion ver) {
        assert ver != null;

        if (cands != null)
            for (GridCacheMvccCandidate c : cands)
                if (c.version().equals(ver))
                    return c;

        return null;
    }

    @Nullable private GridCacheMvccCandidate candidateRemote(GridCacheVersion ver) {
        assert ver != null;

        if (rmts != null) {
            if (rmts.get1().version().equals(ver))
                return rmts.get1();
            else if (rmts.get2() != null && rmts.get2().version().equals(ver))
                return rmts.get2();
        }

        return null;
    }


    /**
     *
     * @param threadId Thread ID.
     * @param reentry Reentry flag.
     * @return Local candidate for the thread.
     */
    @Nullable private GridCacheMvccCandidate localCandidate(long threadId, boolean reentry) {
        if (locs != null)
            for (GridCacheMvccCandidate cand : locs) {
                if (cand.isHeldByThread(threadId)) {
                    if (cand.reentry() && !reentry)
                        continue;

                    return cand;
                }
            }

        return null;
    }

    /**
     * @param cand Existing candidate.
     * @param newCand New candidate.
     * @return {@code False} if new candidate can not be added.
     */
    private boolean compareSerializableVersion(GridCacheMvccCandidate cand, GridCacheMvccCandidate newCand) {
        assert cand.serializable() && newCand.serializable();

        GridCacheVersion candOrder = cand.serializableOrder();

        assert candOrder != null : cand;

        GridCacheVersion newCandOrder = newCand.serializableOrder();

        assert newCandOrder != null : newCand;

        int cmp = SER_VER_COMPARATOR.compare(candOrder, newCandOrder);

        assert cmp != 0;

        return cmp < 0;
    }

    /**
     * @param cand Candidate to add.
     * @return {@code False} if failed to add candidate and transaction should be cancelled.
     */
    private boolean add0(GridCacheMvccCandidate cand) {
        assert cand != null;

        // Local.
        if (cand.local()) {
            if (locs == null)
                locs = new LinkedList<>();

            if (!cand.nearLocal()) {
                if (!locs.isEmpty()) {
                    if (cand.serializable()) {
                        Iterator<GridCacheMvccCandidate> it = locs.descendingIterator();

                        if (cand.read()) {
                            while (it.hasNext()) {
                                GridCacheMvccCandidate c = it.next();

                                if (!c.serializable())
                                    return false;

                                if (!c.read()) {
                                    if (compareSerializableVersion(c, cand))
                                        break;
                                    else
                                        return false;
                                }
                            }
                        }
                        else {
                            while (it.hasNext()) {
                                GridCacheMvccCandidate c = it.next();

                                if (!c.serializable() || !compareSerializableVersion(c, cand))
                                    return false;

                                if (!c.read())
                                    break;
                            }
                        }

                        locs.addLast(cand);

                        return true;
                    }

                    GridCacheMvccCandidate first = locs.getFirst();

                    if (first.owner()) {
                        // If reentry, add at the beginning. Note that
                        // no reentry happens for DHT-local candidates.
                        if (!cand.dhtLocal() && first.hasSameHolderAs(cand)) {
                            assert !first.serializable();

                            cand.setOwner();
                            cand.setReady();
                            cand.setReentry();

                            locs.addFirst(cand);

                            return true;
                        }
                    }

                    // Iterate in reverse order.
                    for (ListIterator<GridCacheMvccCandidate> it = locs.listIterator(locs.size()); it.hasPrevious(); ) {
                        GridCacheMvccCandidate c = it.previous();

                        assert !c.version().equals(cand.version()) : "Versions can't match [existing=" + c +
                            ", new=" + cand + ']';

                        // Add after the owner or serializable tx.
                        if (c.owner() || c.serializable()) {
                            // Threads are checked above.
                            assert cand.dhtLocal() || !c.hasSameHolderAs(cand);

                            // Reposition.
                            it.next();

                            it.add(cand);

                            return true;
                        }

                        // If not the owner, add after the lesser version.
                        if (c.version().isLess(cand.version())) {
                            // Reposition.
                            it.next();

                            it.add(cand);

                            return true;
                        }
                    }
                }

                // Either list is empty or candidate is first.
                locs.addFirst(cand);
            }
            else
                // For near local candidates just add it to the end of list.
                locs.add(cand);
        }
        // Remote.
        else {
            assert !cand.serializable() && !cand.read() : cand;

            assert !cand.owner() || localOwners() == null : "Cannot have local and remote owners " +
                "at the same time [cand=" + cand + ", locs=" + locs + ", rmts=" + rmts + ']';

            if (rmts == null) {
                rmts = new IgnitePair<>();

                cand.setOwner();

                ((GridFutureAdapter) cand.lockFut).onDone();

                rmts.set1(cand);
            } else {
                assert rmts.size() == 1;

                rmts.set2(cand);

//                try {
//                    cand.latch.await();
//                }
//                catch (InterruptedException e) {
//                    e.printStackTrace();
//                    assert false;
//                }
            }
        }

        return true;
    }

    /**
     * @param ver Version.
     * @param preferLoc Whether or not to prefer local candidates.
     */
    private void remove0(GridCacheVersion ver, boolean preferLoc) {
        if (preferLoc) {
            if (!remove0(locs, ver))
                remove0Remote(ver);
        }
        else if (!remove0Remote(ver))
            remove0(locs, ver);

        if (locs != null && locs.isEmpty())
            locs = null;

        if (rmts != null && rmts.isEmpty())
            rmts = null;
    }

    /**
     * Removes candidate from collection.
     *
     * @param col Collection.
     * @param ver Version of the candidate to remove.
     * @return {@code True} if candidate was removed.
     */
    private boolean remove0(Collection<GridCacheMvccCandidate> col, GridCacheVersion ver) {
        if (col != null) {
            for (Iterator<GridCacheMvccCandidate> it = col.iterator(); it.hasNext(); ) {
                GridCacheMvccCandidate cand = it.next();

                if (cand.version().equals(ver)) {
                    cand.setUsed();
                    cand.setRemoved();

                    it.remove();

                    reassign();

                    return true;
                }
            }
        }

        return false;
    }

    private boolean remove0Remote(GridCacheVersion ver) {
        if (rmts != null) {
            GridCacheMvccCandidate candidateToRemove;

            if (rmts.get2() != null && rmts.get2().version().equals(ver)) {
                candidateToRemove = rmts.get2();

                candidateToRemove.setUsed();
                candidateToRemove.setRemoved();

                rmts.set2(null);

            } else if (rmts.get1().version().equals(ver)) {
                candidateToRemove = rmts.get1();

                candidateToRemove.setUsed();
                candidateToRemove.setRemoved();

                rmts.set1(null);

                reassignRemote();
            } else {
                // TODO: 28.09.20 TMP we should never get here.
                assert false;
            }

            return true;
        }

        return false;
    }

    /**
     *
     * @param exclude Versions to exclude form check.
     * @return {@code True} if lock is empty.
     */
    public boolean isEmpty(GridCacheVersion... exclude) {
        if (locs == null && rmts == null)
            return true;

        if (locs != null) {
            assert !locs.isEmpty();

            if (F.isEmpty(exclude))
                return false;

            for (GridCacheMvccCandidate cand : locs)
                if (!U.containsObjectArray(exclude, cand.version()))
                    return false;
        }

        if (rmts != null) {
            assert !rmts.isEmpty();

            if (F.isEmpty(exclude))
                return false;

            return U.containsObjectArray(exclude, rmts.get1()) &&
                (rmts.get2() == null || U.containsObjectArray(exclude, rmts.get2()));
        }

        return true;
    }

    /**
     * @param parent Parent entry.
     * @param threadId Thread ID.
     * @param ver Lock version.
     * @param timeout Lock acquisition timeout.
     * @param reenter Reentry flag ({@code true} if reentry is allowed).
     * @param tx Transaction flag.
     * @param implicitSingle Implicit transaction flag.
     * @param read Read lock flag.
     * @return New lock candidate if lock was added, or current owner if lock was reentered,
     *      or <tt>null</tt> if lock was owned by another thread and timeout is negative.
     */
    @Nullable public GridCacheMvccCandidate addLocal(
        GridCacheEntryEx parent,
        long threadId,
        GridCacheVersion ver,
        long timeout,
        boolean reenter,
        boolean tx,
        boolean implicitSingle,
        boolean read) {
        return addLocal(
            parent,
            /*nearNodeId*/null,
            /*nearVer*/null,
            threadId,
            ver,
            timeout,
            /*serializable order*/null,
            reenter,
            tx,
            implicitSingle,
            /*dht-local*/false,
            /*read*/read
        );
    }

    /**
     * @param parent Parent entry.
     * @param nearNodeId Near node ID.
     * @param nearVer Near version.
     * @param threadId Thread ID.
     * @param ver Lock version.
     * @param timeout Lock acquisition timeout.
     * @param serOrder Version for serializable transactions ordering.
     * @param reenter Reentry flag ({@code true} if reentry is allowed).
     * @param tx Transaction flag.
     * @param implicitSingle Implicit flag.
     * @param dhtLoc DHT local flag.
     * @param read Read lock flag.
     * @return New lock candidate if lock was added, or current owner if lock was reentered,
     *      or <tt>null</tt> if lock was owned by another thread and timeout is negative.
     */
    @Nullable public GridCacheMvccCandidate addLocal(
        GridCacheEntryEx parent,
        @Nullable UUID nearNodeId,
        @Nullable GridCacheVersion nearVer,
        long threadId,
        GridCacheVersion ver,
        long timeout,
        @Nullable GridCacheVersion serOrder,
        boolean reenter,
        boolean tx,
        boolean implicitSingle,
        boolean dhtLoc,
        boolean read) {
        if (log.isDebugEnabled())
            log.debug("Adding local candidate [mvcc=" + this + ", parent=" + parent + ", threadId=" + threadId +
                ", ver=" + ver + ", timeout=" + timeout + ", reenter=" + reenter + ", tx=" + tx + "]");

        // Don't check reenter for DHT candidates.
        if (!dhtLoc && !reenter) {
            GridCacheMvccCandidate owner = localOwner();

            if (owner != null && owner.isHeldByThreadOrVer(threadId, ver))
                return null;
        }

        // If there are pending locks and timeout is negative,
        // then we give up right away.
        if (timeout < 0) {
            if (locs != null || rmts != null) {
                GridCacheMvccCandidate owner = localOwner();

                // Only proceed if this is a re-entry.
                if (owner == null || !owner.isHeldByThreadOrVer(threadId, ver))
                    return null;
            }
        }

        UUID locNodeId = cctx.nodeId();

        // If this is a reentry, then reentry flag will be flipped within 'add0(..)' method.
        GridCacheMvccCandidate cand = new GridCacheMvccCandidate(
            parent,
            locNodeId,
            nearNodeId,
            nearVer,
            threadId,
            ver,
            /*local*/true,
            /*reenter*/false,
            tx,
            implicitSingle,
            /*near-local*/false,
            dhtLoc,
            serOrder,
            read
        );

        if (serOrder == null) {
            boolean add = add0(cand);

            assert add : cand;
        }
        else {
            if (!add0(cand))
                return null;
        }

        return cand;
    }

    /**
     * Adds new remote lock candidate (either near remote or dht remote).
     *
     * @param parent Parent entry.
     * @param nodeId Node ID.
     * @param otherNodeId Other node ID.
     * @param threadId Thread ID.
     * @param ver Lock version.
     * @param tx Transaction flag.
     * @param implicitSingle Implicit flag.
     * @param nearLoc Near local flag.
     * @return Add remote candidate.
     */
    public GridCacheMvccCandidate addRemote(
        GridCacheEntryEx parent,
        UUID nodeId,
        @Nullable UUID otherNodeId,
        long threadId,
        GridCacheVersion ver,
        boolean tx,
        boolean implicitSingle,
        boolean nearLoc) {
        GridCacheMvccCandidate cand = new GridCacheMvccCandidate(
            parent,
            nodeId,
            otherNodeId,
            null,
            threadId,
            ver,
            /*local*/false,
            /*reentry*/false,
            tx,
            implicitSingle,
            nearLoc,
            false,
            null,
            /*read*/false
        );

        addRemote(cand);

        return cand;
    }

    /**
     * Adds new near local lock candidate.
     *
     * @param parent Parent entry.
     * @param nodeId Node ID.
     * @param otherNodeId Other node ID.
     * @param threadId Thread ID.
     * @param ver Lock version.
     * @param tx Transaction flag.
     * @param implicitSingle Implicit flag.
     * @param read Read lock flag.
     * @return Add remote candidate.
     */
    public GridCacheMvccCandidate addNearLocal(GridCacheEntryEx parent,
        UUID nodeId,
        @Nullable UUID otherNodeId,
        long threadId,
        GridCacheVersion ver,
        boolean tx,
        boolean implicitSingle,
        boolean read) {
        GridCacheMvccCandidate cand = new GridCacheMvccCandidate(parent,
            nodeId,
            otherNodeId,
            null,
            threadId,
            ver,
            /*local*/true,
            /*reentry*/false,
            tx,
            implicitSingle,
            /*near loc*/true,
            /*dht loc*/false,
            null,
            /*read*/read);

        add0(cand);

        return cand;
    }

    /**
     * @param cand Remote candidate.
     */
    private void addRemote(GridCacheMvccCandidate cand) {
        assert !cand.local();

        if (log.isDebugEnabled())
            log.debug("Adding remote candidate [mvcc=" + this + ", cand=" + cand + "]");

        cctx.versions().onReceived(cand.nodeId(), cand.version());

        add0(cand);
    }

    /**
     * @param ver Lock version to acquire or set to ready.
     * @return Current owner.
     */
    @Nullable public CacheLockCandidates readyLocal(GridCacheVersion ver) {
        GridCacheMvccCandidate cand = candidate(ver);

        if (cand == null)
            return allOwners();

        assert cand.local();

        return readyLocal(cand);
    }

    /**
     * @param cand Local candidate added in any of the {@code addLocal(..)} methods.
     * @return Current lock owner.
     */
    @Nullable public CacheLockCandidates readyLocal(GridCacheMvccCandidate cand) {
        assert cand.local();

        cand.setReady();

        reassign();

        return allOwners();
    }

    /**
     * Marks near-local candidate as ready and makes locks reassignment. Following reorderings are performed when
     * candidate is marked ready:
     * <ul>
     *     <li/> All candidates preceding ready one are moved right after it.
     *     <li/> Near local candidate is assigned a mapped dht version. All remote non-pending candidates with
     *          version less then mapped dht version are marked as owned.
     * </ul>
     *
     * @param ver Version to mark as ready.
     * @param mappedVer Mapped dht version.
     * @param committedVers Committed versions.
     * @param rolledBackVers Rolled back versions.
     * @param pending Pending dht versions that are not owned and which version is less then mapped.
     * @return Lock owner after reassignment.
     */
    @Nullable public CacheLockCandidates readyNearLocal(GridCacheVersion ver,
        GridCacheVersion mappedVer,
        Collection<GridCacheVersion> committedVers,
        Collection<GridCacheVersion> rolledBackVers,
        Collection<GridCacheVersion> pending)
    {
        GridCacheMvccCandidate cand = candidate(locs, ver);

        if (cand != null) {
            assert cand.nearLocal() : "Near local candidate is not marked as near local: " + cand;

            cand.setReady();

            boolean setMapped = cand.otherVersion(mappedVer);

            assert setMapped : "Failed to set mapped dht version for near local candidate [mappedVer=" +
                mappedVer + ", cand=" + cand + ']';

            // For near locals we move all not owned candidates after this one.
            List<GridCacheMvccCandidate> mvAfter = null;

            for (ListIterator<GridCacheMvccCandidate> it = locs.listIterator(); it.hasNext(); ) {
                GridCacheMvccCandidate c = it.next();

                assert c.nearLocal() : "Near local candidate is not marked as near local: " + c;

                if (c == cand) {
                    if (mvAfter != null)
                        for (GridCacheMvccCandidate mv : mvAfter)
                            it.add(mv);

                    break;
                }
                else {
                    if (c.owner())
                        continue;

                    assert !c.ready() || (c.read() && cand.read()) :
                        "Cannot have more then one ready near-local candidate [c=" + c + ", cand=" + cand +
                            ", mvcc=" + this + ']';

                    it.remove();

                    if (mvAfter == null)
                        mvAfter = new LinkedList<>();

                    mvAfter.add(c);
                }
            }

            // TODO: 28.09.20 Investigate.
//            // Mark all remote candidates with less version as owner unless it is pending.
//            if (rmts != null) {
//                for (GridCacheMvccCandidate rmt : rmts) {
//                    GridCacheVersion rmtVer = rmt.version();
//
//                    if (rmtVer.isLess(mappedVer)) {
//                        if (!pending.contains(rmtVer) &&
//                            !mappedVer.equals(rmt.ownerVersion()))
//                            rmt.setOwner();
//                    }
//                    else {
//                        // Remote version is greater, so need to check if it was committed or rolled back.
//                        if (committedVers.contains(rmtVer) || rolledBackVers.contains(rmtVer))
//                            rmt.setOwner();
//                    }
//                }
//            }

            reassign();
        }

        return allOwners();
    }

    /**
     * For all remote candidates standing behind the candidate being salvaged marks their transactions
     * as system invalidate and marks these candidates as owned and used.
     *
     * @param ver Version to salvage.
     * @param near {@code True} If salvage near cache candidate.
     */
    public void salvageRemote(GridCacheVersion ver, boolean near) {
        assert ver != null;

        if (rmts != null && rmts.get2() != null) {
            GridCacheMvccCandidate candidateToSalvage = rmts.get2();

            IgniteInternalTx tx = near ? cctx.tm().nearTx(candidateToSalvage.version()) :
                cctx.tm().tx(candidateToSalvage.version());

            if (tx != null) {
                tx.systemInvalidate(true);

                candidateToSalvage.setOwner();
                candidateToSalvage.setUsed();
            }
            else
                rmts.set2(null);
        }
    }

    /**
     * Assigns local lock.
     */
    private void reassign() {
        GridCacheMvccCandidate firstRmt = null;

        if (rmts != null)
            reassignRemote();

        if (locs != null) {
            boolean first = true;

            ListIterator<GridCacheMvccCandidate> it = locs.listIterator();

            while (it.hasNext()) {
                GridCacheMvccCandidate cand = it.next();

                if (first) {
                    if (cand.read()) {
                        if (cand.ready() && !cand.owner())
                            cand.setOwner();

                        while (it.hasNext()) {
                            cand = it.next();

                            if (!cand.read())
                                break;

                            if (cand.ready() && !cand.owner())
                                cand.setOwner();
                        }

                        return;
                    }
                    else if (cand.serializable()) {
                        if (cand.owner() || !cand.ready())
                            return;

                        cand.setOwner();

                        return;
                    }

                    first = false;
                }

                if (cand.owner())
                    return;

                if (cand.ready()) {
                    GridCacheMvccCandidate prev = nonRollbackPrevious(cand);

                    // If previous has not been acquired, this candidate cannot acquire lock either,
                    // so we move on to the next one.
                    if (prev != null && !prev.owner())
                        continue;

                    boolean assigned = false;

                    if (!cctx.isNear() && firstRmt != null && cand.version().isGreater(firstRmt.version())) {
                        // Check previous candidates for 2 cases:
                        // 1. If this candidate is waiting for a smaller remote version,
                        //    then we must check if previous candidate is the owner and
                        //    has the same remote candidate version. In that case, we can
                        //    safely set this candidate to owner as well.
                        // 2. If this candidate is waiting for a smaller remote version,
                        //    then we must check if previous candidate is the owner and
                        //    any of the local candidates with versions smaller than first
                        //    remote version have the same key as the previous owner. In
                        //    that case, we can safely set this candidate to owner as well.
                        while (prev != null && prev.owner()) {
                            for (GridCacheMvccCandidate c : prev.parent().remoteMvccSnapshot()) {
                                if (c.version().equals(firstRmt.version())) {
                                    cand.setOwner();

                                    assigned = true;

                                    break; // For.
                                }
                            }

                            if (!assigned) {
                                for (GridCacheMvccCandidate c : locs) {
                                    if (c == cand || c.version().isGreater(firstRmt.version()))
                                        break;

                                    for (GridCacheMvccCandidate p = c.previous(); p != null; p = p.previous()) {
                                        if (p.key().equals(prev.key())) {
                                            cand.setOwner();

                                            assigned = true;

                                            break; // For.
                                        }
                                    }

                                    if (assigned)
                                        break; // For.
                                }
                            }

                            if (assigned)
                                break; // While.

                            prev = prev.previous();
                        }
                    }

                    if (!assigned) {
                        if (!cctx.isNear() && firstRmt != null) {
                            if (cand.version().isLess(firstRmt.version())) {
                                assert !cand.nearLocal();

                                cand.setOwner();

                                assigned = true;
                            }
                        }
                        else {
                            cand.setOwner();

                            assigned = true;
                        }
                    }

                    if (assigned) {
                        assert !cand.serializable() : cand;

                        it.remove();

                        // Owner must be first in the list.
                        locs.addFirst(cand);
                    }

                    return;
                }
            }
        }
    }

    private void reassignRemote() {
        if (rmts.get1() == null && rmts.get2() != null) {
            rmts.swap();

            rmts.get1().setOwner();

            ((GridFutureAdapter) rmts.get1().lockFut).onDone();
//            rmts.get1().latch.countDown();

            // TODO: 28.09.20 Critical issue here, release lock on sendingPrepareResponse or send listener notification.
        }
    }

    /**
     * @param cand Candidate to check.
     * @return First predecessor that is owner or is not used.
     */
    @Nullable private GridCacheMvccCandidate nonRollbackPrevious(GridCacheMvccCandidate cand) {
        for (GridCacheMvccCandidate c = cand.previous(); c != null; c = c.previous()) {
            if (c.owner() || !c.used())
                return c;
        }

        return null;
    }

    /**
     * Checks if lock should be assigned.
     *
     * @return Owner.
     */
    @Nullable public CacheLockCandidates recheck() {
        reassign();

        return allOwners();
    }

    /**
     * Local local release.
     *
     * @return Removed candidate.
     */
    @Nullable public GridCacheMvccCandidate releaseLocal() {
        return releaseLocal(Thread.currentThread().getId());
    }

    /**
     * Local release.
     *
     * @param threadId ID of the thread.
     * @return Removed candidate.
     */
    @Nullable public GridCacheMvccCandidate releaseLocal(long threadId) {
        CacheLockCandidates owners = localOwners();

        // Release had no effect.
        if (owners == null)
            return null;

        GridCacheMvccCandidate owner = null;

        for (int i = 0; i < owners.size(); i++) {
            GridCacheMvccCandidate owner0 = owners.candidate(i);

            if (owner0.isHeldByThread(threadId)) {
                owner = owner0;

                break;
            }
        }

        if (owner != null) {
            owner.setUsed();

            remove0(owner.version(), true);

            return owner;
        }
        else
            return null;
    }

    /**
     * Removes lock even if it is not owner.
     *
     * @param ver Lock version.
     */
    public void remove(GridCacheVersion ver) {
        remove0(ver, false);
    }

    /**
     * Removes all candidates for node.
     *
     * @param nodeId Node ID.
     * @return Current owner.
     */
    @Nullable public CacheLockCandidates removeExplicitNodeCandidates(UUID nodeId) {
        if (rmts != null) {
            if (!rmts.get2().tx() && (nodeId.equals(rmts.get2().nodeId()) || nodeId.equals(rmts.get2().otherNodeId()))) {
                rmts.get2().setUsed(); // Mark as used to be consistent.
                rmts.get2().setRemoved();

                rmts.set2(null);
            }

            if (!rmts.get1().tx() && (nodeId.equals(rmts.get1().nodeId()) || nodeId.equals(rmts.get1().otherNodeId()))) {
                rmts.get1().setUsed(); // Mark as used to be consistent.
                rmts.get1().setRemoved();

                rmts.set1(null);

                if (rmts.get2() != null) {
                    reassignRemote();

                    return rmts.get1();
                }
            }

            if (rmts.isEmpty())
                rmts = null;
        }

        return null;
    }

    /**
     * Gets candidate for lock ID.
     *
     * @param ver Lock version.
     * @return Candidate or <tt>null</tt> if there is no candidate for given ID.
     */
    @Nullable public GridCacheMvccCandidate candidate(GridCacheVersion ver) {
        GridCacheMvccCandidate cand = candidate(locs, ver);

        if (cand == null)
            cand = candidateRemote(ver);

        return cand;
    }

    /**
     * Gets candidate for lock ID.
     *
     * @param threadId Thread ID.
     * @return Candidate or <tt>null</tt> if there is no candidate for given ID.
     */
    @Nullable GridCacheMvccCandidate localCandidate(long threadId) {
        // Don't return reentries.
        return localCandidate(threadId, false);
    }

    /**
     * @param nodeId Node ID.
     * @param threadId Thread ID.
     * @return Remote candidate.
     */
    @Nullable GridCacheMvccCandidate remoteCandidate(UUID nodeId, long threadId) {
        if (rmts != null) {
            if (rmts.get1().nodeId().equals(nodeId) && rmts.get1().isHeldByThread(threadId))
                return rmts.get1();

            if (rmts.get2() != null && rmts.get2().nodeId().equals(nodeId) && rmts.get2().isHeldByThread(threadId))
                return rmts.get2();
        }

        return null;
    }

    /**
     * Local candidate.
     *
     * @param nodeId Node ID.
     * @param threadId Thread ID.
     * @return Local candidate.
     */
    @Nullable public GridCacheMvccCandidate localCandidate(UUID nodeId, long threadId) {
        if (locs != null)
            for (GridCacheMvccCandidate c : locs)
                if (c.nodeId().equals(nodeId) && c.isHeldByThread(threadId))
                    return c;

        return null;
    }

    /**
     * Local candidate.
     *
     * @param nodeId Node ID.
     * @param threadId Thread ID.
     * @param ver Lock version.
     * @return Local candidate.
     */
    @Nullable public GridCacheMvccCandidate localCandidateByThreadOrVer(UUID nodeId, long threadId, GridCacheVersion ver) {
        if (locs != null) {
            for (GridCacheMvccCandidate c : locs) {
                if (c.nodeId().equals(nodeId) && c.isHeldByThreadOrVer(threadId, ver))
                    return c;
            }
        }

        return null;
    }

    /**
     *
     * @param ver Version.
     * @return {@code True} if candidate with given version exists.
     */
    boolean hasCandidate(GridCacheVersion ver) {
        return candidate(ver) != null;
    }

    /**
     * @param reentry Reentry flag.
     * @return Collection of local candidates.
     */
    public List<GridCacheMvccCandidate> localCandidatesNoCopy(boolean reentry) {
        return candidates(locs, reentry, false, cctx.emptyVersion());
    }

    /**
     * @param excludeVers Exclude versions.
     * @return Collection of local candidates.
     */
    public Collection<GridCacheMvccCandidate> localCandidates(GridCacheVersion... excludeVers) {
        return candidates(locs, false, true, excludeVers);
    }

    /**
     * @param reentries Flag to include reentries.
     * @param excludeVers Exclude versions.
     * @return Collection of local candidates.
     */
    public List<GridCacheMvccCandidate> localCandidates(boolean reentries,
        GridCacheVersion... excludeVers) {
        return candidates(locs, reentries, true, excludeVers);
    }

    /**
     * @param excludeVers Exclude versions.
     * @return Collection of remote candidates.
     */
    public List<GridCacheMvccCandidate> remoteCandidates(GridCacheVersion... excludeVers) {
        // TODO: 28.09.20 Optimize: first of all check usages, optimize method itself and also check whether it's possible to have reentries in rmts.
        if (rmts == null)
            return Collections.emptyList();

        assert !rmts.isEmpty();

        if (F.isEmpty(excludeVers))
            return rmts.get2() == null ?  Collections.singletonList(rmts.get1()) :  Arrays.asList(rmts.get1(), rmts.get2());

        List<GridCacheMvccCandidate> cands = new ArrayList<>(rmts.size());

        if ((!rmts.get1().reentry()) && !U.containsObjectArray(excludeVers, rmts.get1().version()))
            cands.add(rmts.get1());

        if (rmts.get2() != null && ((!rmts.get1().reentry()) && !U.containsObjectArray(excludeVers, rmts.get1().version())))
            cands.add(rmts.get2());

        return cands;
    }

    /**
     * @param col Collection of candidates.
     * @param reentries Reentry flag.
     * @param cp Whether to copy or not.
     * @param excludeVers Exclude versions.
     * @return Collection of candidates minus the exclude versions.
     */
    private List<GridCacheMvccCandidate> candidates(List<GridCacheMvccCandidate> col,
        boolean reentries, boolean cp, GridCacheVersion... excludeVers) {
        if (col == null)
            return Collections.emptyList();

        assert !col.isEmpty();

        if (!cp && F.isEmpty(excludeVers))
            return col;

        List<GridCacheMvccCandidate> cands = new ArrayList<>(col.size());

        for (GridCacheMvccCandidate c : col) {
            // Don't include reentries.
            if ((reentries || !c.reentry()) && !U.containsObjectArray(excludeVers, c.version()))
                cands.add(c);
        }

        return cands;
    }

    /**
     * @param threadId Thread ID to check.
     * @param exclude Versions to ignore.
     * @return {@code True} if lock is owned by the thread with given ID.
     */
    boolean isLocallyOwnedByThread(long threadId, boolean allowDhtLoc, GridCacheVersion... exclude) {
        CacheLockCandidates owners = localOwners();

        if (owners != null) {
            for (int i = 0; i < owners.size(); i++) {
                GridCacheMvccCandidate owner = owners.candidate(i);

                if (owner.isHeldByThread(threadId) && owner.nodeId().equals(cctx.nodeId()) &&
                    (allowDhtLoc || !owner.dhtLocal()) && !U.containsObjectArray(exclude, owner.version()))
                    return true;
            }
        }

        return false;
    }

    /**
     *
     * @param lockVer ID of lock candidate.
     * @return {@code True} if candidate is owner.
     */
    boolean isLocallyOwned(GridCacheVersion lockVer) {
        CacheLockCandidates owners = localOwners();

        return owners != null && owners.hasCandidate(lockVer);
    }

    /**
     * @param lockVer Lock ID.
     * @param threadId Thread ID.
     * @return {@code True} if locked by lock ID or thread ID.
     */
    boolean isLocallyOwnedByIdOrThread(GridCacheVersion lockVer, long threadId) {
        CacheLockCandidates owners = localOwners();

        if (owners != null) {
            for (int i = 0; i < owners.size(); i++) {
                GridCacheMvccCandidate owner = owners.candidate(i);

                if (owner.isHeldByThreadOrVer(threadId, lockVer))
                    return true;
            }
        }

        return false;
    }

    /**
     * @return Local MVCC candidates.
     */
    @Nullable List<GridCacheMvccCandidate> allLocal() {
        return locs;
    }

    /**
     * @param ver Version to check for ownership.
     * @return {@code True} if lock is owned by the specified version.
     */
    boolean isOwnedBy(GridCacheVersion ver) {
        CacheLockCandidates owners = allOwners();

        return owners != null && owners.hasCandidate(ver);
    }

    /** {@inheritDoc} */
    @Override public String toString() { // Synchronize to ensure one-thread at a time.
        return S.toString(GridCacheMvcc.class, this);
    }
}
