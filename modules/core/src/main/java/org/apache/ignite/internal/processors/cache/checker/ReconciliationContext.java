package org.apache.ignite.internal.processors.cache.checker;

import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManagerImpl;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.checker.util.KeyComparator;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/** Context for a cache size reconciliation. */
public class ReconciliationContext {
    public enum SizeReconciliationState {
    NOT_STARTED,
    IN_PROGRESS,
    FINISHED
}

    /** */
    private final Map<Integer, SizeReconciliationState> sizeReconciliationState = new ConcurrentHashMap<>();

    /** */
    private final Map<Integer, KeyCacheObject> firstKeys = new ConcurrentHashMap<>();

    /** */
    public final Map<Integer, KeyCacheObject> lastKeys = new ConcurrentHashMap<>();

    /** */
    public final Map<Integer, AtomicLong> sizes = new ConcurrentHashMap<>();

    /** */
    public final Map<Integer, Map<KeyCacheObject, Boolean>> tempMap = new ConcurrentHashMap<>();

    /** */
    public static final KeyComparator KEY_COMPARATOR = new KeyComparator();

    /** */
    public SizeReconciliationState sizeReconciliationState(int cacheId) {
        return sizeReconciliationState.get(cacheId);
    }

    /** */
    public void sizeReconciliationState(int cacheId, SizeReconciliationState sizeReconciliationState) {
        this.sizeReconciliationState.put(cacheId, sizeReconciliationState);
    }

    /** */
    public KeyCacheObject lastKey(int cacheId) {
        return lastKeys.get(cacheId);
    }

    /** */
    public void lastKey(int cacheId, KeyCacheObject key) {
        lastKeys.put(cacheId, key);
    }

    /** */
    public KeyCacheObject firstKey(int cacheId) {
        return firstKeys.get(cacheId);
    }

    /** */
    public void firstKey(int cacheId, KeyCacheObject key) {
        firstKeys.put(cacheId, key);
    }

    /** */
    public AtomicLong size(int cacheId) {
        return sizes.get(cacheId);
    }

    /** */
    public void size(int cacheId, long size) {
        sizes.putIfAbsent(cacheId, new AtomicLong());

        sizes.get(cacheId).set(size);
    }
}
