package org.apache.ignite.internal.processors.query.stat;

import org.apache.ignite.internal.processors.query.stat.messages.StatisticsKeyMessage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Statistics gathering context.
 */
public class StatisticsGatheringContext {
    /** Gathering id. */
    private final UUID gatId;

    /** Keys to collect statistics by. */
    private final Set<StatisticsKeyMessage> keys;

    /** Amount of remaining partitions */
    private int remainingParts;

    /** Collected local statistics. */
    private final Map<StatisticsKeyMessage, Collection<ObjectStatisticsImpl>> collectedStatistics;

    /** Done future adapter. */
    private final StatisticsGatheringFutureAdapter doneFut;

    /**
     * Constructor in case when there are already gathering id generated.
     *
     * @param gatId Gathering id.
     * @param keys Collection of keys to collect statistics by.
     * @param remainingParts Number of partition to be collected by gathering context.
     */
    public StatisticsGatheringContext(UUID gatId, Set<StatisticsKeyMessage> keys, int remainingParts) {
        this.gatId = gatId;
        collectedStatistics = null;
        this.keys = keys;
        this.remainingParts = remainingParts;
        this.doneFut = new StatisticsGatheringFutureAdapter(gatId);;
    }

    /**
     * Constructor.
     *
     * @param keys Keys to collect statistics by.
     */
    /*public StatisticsGatheringContext(
            Set<StatisticsKeyMessage> keys
    ) {
        gatId = UUID.randomUUID();
        collectedStatistics = new ConcurrentHashMap<>();
        this.keys = keys;
        this.doneFut = new StatisticsGatheringFutureAdapter(gatId);;
    }*/

    /**
     * @return Collected statistics map.
     */
    public Map<StatisticsKeyMessage, Collection<ObjectStatisticsImpl>> collectedStatistics() {
        return collectedStatistics;
    }

    /**
     * @return Collection of keys to collect statistics by.
     */
    public Collection<StatisticsKeyMessage> keys() {
        return keys;
    }

    /**
     * Register collected object statistics data.
     *
     * @param objsData Object statistics data to register.
     * @param parts Total number of partitions in collected data.
     * @return {@code true} if all required partition collected, {@code false} otherwise.
     */
    public boolean registerCollected(Map<StatisticsKeyMessage, Collection<ObjectStatisticsImpl>> objsData, int parts) {
        for (Map.Entry<StatisticsKeyMessage, Collection<ObjectStatisticsImpl>> objData : objsData.entrySet())
            collectedStatistics.compute(objData.getKey(), (k, v) -> {
                if (v == null)
                    v = new ArrayList<>();

                v.addAll(objData.getValue());

                return v;
            });

        return decrement(parts);
    }

    /**
     * Decrement remaining parts and tell if all required partitions are collected.
     *
     * @param parts Decrement.
     * @return {@code true} if all required partition collected, {@code false} otherwise.
     */
    private synchronized boolean decrement(int parts) {
        this.remainingParts -= parts;
        return remainingParts == 0;
    }

    /**
     * @return Gathering (whole process) id.
     */
    public UUID gatId() {
        return gatId;
    }

    /**
     * @return Collection control future.
     */
    public StatisticsGatheringFutureAdapter doneFut() {
        return doneFut;
    }
}
