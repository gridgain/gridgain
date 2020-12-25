package org.apache.ignite.internal.processors.query.stat;

import org.apache.ignite.internal.processors.query.stat.messages.StatisticsKeyMessage;
import org.apache.ignite.internal.processors.query.stat.messages.StatisticsObjectData;

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
    private Set<StatisticsKeyMessage> keys;

    /** Collected local statistics. */
    private final Map<StatisticsKeyMessage, Collection<StatisticsObjectData>> collectedStatistics;

    /** Done future adapter. */
    private final StatisticsGatheringFutureAdapter doneFut;

    /**
     * Constructor in case when there are already gathering id generated.
     *
     * @param gatId Gathering id.
     * @param keys Collection of keys to collect statistics by.
     */
    public StatisticsGatheringContext(UUID gatId, Set<StatisticsKeyMessage> keys) {
        this.gatId = gatId;
        collectedStatistics = null;
        this.keys = keys;
        this.doneFut = new StatisticsGatheringFutureAdapter(gatId);;
    }

    /**
     * Constructor.
     *
     * @param keys Keys to collect statistics by.
     *             // TODO remove comment below
     * @param remainingColReqs Collection of remaining requests. If {@code null} - it's local collection task.
     */
    public StatisticsGatheringContext(
            Set<StatisticsKeyMessage> keys//, Map<UUID, StatisticsAddrRequest<StatisticsGatheringRequest>> remainingColReqs
    ) {
        gatId = UUID.randomUUID();
        collectedStatistics = new ConcurrentHashMap<>();
        this.keys = keys;
        this.doneFut = new StatisticsGatheringFutureAdapter(gatId);;
    }

    /**
     * @return Collected statistics map.
     */
    public Map<StatisticsKeyMessage, Collection<StatisticsObjectData>> collectedStatistics() {
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
     * @param objData Object statistics data to register.
     */
    public void registerCollected(Collection<StatisticsObjectData> objData) {
        objData.forEach(data -> {
            assert keys.contains(data.key());

            collectedStatistics.compute(data.key(), (k, v) -> {
               if (v == null)
                   v = new ArrayList<>();

               v.add(data);

               return v;
            });
        });
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
