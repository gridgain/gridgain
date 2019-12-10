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

package org.apache.ignite.internal.processors.cache.checker.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.checker.objects.VersionedValue;
import org.apache.ignite.internal.processors.cache.verify.PartitionReconciliationDataRowMeta;
import org.apache.ignite.internal.processors.cache.verify.PartitionReconciliationKeyMeta;
import org.apache.ignite.internal.processors.cache.verify.PartitionReconciliationValueMeta;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;

/**
 *
 */
public class ConsistencyCheckUtils {
    /**
     *
     */
    public static Map<KeyCacheObject, Map<UUID, GridCacheVersion>> checkConflicts(
        Map<KeyCacheObject, Map<UUID, GridCacheVersion>> oldKeys,
        Map<KeyCacheObject, Map<UUID, VersionedValue>> actualKeys,
        GridCacheContext cctx
    ) {
        Map<KeyCacheObject, Map<UUID, GridCacheVersion>> keysWithConflicts = new HashMap<>();

        // Actual keys are a subset of old keys.
        // TODO: 05.12.19 Seems that it's not correct to use keyCacheObject.equals() here.
        for (Map.Entry<KeyCacheObject, Map<UUID, GridCacheVersion>> keyEntry : oldKeys.entrySet()) {
            Map<UUID, VersionedValue> newVer = actualKeys.get(keyEntry.getKey());

            if (newVer != null) {
                // Elements with min GridCacheVersion should increment
                if (!checkConsistency(keyEntry.getValue(), newVer))
                    keysWithConflicts.put(keyEntry.getKey(), keyEntry.getValue());
                else if (keyEntry.getValue().size() != cctx.affinity().nodesByKey(
                        keyEntry.getKey().value(cctx.cacheObjectContext(), false),
                        cctx.affinity().affinityTopologyVersion()).
                        size())
                    keysWithConflicts.put(keyEntry.getKey(), keyEntry.getValue());
            }
        }

        return keysWithConflicts;
    }

    /**
     *
     */
    public static boolean checkConsistency(Map<UUID, GridCacheVersion> oldKeyVer,
        Map<UUID, VersionedValue> actualKeyVer) {
        assert oldKeyVer.size() > 1;

        if (actualKeyVer.isEmpty())
            return true;

        //TODO Possible you can check it use only one iteration.
        Set<UUID> maxVersions = new HashSet<>();

        maxVersions.add(oldKeyVer.keySet().iterator().next());

        for (Map.Entry<UUID, GridCacheVersion> entry : oldKeyVer.entrySet()) {
            GridCacheVersion lastMaxVer = oldKeyVer.get(maxVersions.iterator().next());
            GridCacheVersion curVer = entry.getValue();

            if (curVer.isGreater(lastMaxVer)) {
                maxVersions.clear();
                maxVersions.add(entry.getKey());
            }
            else if (curVer.equals(lastMaxVer))
                maxVersions.add(entry.getKey());
        }

        GridCacheVersion maxVer = oldKeyVer.get(maxVersions.iterator().next());

        for (UUID maxVerOwner : maxVersions) {
            VersionedValue verVal = actualKeyVer.get(maxVerOwner);
            if (verVal == null || maxVer.isLess(verVal.version()))
                return true;
        }

        boolean allNonMaxChanged = true;

        for (Map.Entry<UUID, GridCacheVersion> entry : oldKeyVer.entrySet()) {
            VersionedValue actualVer = actualKeyVer.get(entry.getKey());
            if (!maxVersions.contains(entry.getKey()) && (actualVer == null || !actualVer.version().isGreaterEqual(maxVer))) {
                allNonMaxChanged = false;

                break;
            }
        }

        return allNonMaxChanged;
    }

    /**
     *
     */
    public static KeyCacheObject unmarshalKey(KeyCacheObject unmarshalKey,
        GridCacheContext<Object, Object> cctx) throws IgniteCheckedException {
        if (unmarshalKey == null)
            return null;

        unmarshalKey.finishUnmarshal(cctx.cacheObjectContext(), null);

        return unmarshalKey;
    }

    /**
     *
     */
    public static List<Map<UUID, PartitionReconciliationDataRowMeta>> mapPartitionReconciliation(
        Map<KeyCacheObject, Map<UUID, GridCacheVersion>> conflicts,
        Map<KeyCacheObject, Map<UUID, VersionedValue>> actualKeys,
        CacheObjectContext ctx
    ) throws IgniteCheckedException {
        List<Map<UUID, PartitionReconciliationDataRowMeta>> brokenKeys = new ArrayList<>();

        for (Map.Entry<KeyCacheObject, Map<UUID, GridCacheVersion>> entry : conflicts.entrySet()) {
            KeyCacheObject key = entry.getKey();

            Map<UUID, PartitionReconciliationDataRowMeta> map = new HashMap<>();

            for (Map.Entry<UUID, GridCacheVersion> versionEntry : entry.getValue().entrySet()) {
                UUID nodeId = versionEntry.getKey();

                Optional<CacheObject> cacheObjOpt = Optional.ofNullable(actualKeys.get(key))
                    .flatMap(keyVersions -> Optional.ofNullable(keyVersions.get(nodeId)))
                    .map(VersionedValue::value);

                Optional<String> valStr = cacheObjOpt
                    .flatMap(co -> Optional.ofNullable(co.value(ctx, false)))
                    .map(Object::toString);

                Object keyVal = key.value(ctx, false);

                map.put(nodeId,
                    new PartitionReconciliationDataRowMeta(
                        new PartitionReconciliationKeyMeta(
                            key.valueBytes(ctx),
                            keyVal != null ? keyVal.toString() : null,
                            versionEntry.getValue()
                        ),
                        new PartitionReconciliationValueMeta(
                            cacheObjOpt.isPresent() ? cacheObjOpt.get().valueBytes(ctx) : null,
                            valStr.orElse(null)
                        )
                    ));
            }

            brokenKeys.add(map);
        }

        return brokenKeys;
    }
}
