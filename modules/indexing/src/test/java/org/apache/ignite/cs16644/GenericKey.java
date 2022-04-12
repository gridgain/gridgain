package org.apache.ignite.cs16644;

import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;

/**
 * Created by yuestanl on 16/12/2019.
 */
public class GenericKey implements Serializable {
    private final UUID uuid;
    @AffinityKeyMapped
    private final GenericAffinityKey affinityKey;

    public GenericKey(UUID uuid, GenericAffinityKey affinityKey) {
        this.uuid = uuid;
        this.affinityKey = affinityKey;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GenericKey that = (GenericKey) o;
        return Objects.equals(uuid, that.uuid) &&
            Objects.equals(affinityKey, that.affinityKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uuid, affinityKey);
    }

    @Override
    public String toString() {
        return "GenericKey{" +
            "uuid=" + uuid +
            ", affinityKey=" + affinityKey +
            '}';
    }
}
