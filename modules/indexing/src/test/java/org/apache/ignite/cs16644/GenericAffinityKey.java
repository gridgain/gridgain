package org.apache.ignite.cs16644;

import java.io.Serializable;
import java.util.Objects;

/**
 * Created by yuestanl on 16/12/2019.
 */
public class GenericAffinityKey implements Serializable {
    private final String key;

    public GenericAffinityKey(String key) {
        this.key = key;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GenericAffinityKey that = (GenericAffinityKey) o;
        return Objects.equals(key, that.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key);
    }

    @Override
    public String toString() {
        return "GenericAffinityKey{" +
            " key='" + key + '\'' +
            '}';
    }
}
