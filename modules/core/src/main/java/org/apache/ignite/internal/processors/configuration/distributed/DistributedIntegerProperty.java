package org.apache.ignite.internal.processors.configuration.distributed;

/**
 * Implementation of {@link DistributedProperty} for {@link Integer}.
 */
public class DistributedIntegerProperty extends DistributedComparableProperty<Integer> {
    /** {@inheritDoc} */
    DistributedIntegerProperty(String name) {
        super(name, Integer::parseInt);
    }

    /**
     * @param name Name of property.
     * @return Property detached from processor.(Distributed updating are not accessable).
     */
    public static DistributedIntegerProperty detachedIntegerProperty(String name) {
        return new DistributedIntegerProperty(name);
    }
}
