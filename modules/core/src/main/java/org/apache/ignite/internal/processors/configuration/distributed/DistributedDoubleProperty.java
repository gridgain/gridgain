package org.apache.ignite.internal.processors.configuration.distributed;

/**
 * Implementation of {@link DistributedProperty} for {@link Double}.
 */
public class DistributedDoubleProperty extends DistributedComparableProperty<Double> {
    /** {@inheritDoc} */
    DistributedDoubleProperty(String name) {
        super(name);
    }

    /**
     * @param name Name of property.
     * @return Property detached from processor.(Distributed updating are not accessable).
     */
    public static DistributedDoubleProperty detachedDoubleProperty(String name) {
        return new DistributedDoubleProperty(name);
    }
}
