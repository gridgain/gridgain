package org.apache.ignite.tools.marshaller;

import sun.misc.ObjectInputFilter;

import java.util.function.Function;

/** */
public class IgniteObjectInputFilter implements ObjectInputFilter {
    /** */
    private final Function<String, Boolean> clsFilter;

    /** @param clsFilter Ignite marshaller class filter to which class validation will be delegated. */
    public IgniteObjectInputFilter(Function<String, Boolean> clsFilter) {
        this.clsFilter = clsFilter;
    }

    /** {@inheritDoc} */
    @Override public Status checkInput(FilterInfo filterInfo) {
        Class<?> cls = filterInfo.serialClass();

        if (cls == null)
            return Status.UNDECIDED;

        return clsFilter.apply(cls.getName()) ? Status.ALLOWED : Status.REJECTED;
    }

    /** @return Ignite marshaller class filter. */
    public Function<String, Boolean> classFilter() {
        return clsFilter;
    }
}
