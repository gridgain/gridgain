package org.apache.ignite.marshaller;

import org.apache.ignite.internal.ClassSet;
import org.apache.ignite.lang.IgnitePredicate;

import java.util.Objects;

/** */
public class IgniteMarshallerClassFilter implements IgnitePredicate<String> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final transient ClassSet whiteList;

    /** */
    private final transient ClassSet blackList;

    /**
     * @param whiteList Class white list.
     * @param blackList Class black list.
     */
    public IgniteMarshallerClassFilter(ClassSet whiteList, ClassSet blackList) {
        this.whiteList = whiteList;
        this.blackList = blackList;
    }

    /** {@inheritDoc} */
    @Override public boolean apply(String s) {
        // Allows all arrays.
        if ((blackList != null || whiteList != null) && s.charAt(0) == '[')
            return true;

        return (blackList == null || !blackList.contains(s)) && (whiteList == null || whiteList.contains(s));
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (!(o instanceof IgniteMarshallerClassFilter))
            return false;

        IgniteMarshallerClassFilter other = (IgniteMarshallerClassFilter)o;

        return Objects.equals(whiteList, other.whiteList) && Objects.equals(blackList, other.blackList);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(whiteList, blackList);
    }
}
