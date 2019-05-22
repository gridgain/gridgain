package org.apache.ignite.internal.processors.tracing;

import java.io.Serializable;

public interface SerializedSpan extends Serializable {
    byte[] value();
}
