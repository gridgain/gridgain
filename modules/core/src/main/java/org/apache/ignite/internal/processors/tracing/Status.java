package org.apache.ignite.internal.processors.tracing;

public enum Status {
    OK(0),
    CANCELLED(1),
    ABORTED(10);

    private final int value;

    Status(int value) {
        this.value = value;
    }
}
