package org.apache.ignite.internal.processors.tracing;

public interface Span {
    Span addTag(String tagName, String tagVal);
    Span addLog(String logDesc);
    Span end();
}
