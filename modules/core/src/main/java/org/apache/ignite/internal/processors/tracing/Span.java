package org.apache.ignite.internal.processors.tracing;

import java.util.Map;

public interface Span {
    Span addTag(String tagName, String tagVal);
    default Span addTag(String tagName, Long tagVal) {
        return addTag(tagName, String.valueOf(tagVal));
    }
    default Span addTag(String tagName, Integer tagVal) {
        return addTag(tagName, String.valueOf(tagVal));
    }
    Span addLog(String logDesc);
    Span addLog(String logDesc, Map<String, String> attributes);
    Span setStatus(Status status);
    Span end();
}
