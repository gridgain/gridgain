package org.apache.ignite.internal.processors.tracing.messages;

/**
 * This interface indicates class which able to transfer span.
 */
public interface SpanTransport {
    /**
     * Stored span for transferring.
     *
     * @param span Binary view of span.
     */
    void span(byte[] span);

    /**
     * @return Binary view of span.
     */
    byte[] span();
}
