package org.apache.ignite.console.event;

import org.springframework.context.ApplicationEvent;

import java.util.concurrent.CompletableFuture;


public abstract class CallableEvent<T> extends ApplicationEvent {

    private CompletableFuture<T> future;

    public CallableEvent(CompletableFuture<T> future) {
        super(new Object());
        this.future = future;
    }

    public CompletableFuture<T> getFuture() {
        return future;
    }
}
