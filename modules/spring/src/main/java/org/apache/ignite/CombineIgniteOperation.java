package org.apache.ignite;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class CombineIgniteOperation<T> {
    protected Random rand = new Random();
    protected AtomicInteger counter = new AtomicInteger();

    protected Ignite ignite;

    protected abstract T evaluate0() throws Exception;

    public T evaluate(Ignite ignite) throws Exception {
        this.ignite = ignite;

        return evaluate0();
    }

    protected String getString(String str) {
        return str + counter.getAndIncrement();
    }

    protected void handle(Exception e, String name) {
    }
}
