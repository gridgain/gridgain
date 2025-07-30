/*
 * Copyright (c) 2025 Hewlett-Packard Enterprise Development LP.
 * All Rights Reserved.
 *
 * This software is the confidential and proprietary information of
 * Hewlett-Packard Enterprise Development LP.
 */
package org.apache.ignite.internal.benchmarks.trafgen.utils;

/**
 * Utility class to compute metrics.
 */
public final class Metric
{
    private static final double NANO_TO_MS = 1000000.0;
    private static final double MS_TO_SEC = 1000.0;

    private final String label;
    private final String prefix;

    private long reqCount = 0;
    private long reqDuration = 0;
    private long start = 0;
    private long duration = 0;

    /**
     * Class Constructor.
     *
     * @param aLabel
     *            Metric name
     */
    public Metric(String aLabel)
    {
        this(aLabel, "");
    }

    public void clear() {
        reqCount = 0;
        reqDuration = 0;
        start = 0;
        duration = 0;
    }

    /**
     * Class Constructor.
     *
     * @param aLabel
     *            Metric name
     */
    public Metric(String aLabel, String aPrefix)
    {
        label = aLabel;
        prefix = aPrefix;
    }

    /**
     * The current timestamp in ns.
     *
     * @return The current timestamp in ns
     */
    public static long now()
    {
        return System.nanoTime();
    }

    /**
     * Increment an operation count and latency.
     *
     * @param ts
     *            the operation start timestamp
     */
    public void push(long ts)
    {
        long now = now();
        synchronized (this)
        {
            if (start == 0)
            {
                start = now;
            }
            reqDuration += (now - ts);
            reqCount++;
        }
    }

    /**
     * Operations average latency in ms.
     *
     * @return Operations average latency in ms.
     */
    public synchronized double latency()
    {
        return reqCount <= 0 ? 0 : (reqDuration / reqCount) / NANO_TO_MS;
    }

    /**
     * Get the number of operations.
     *
     * @return Operations count
     */
    public synchronized long count()
    {
        return reqCount;
    }

    /**
     * Get duration in ms.
     *
     * @return this metric lifetime from creation to stop()
     */
    public synchronized double duration()
    {
        final long dur = (duration == 0) ? (now() - start) : duration;
        return dur / NANO_TO_MS;
    }

    /**
     * Freeze this metric content.
     */
    public synchronized void stop()
    {
        duration = now() - start;
    }

    /**
     * Get the average throughput per second.
     *
     * @return average throughput per second
     */
    public synchronized double throughput()
    {
        return count() * MS_TO_SEC / duration();
    }

    /**
     * Print report to traces.
     */
    public void trace()
    {
        System.out.println(output());
    }

    /**
     * Print report to stdout.
     */
    public void print()
    {
        System.out.println(output());
    }

    private String output()
    {
        return String.format("%s%-20s: req number [%8d], time [%10f ms], op/sec [%10f], avg latency [%10f ms]", prefix,
            label, count(), duration(), throughput(), latency());
    }
}
