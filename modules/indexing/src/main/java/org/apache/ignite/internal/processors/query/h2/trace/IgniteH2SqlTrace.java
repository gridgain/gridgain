/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.h2.trace;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.binary.BinaryRawWriter;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinaryWriter;
import org.apache.ignite.binary.Binarylizable;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.binary.streams.BinaryHeapInputStream;
import org.apache.ignite.internal.binary.streams.BinaryHeapOutputStream;
import org.apache.ignite.internal.processors.query.h2.H2SqlTrace;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Command execution result.
 */
public class IgniteH2SqlTrace extends H2SqlTrace implements Binarylizable {
    private String name;

    private Map<String, LongAdder> cntrs = new HashMap<>();

    private List<String> msgs = new ArrayList<>();

    private IgniteH2SqlTrace parent;

    private List<H2SqlTrace> children = new ArrayList<>();

    private final long t0;

    private long duration;

    private static ThreadLocal<IgniteH2SqlTrace> traceThreaded = new ThreadLocal<>();

    public IgniteH2SqlTrace() {
        t0 = System.nanoTime();
    }

    public IgniteH2SqlTrace(String name) {
        this();

        this.name  = name;
    }

    @Override public H2SqlTrace createTrace(String name) {
        IgniteH2SqlTrace trace = new IgniteH2SqlTrace(name);

        children.add(trace);

        return trace;
    }

    @Override public void addTrace(H2SqlTrace trace) {
        if (trace != null)
            children.add(trace);
    }

    @Override public H2SqlTrace parent() {
        return parent;
    }

    @Override public H2SqlTrace root() {
        H2SqlTrace r = this;

        while (r.parent() != null)
            r = r.parent();

        return r;
    }

    @Override public H2SqlTrace current() {
        return null;
    }

    @Override public String name() {
        return name;
    }

    @Override public void log(String msg) {
        msgs.add(msg);
    }

    @Override public void add(String cntName, long val) {
        LongAdder cntNew = new LongAdder();

        LongAdder cntOld = cntrs.putIfAbsent(cntName, cntNew);

        if (cntOld != null)
            cntNew = cntOld;

        cntNew.add(val);
    }

    @Override public void close() throws Exception {
        duration = System.nanoTime() - t0;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
        BinaryRawWriter brw = writer.rawWriter();

        final Map<String, Long> cntrs4Write = new HashMap<>();

        cntrs.forEach((k, v)-> cntrs4Write.put(k, v.longValue()));

        brw.writeString(name);
        brw.writeLong(duration);
        brw.writeMap(cntrs4Write);
        brw.writeCollection(msgs);
        brw.writeCollection(children);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
        BinaryRawReader brr = reader.rawReader();

        name = brr.readString();
        duration = brr.readLong();
        Map<String, Long> cntrsRead = brr.readMap();
        msgs = (List)brr.readCollection();
        children = (List)brr.readCollection();

        cntrsRead.forEach((k, v)-> {
            LongAdder la = new LongAdder();
            la.add(v);
            cntrs.put(k, la);
        });

        for (H2SqlTrace c : children)
            ((IgniteH2SqlTrace)c).parent = this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        final StringBuilder sb = new StringBuilder();

        return print(sb, "");
    }

    /** */
    public String print(StringBuilder sb, String indent) {
        sb.append(indent).append(">>> ").append(name).append('\n')
            .append(indent).append("duration=" + (double)duration / 1000000).append("ms\n");

        if (cntrs != null)
            cntrs.forEach((key, value) -> sb.append(indent).append(key).append('=').append(value.longValue()).append('\n'));

        if (msgs != null)
            msgs.forEach(msg -> sb.append(indent).append(msg).append('\n'));

        if (children != null)
            children.forEach(c -> ((IgniteH2SqlTrace)c).print(sb, indent + "  "));

        sb.append(indent).append("<<<\n");

        return sb.toString();
    }

    public static void trace(IgniteH2SqlTrace trace) {
        traceThreaded.set(trace);
    }

    public static void reset() {
        traceThreaded.remove();
    }

    public static byte[] toBytes(BinaryContext bctx, IgniteH2SqlTrace trace) {
        if (trace == null)
            return null;

        BinaryWriterExImpl writer = new BinaryWriterExImpl(bctx, new BinaryHeapOutputStream(1000),
            null, null);

        trace.writeBinary(writer);

        return writer.array();
    }

    public static IgniteH2SqlTrace fromBytes(BinaryContext bctx, byte[] arr) {
        if (F.isEmpty(arr))
            return null;

        BinaryReaderExImpl reader = new BinaryReaderExImpl(bctx, new BinaryHeapInputStream(arr), null,
            null, false);

        IgniteH2SqlTrace trace = new IgniteH2SqlTrace();

        trace.readBinary(reader);

        return trace;
    }

}
