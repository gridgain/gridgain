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

package org.apache.ignite.testframework;

import java.io.EOFException;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.internal.util.GridJavaProcess;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteInClosure;

/**
 *
 */
@SuppressWarnings("unchecked")
public final class TestJavaProcess {
    /** */
    private static final FileSystem FS = FileSystems.getDefault();

    /** */
    private GridJavaProcess proc;

    /** */
    private String name;

    /** */
    private Context procCtx = new Context();

    /** */
    private Object res;

    /** */
    private final IgniteInClosure<String> outC = new IgniteInClosure<String>() {
        @Override public void apply(String s) {
            System.out.println(name + ">>>" + s);
        }
    };

    /**
     * Private constructor to promote factory method usage.
     */
    private TestJavaProcess() {
        // No-op
    }

    /**
     *
     */
    public static void exec(GridTestUtils.IgniteRunnableX r, String... jvmArgs) throws Exception {
        TestJavaProcess proc = new TestJavaProcess();

        proc.name = r.getClass().getSimpleName();

        proc.procCtx.inputFile(r)
            .jvmArgs(jvmArgs);

        try {
            proc.proc = GridJavaProcess.exec(TestProcessClosureLauncher.class.getName(),
                proc.procCtx.params(),
                null,
                proc.outC,
                null,
                null,
                proc.procCtx.jvmArgs,
                null
            );

            proc.waitFor();
        }
        finally {
            if(proc.proc != null)
                proc.proc.kill();

            proc.procCtx.close();
        }
    }

    /**
     *
     */
    public static <R> R exec(IgniteCallable r, String... jvmArgs) throws Exception {
        TestJavaProcess proc = new TestJavaProcess();

        proc.name = r.getClass().getSimpleName();

        proc.procCtx.inputFile(r)
            .jvmArgs(jvmArgs);

        try {
            proc.proc = GridJavaProcess.exec(TestProcessClosureLauncher.class.getName(),
                proc.procCtx.params(),
                null,
                proc.outC,
                null,
                null,
                proc.procCtx.jvmArgs,
                null
            );

            proc.waitFor();

            return (R)proc.res;
        }
        finally {
            if(proc.proc != null)
                proc.proc.kill();

            proc.procCtx.close();
        }
    }

    /** */
    private int waitFor() throws Exception {
        int ret = proc.getProcess().waitFor();

        res = procCtx.result();

        if (res instanceof Exception)
            throw new Exception("The closure is finished with exception", (Exception)res);
        else if (res instanceof Error)
            throw new Exception("The closure is finished with error", (Error)res);

        if (ret != 0)
            throw new Exception("Abnormal exit code [name=" + name +", code=" + ret);

        return ret;
    }

    /**
     *
     */
    private static class Context implements AutoCloseable {
        /** */
        String inFileName;

        /** */
        String outFileName = "out-" + UUID.randomUUID().toString() + ".dat";

        /** */
        List<String> jvmArgs;

        /** */
        private Context() {
            // No-op.
        }

        /** */
        Context inputFile(Object closure, Object... params) throws Exception {
            inFileName = "in-" + UUID.randomUUID().toString() + ".dat";

            try (ObjectOutput oos = new ObjectOutputStream(Files.newOutputStream(FS.getPath(inFileName)))) {
                oos.writeObject(closure);

                for (Object p : params)
                    oos.writeObject(p);
            }

            return this;
        }

        /**
         *
         */
        Context jvmArgs(String... jvmArgs) {
            this.jvmArgs = Arrays.asList(jvmArgs);

            return this;
        }

        /**
         *
         */
        String params() {
            StringBuilder sb = new StringBuilder();

            sb.append("in ");
            sb.append(inFileName).append(' ');
            sb.append("out ");
            sb.append(outFileName);

            return sb.toString();
        }

        /**
         */
        public Object result() throws Exception {
            try (ObjectInputStream oi = new ObjectInputStream(Files.newInputStream(FS.getPath(outFileName)))) {
                return oi.readObject();
            }
            catch (EOFException e) {
                return null;
            }
        }

        /** {@inheritDoc} */
        @Override public void close() throws Exception {
            Files.delete(FS.getPath(inFileName));
            Files.delete(FS.getPath(outFileName));
        }

    }
}