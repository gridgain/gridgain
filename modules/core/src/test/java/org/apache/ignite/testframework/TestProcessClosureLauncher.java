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

import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.concurrent.Callable;
import java.util.function.Function;
import org.apache.ignite.internal.util.GridJavaProcess;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Launches the test closures at separate java process.
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class TestProcessClosureLauncher {
    private static final FileSystem FS = FileSystems.getDefault();

    /**
     * @param args launcher arguments.
     * @throws Exception On error.
     */
    public static void main(String[] args) throws Exception {
        X.println(GridJavaProcess.PID_MSG_PREFIX + U.jvmPid());

        Parameters params = Parameters.parse(args);

        try (ObjectInput ois = new ObjectInputStream(Files.newInputStream(FS.getPath(params.inFileName)))) {
            try (ObjectOutput oos = new ObjectOutputStream(Files.newOutputStream(FS.getPath(params.outFileName)))) {
                Object c = ois.readObject();

                X.println("Starting closure: " + c.getClass().getSimpleName());

                try {
                    if (c instanceof GridTestUtils.IgniteRunnableX || c instanceof GridTestUtils.RunnableX)
                        ((Runnable)c).run();
                    else if (c instanceof Callable) {
                        Object res = ((Callable)c).call();

                        oos.writeObject(res);
                    }
                    else if (c instanceof Function) {
                        Object p0 = ois.readObject();

                        X.println("Parameter:  " + p0);

                        Object res = ((Function)c).apply(p0);

                        oos.writeObject(res);
                    }
                } catch (Throwable ex) {
                    System.err.println("Exception on run closure: ");
                    ex.printStackTrace(System.err);

                    oos.writeObject(ex);
                }
            }
        }
    }

    /**
     *
     */
    public static class Parameters {
        /** Input file name. */
        private String inFileName;

        /** Output file name. */
        private String outFileName;

        /**
         * @param args Command line arguments.
         * @return launcher parameters.
         * @throws Exception On error.
         */
        public static Parameters parse(String[] args) throws Exception {
            Parameters params = new Parameters();

            for (int i = 0; i < args.length; ++i) {
                switch (args[i]) {
                    case "in":
                        if (i + 1 >= args.length)
                            throw new Exception("");

                        params.inFileName = args[++i];

                        break;

                    case "out":
                        if (i + 1 >= args.length)
                            throw new Exception("");

                        params.outFileName = args[++i];

                        break;

                    default:
                        break;
                }
            }

            return params;
        }
    }
}