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

package org.apache.ignite;

import com.atlassian.clover.CloverNames;
import com.atlassian.clover.ant.types.CloverOptimizedTestSet;
import com.atlassian.clover.api.CloverException;
import com.atlassian.clover.api.registry.ClassInfo;
import com.atlassian.clover.api.registry.FileInfo;
import com.atlassian.clover.registry.Clover2Registry;
import com.atlassian.clover.registry.entities.FullProjectInfo;
import org.apache.tools.ant.Project;
import org.apache.tools.ant.types.FileSet;
import org.apache.tools.ant.types.Resource;
import org.junit.runner.Description;
import org.junit.runner.manipulation.Filter;
import org.junit.runner.manipulation.NoTestsRemainException;
import org.junit.runners.Suite;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

//CLOVER:OFF
/** */
public class OpenCloverOptimizer extends Suite {
    /** {@inheritDoc} */
    public OpenCloverOptimizer(Class<?> klass, RunnerBuilder builder) throws InitializationError {
        super(klass, builder);

        try {

            getClass().getClassLoader().loadClass("com.atlassian.clover.ant.types.CloverOptimizedTestSet");

            filter(new OpenCloverFilter());
        } catch (ClassNotFoundException | NoTestsRemainException | NoSnapshotException ignored) {
        } catch (Exception e) {
            throw new InitializationError(e);
        }
    }

    /** */
    private static final class OpenCloverFilter extends Filter {
        /** */
        final Set<String> resources;
        /** */
        private final FullProjectInfo projectInfo;

        /** */
        private OpenCloverFilter() throws NoSnapshotException, CloverException {
            String snapshot = resolveSnapshotFile();
            String testDirectory = resolveTestDirectory();
            String database = resolveCloverDatabase();

            List<String> causes = null;

            if (snapshot == null)
                causes = addTo("failed to resolve snapshot location", causes);
            if (database == null)
                causes = addTo("failed to resolve clover database location", causes);
            if (testDirectory == null)
                causes = addTo("failed to resolve tests source roots", causes);
            if (causes != null)
                throw new NoSnapshotException("Failed to build test cases filter, " +
                        "mandatory information is missing: " + String.join("; ", causes) + ".");

            Project project = new Project();
            project.init();
            project.setProperty(CloverNames.PROP_INITSTRING, database);

            CloverOptimizedTestSet testsToRun = new CloverOptimizedTestSet();

            testsToRun.setSnapshotFile(new File(snapshot));
            testsToRun.setProject(project);

            File[] testRoots = Optional.ofNullable(new File(testDirectory).listFiles(File::isDirectory)).orElse(new File[0]);

            for (File testSourceRoot : testRoots) {
                FileSet testFileSet = new FileSet();
                testFileSet.setProject(project);
                testFileSet.setDir(testSourceRoot);

                testsToRun.add(testFileSet);
            }

            resources = testsToRun.getOptimizedTestResource().stream().map(Resource::getName).collect(Collectors.toSet());
            projectInfo = Clover2Registry.fromInitString(database, "Registry").getProject();
        }

        /** */
        private String resolveSnapshotFile() {
            return resolve("CLOVER_SNAPSHOT", ".clover", "clover.snapshot");
        }

        /** */
        private String resolveCloverDatabase() {
            return resolve("CLOVER_DATABASE", ".clover", "clover.db");
        }

        /** */
        private String resolveTestDirectory() {
            return resolve("TEST_DIR", "src", "test");
        }

        /** */
        private List<String> addTo(String msg, List<String> dst) {
            if (dst == null)
                dst = new ArrayList<>();

            dst.add(msg);

            return dst;
        }

        /** */
        private String resolve(String envVar, String parent, String child) {
            String path = System.getenv(envVar);

            if (path != null) return path;

            String workingDirectory = System.getProperty("user.dir");

            File dir = new File(workingDirectory);

            while (dir != null) {
                if (dir.isDirectory()) {
                    File[] files = dir.listFiles(f -> f.isDirectory() && f.getName().equals(parent));
                    if (files != null && files.length > 0) {
                        File db = new File(files[0], child);
                        if (db.exists()) return db.getPath();
                    }
                }

                dir = dir.getParentFile();
            }

            return null;
        }

        /** {@inheritDoc} */
        @Override public boolean shouldRun(Description description) {
            if (!description.isTest())
                return true;

            String resourceName = Optional.ofNullable(projectInfo.findClass(description.getClassName()))
                    .map(ClassInfo::getContainingFile)
                    .map(FileInfo::getPackagePath).orElse(null);

            return resourceName == null || resources.contains(resourceName);
        }

        /** {@inheritDoc} */
        @Override public String describe() {
            return "includes: " + resources;
        }
    }

    /** */
    private static final class NoSnapshotException extends Exception {
        /** */
        private static final long serialVersionUID = 1401367762425694364L;

        /** */
        public NoSnapshotException(String message) {
            super(message);
        }
    }
}
//CLOVER:ON
