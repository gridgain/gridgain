/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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

/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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

import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.domain.JavaMember;
import com.tngtech.archunit.core.domain.JavaModifier;
import com.tngtech.archunit.core.domain.properties.HasAnnotations;
import com.tngtech.archunit.core.domain.properties.HasName;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.core.importer.Location;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.lang.IgniteExperimental;
import org.junit.Assert;
import org.junit.Test;

/** */
public class ApiComplianceTest {
    /** */
    private static final String INTERNAL_PACKAGES_PATTERN = "/internal/";

    /** */
    private static final String IGNITE_ROOT_PACKAGE_PATTERN = "/org/apache/ignite/";

    /** */
    private static final String PATH_TO_JAR = "/Users/korlov/.m2/repository/org/gridgain/ignite-core/8.7.13/ignite-core-8.7.13.jar";

    /** */
    private static final ImportOption IGNORE_INTERNALS = new ImportOption() {
        @Override public boolean includes(Location location) {
            return !location.contains(INTERNAL_PACKAGES_PATTERN);
        }
    };

    /** */
    private static final ImportOption IGNITE_SRC = new ImportOption() {
        @Override public boolean includes(Location location) {
            return location.contains(IGNITE_ROOT_PACKAGE_PATTERN);
        }
    };

    /** */
    @Test
    public void test() throws URISyntaxException {
        JavaClasses curClasses = loadLocalClasses();
        JavaClasses refClasses = loadReferenceClasses(Location.of(createUriFromFilePath(PATH_TO_JAR)));

        List<JavaClass> publicClasses = refClasses.stream()
            .filter(clz -> clz.getModifiers().contains(JavaModifier.PUBLIC))
            .collect(Collectors.toList());

        List<String> errors = new ArrayList<>();

        for (JavaClass cls : publicClasses) {
            if (!curClasses.contain(cls.getFullName())) {
                if (!couldBeDeletedSafe(cls))
                    errors.add("Missing public class: " + cls.getFullName());

                continue;
            }

            JavaClass curClz = curClasses.get(cls.getFullName());

            Map<String, JavaMember> refMembers = publicNonSynthetic(cls.getMembers().stream())
                .collect(Collectors.toMap(HasName.AndFullName::getFullName, Function.identity()));

            Map<String, JavaMember> curMembers = publicNonSynthetic(curClz.getMembers().stream())
                .collect(Collectors.toMap(HasName.AndFullName::getFullName, Function.identity()));

            for (String name : refMembers.keySet()) {
                if (!curMembers.containsKey(name) && !couldBeDeletedSafe(refMembers.get(name)))
                    errors.add("Missing public member: " + name);
            }
        }

        if (!errors.isEmpty()) {
            System.err.println("Found " + errors.size() + " problems:");

            for (String msg : errors)
                System.err.println("\t" + msg);

            Assert.fail();
        }
    }

    /**
     * @param annotated Member.
     */
    private boolean couldBeDeletedSafe(HasAnnotations<?> annotated) {
        return annotated.isAnnotatedWith(Deprecated.class)
            || annotated.isAnnotatedWith(IgniteExperimental.class);
    }

    /** */
    private JavaClasses loadLocalClasses() {
        return createIgniteSrcFileImporter()
            .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
            .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_JARS)
            .importPackages(Collections.singleton(IGNITE_ROOT_PACKAGE_PATTERN));
    }

    /** */
    private JavaClasses loadReferenceClasses(Location refJarLocation) {
        return createIgniteSrcFileImporter()
            .importLocations(Collections.singleton(refJarLocation));
    }

    /** */
    private ClassFileImporter createIgniteSrcFileImporter() {
        return new ClassFileImporter()
            .withImportOption(IGNORE_INTERNALS)
            .withImportOption(IGNITE_SRC);
    }

    /**
     * @param path Path.
     */
    private URI createUriFromFilePath(String path) throws URISyntaxException {
        return new URI("jar:file://" + path + "!/");
    }

    /**
     * @param src Source.
     */
    private Stream<JavaMember> publicNonSynthetic(Stream<JavaMember> src) {
        return src.filter(member -> member.getModifiers().contains(JavaModifier.PUBLIC))
            .filter(member -> !member.getModifiers().contains(JavaModifier.SYNTHETIC));
    }
}
