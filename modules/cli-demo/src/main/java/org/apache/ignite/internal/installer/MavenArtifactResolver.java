/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.installer;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.ParseException;
import java.util.stream.Collectors;
import org.apache.ivy.Ivy;
import org.apache.ivy.core.module.descriptor.DefaultDependencyDescriptor;
import org.apache.ivy.core.module.descriptor.DefaultModuleDescriptor;
import org.apache.ivy.core.module.descriptor.ModuleDescriptor;
import org.apache.ivy.core.module.id.ModuleRevisionId;
import org.apache.ivy.core.report.ResolveReport;
import org.apache.ivy.core.resolve.ResolveOptions;
import org.apache.ivy.core.retrieve.RetrieveOptions;
import org.apache.ivy.core.retrieve.RetrieveReport;
import org.apache.ivy.core.settings.IvySettings;
import org.apache.ivy.plugins.resolver.IBiblioResolver;

/**
 *
 */
public class MavenArtifactResolver {
    public ResolveResult resolve(
        Path mavenRoot,
        String grpId,
        String artifactId,
        String version
    ) throws IOException {
        // create an ivy instance
        File tmpDir = Files.createTempDirectory("ignite-installer-cache").toFile();
        tmpDir.deleteOnExit();

        IvySettings ivySettings = new IvySettings();
        ivySettings.setDefaultCache(tmpDir);
        ivySettings.setDefaultCacheArtifactPattern("[artifact](-[classifier]).[revision].[ext]");

        // use the biblio resolver, if you consider resolving
        // POM declared dependencies
        IBiblioResolver br = new IBiblioResolver();
        br.setM2compatible(true);
        br.setUsepoms(true);
        br.setName("central");

        ivySettings.addResolver(br);
        ivySettings.setDefaultResolver(br.getName());

        Ivy ivy = Ivy.newInstance(ivySettings);


        // Step 1: you always need to resolve before you can retrieve
        //
        ResolveOptions ro = new ResolveOptions();
        // this seems to have no impact, if you resolve by module descriptor
        //
        // (in contrast to resolve by ModuleRevisionId)
        ro.setTransitive(true);
        // if set to false, nothing will be downloaded
        ro.setDownload(true);

        // 1st create an ivy module (this always(!) has a "default" configuration already)
        DefaultModuleDescriptor md = DefaultModuleDescriptor.newDefaultInstance(
            // give it some related name (so it can be cached)
            ModuleRevisionId.newInstance(
                "org.apache.ignite",
                "installer-envelope",
                "working"
            )
        );

        // 2. add dependencies for what we are really looking for
        ModuleRevisionId ri = ModuleRevisionId.newInstance(
            grpId,
            artifactId,
            version
        );
        // don't go transitive here, if you want the single artifact
        DefaultDependencyDescriptor dd = new DefaultDependencyDescriptor(md, ri, false, false, true);

        // map to master to just get the code jar. See generated ivy module xmls from maven repo
        // on how configurations are mapped into ivy. Or check
        // e.g. http://lightguard-jp.blogspot.de/2009/04/ivy-configurations-when-pulling-from.html
        dd.addDependencyConfiguration("default", "master");
        dd.addDependencyConfiguration("default", "runtime");
        dd.addDependencyConfiguration("default", "compile");

        md.addDependency(dd);

        try {
            // now resolve
            ResolveReport rr = ivy.resolve(md,ro);

            if (rr.hasError())
                throw new RuntimeException(rr.getAllProblemMessages().toString());

            // Step 2: retrieve
            ModuleDescriptor m = rr.getModuleDescriptor();

            RetrieveReport retrieveReport = ivy.retrieve(
                m.getModuleRevisionId(),
                new RetrieveOptions()
                    // this is from the envelop module
                    .setConfs(new String[] {"default"})
                    .setDestArtifactPattern(mavenRoot.toFile().getAbsolutePath() + "/[artifact](-[classifier]).[revision].[ext]")
            );


            return new ResolveResult(
                retrieveReport.getCopiedFiles().stream().map(File::getName).collect(Collectors.toList())
            );
        }
        catch (ParseException e) {
            // TOOD
            throw new IOException(e);
        }
    }
}
