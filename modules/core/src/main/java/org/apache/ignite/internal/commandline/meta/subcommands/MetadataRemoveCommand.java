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

package org.apache.ignite.internal.commandline.meta.subcommands;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.logging.Logger;
import org.apache.ignite.internal.binary.BinaryMetadata;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientCompute;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientDisconnectedException;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.CommandLogger;
import org.apache.ignite.internal.commandline.meta.MetadataSubCommandsList;
import org.apache.ignite.internal.commandline.meta.tasks.MetadataMarshalled;
import org.apache.ignite.internal.commandline.meta.tasks.MetadataRemoveTask;
import org.apache.ignite.internal.commandline.meta.tasks.MetadataTypeArgs;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.visor.VisorTaskArgument;

/**
 *
 */
public class MetadataRemoveCommand
    extends MetadataAbstractSubCommand<MetadataTypeArgs, MetadataMarshalled> {
    /** Output file name. */
    private static String OPT_OUT_FILE_NAME = "--out";

    /** Output file. */
    private Path outFile;

    /** {@inheritDoc} */
    @Override protected String taskName() {
        return MetadataRemoveTask.class.getName();
    }

    /** {@inheritDoc} */
    @Override public MetadataTypeArgs parseArguments0(CommandArgIterator argIter) {
        outFile = null;

        MetadataTypeArgs argType = MetadataTypeArgs.parseArguments(argIter);

        while (argIter.hasNextSubArg() && outFile == null) {
            String opt = argIter.nextArg("");

            if (OPT_OUT_FILE_NAME.equalsIgnoreCase(opt)) {
                String fileName = argIter.nextArg("output file name");

                outFile = FS.getPath(fileName);
            }
        }

        if (outFile != null) {
            try (OutputStream os = Files.newOutputStream(outFile)) {
                os.close();

                Files.delete(outFile);
            }
            catch (IOException e) {
                throw new IllegalArgumentException("Cannot write to output file " + outFile +
                    ". Error: " + e.toString(), e);
            }
        }

        return argType;
    }

    /** {@inheritDoc} */
    @Override protected MetadataMarshalled execute0(
        GridClientConfiguration clientCfg,
        GridClient client
    ) throws Exception {
        GridClientCompute compute = client.compute();

        Collection<GridClientNode> connectableNodes = compute.nodes(GridClientNode::connectable);

        if (F.isEmpty(connectableNodes))
            throw new GridClientDisconnectedException("Connectable nodes not found", null);

        GridClientNode node = connectableNodes.stream()
            .findAny().orElse(null);

        if (node == null)
            node = compute.balancer().balancedNode(connectableNodes);

        return compute.projection(node).execute(
            taskName(),
            new VisorTaskArgument<>(node.nodeId(), arg(), false)
        );
    }

    /** {@inheritDoc} */
    @Override protected void printResult(MetadataMarshalled res, Logger log) {
        if (res.metadata() == null) {
            log.info("Type not found");

            return;
        }

        BinaryMetadata m = res.metadata();

        if (outFile == null)
            outFile = FS.getPath(m.typeId() + ".bin");

        try {
            storeMeta(res.metadataMarshalled(), outFile);
        }
        catch (IOException e) {
            log.severe("Cannot store removed type'" + m.typeName() + "' to: " + outFile);
            log.severe(CommandLogger.errorMessage(e));

            return;
        }

        log.info("Type '" + m.typeName() + "' is removed. Metadata is stored at: " + outFile);
    }

    /**
     *
     */
    private void storeMeta(byte[] marshalledMeta, Path outFile) throws IOException {
        try (OutputStream os = Files.newOutputStream(outFile)) {
            os.write(marshalledMeta);
        }
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return MetadataSubCommandsList.REMOVE.text();
    }
}
