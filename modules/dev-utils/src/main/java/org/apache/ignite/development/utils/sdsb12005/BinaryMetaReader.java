package org.apache.ignite.development.utils.sdsb12005;

import java.io.File;
import java.io.FileInputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.MarshallerContextImpl;
import org.apache.ignite.internal.binary.BinaryCachingMetadataHandler;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.binary.BinaryMetadata;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.logger.NullLogger;

public class BinaryMetaReader {
    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("Usage:\n" +
                "\tjava -jar binary-meta.jar <path to .../binary_meta_folder> or\n" +
                "\tjava -jar binary-meta.jar <path to .../binary_meta_folder/file.bin>");
            System.exit(0);
        }

        final File path = new File(args[0]);

        for (Map.Entry<File, BinaryMetadata> entry : readMeta(path).entrySet()) {
            if (entry.getValue() == null) {
                System.err.println("Can't unmarshal file:" + entry.getKey().getAbsolutePath());
            }
            else {
                System.out.println(entry.getKey().getAbsolutePath() + " " + entry.getValue());
            }
        }
    }

    public static Map<File, BinaryMetadata> readMeta(final File path) throws Exception {
        final IgniteConfiguration cfg = new IgniteConfiguration();

        final BinaryMarshaller marshaller = createMarshaller(cfg);

        final Collection<File> files = filesInFolder(path);

        return files.parallelStream().collect(Collectors.toMap(file -> file, file -> readMeta(marshaller, file, cfg)));
    }

    /**
     * Get all *.bin files from folder
     *
     * @param folder - folder
     * @return Collection of File
     */
    static Collection<File> filesInFolder(final File folder) {
        if (!folder.isDirectory())
            return Collections.singletonList(folder);

        Collection<File> result = new LinkedList<>();

        for (final File fileEntry : folder.listFiles()) {
            if (fileEntry.isDirectory())
                result.addAll(filesInFolder(fileEntry));
            else if (fileEntry.getName().endsWith(".bin"))
                result.add(fileEntry);

        }
        return result;
    }

    /**
     * Read metadata from file
     *
     * @param marshaller - any BinaryMarshaller, returned by createMarshaller() method is good one
     * @param file - bin file
     * @param cfg - any Ignite config
     * @return BinaryMetadata object
     */
    private static BinaryMetadata readMeta(BinaryMarshaller marshaller, final File file, IgniteConfiguration cfg) {
        try (FileInputStream fs = new FileInputStream(file)) {
            final BinaryMetadata bm = marshaller.unmarshal(fs, U.resolveClassLoader(cfg));
            return bm;
        }
        catch (Exception ex) {
            System.err.println("Can't unmarshal file:" + file.getAbsolutePath());
        }
        return null;
    }

    /**
     * @return BinaryMarshaller - simple binary marshaller
     **/
    private static BinaryMarshaller createMarshaller(IgniteConfiguration cfg) throws Exception {
        BinaryContext ctx = new BinaryContext(BinaryCachingMetadataHandler.create(), cfg,
            new NullLogger());
        BinaryMarshaller marsh = new BinaryMarshaller();
        BinaryConfiguration bCfg = new BinaryConfiguration();
        cfg.setBinaryConfiguration(bCfg);
        marsh.setContext(new MarshallerContextImpl(null, null));
        IgniteUtils.invoke(BinaryMarshaller.class, marsh, "setBinaryContext", ctx, cfg);
        return marsh;
    }
}
