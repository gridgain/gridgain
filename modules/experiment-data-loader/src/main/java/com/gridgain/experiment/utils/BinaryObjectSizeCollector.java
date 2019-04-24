package com.gridgain.experiment.utils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Iterator;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.internal.binary.BinaryObjectExImpl;

public class BinaryObjectSizeCollector {

    public static void main(String[] args) {

        if (args.length != 3) {
            throw new IllegalArgumentException("Not all parameters");
        }

        String configUrl = args[0];

        String storeDir = args[1];

        int lines = Integer.valueOf(args[2]);

        try (Ignite ignite = Ignition.start(configUrl)) {
            generateMetrics(storeDir, "abbr5", lines, ignite);
            generateMetrics(storeDir,"abbr4_p10c", lines, ignite);
            generateMetrics(storeDir,"abbr3_RFRNC", lines, ignite);
            generateMetrics(storeDir,"abbr2_e4t_STUS_NPI", lines, ignite);
            generateMetrics(storeDir,"abbr1_CD", lines, ignite);
        }
    }

    private static <K, V> void generateMetrics(String storeDir, String cacheName, int limit, Ignite ignite) {
        IgniteCache<BinaryObject, BinaryObject> cache = ignite.cache(cacheName).withKeepBinary();

        int i = 0;

        Iterator<Cache.Entry<BinaryObject, BinaryObject>> it = cache.iterator();
        StringBuilder stringBuilder = new StringBuilder("Table - " + cacheName + "\n");

        while (i < limit && it.hasNext()) {
            Cache.Entry<BinaryObject, BinaryObject> e = it.next();

            stringBuilder.append(((BinaryObjectExImpl)e.getValue()).array().length + "\n");

            i++;
        }

        try {
            Files.write(new File(storeDir + "/" +cacheName + ".txt").toPath(), stringBuilder.toString().getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
