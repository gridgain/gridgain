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

package org.apache.ignite.examples;

//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.List;
//import java.util.Random;
//import java.util.logging.Formatter;
//import java.util.logging.Handler;
//import java.util.logging.LogRecord;
//import java.util.logging.Logger;
//import java.util.logging.StreamHandler;
//import org.apache.ignite.Ignite;
//import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
//import org.apache.ignite.Ignition;
//import org.apache.ignite.configuration.DataRegionConfiguration;
//import org.apache.ignite.configuration.DataStorageConfiguration;
//import org.apache.ignite.configuration.IgniteConfiguration;
//import org.apache.ignite.internal.commandline.CommandHandler;

/**
 * Starts up an empty node with example compute configuration.
 */
public class ExampleNodeStartup {
    /**
     * Start up an empty node with example compute configuration.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteException If failed.
     */
    public static void main(String[] args) throws IgniteException {
//
//        System.setProperty("IGNITE_DIRECT_IO_ENABLED", "true");
//
//        IgniteConfiguration cfg = new IgniteConfiguration();
//        cfg.setIgniteInstanceName("server");
//        cfg.setDataStorageConfiguration(
//            new DataStorageConfiguration()
//            .setDefaultDataRegionConfiguration(
//                new DataRegionConfiguration().setPersistenceEnabled(true)
//        ));
//
//        Ignite ignite = Ignition.start(cfg);
//        ignite.cluster().active(true);
//
//        IgniteCache<Object, Object> cache = ignite.getOrCreateCache("myCache");
//        pupulateCache(cache);
//
//
//        IgniteCache<Object, Object> xxxCache = ignite.getOrCreateCache("xxxCache");
//        pupulateCache(xxxCache);
//
//        ArrayList<String> cmdArgs = new ArrayList<>();
//        cmdArgs.add("--defragmentation");
//        cmdArgs.add("schedule");
//        cmdArgs.add("--nodes");
//        cmdArgs.add("e387f5c5-c9e8-4c12-8c61-b143365a3753");
//
//
//
//        CommandHandler handler = createCommandHandler();
//        handler.execute(cmdArgs);
//
//        System.out.println("Done");
    }

//
//    /** */
//    private static CommandHandler createCommandHandler() {
//        Logger log = CommandHandler.initLogger(null);
//        return new CommandHandler(log);
//    }
//
//
//    private static void pupulateCache(IgniteCache cache){
//        Random rnd = new Random();
//        for(int i = 0; i < 10000; i++) {
//
//            int len = rnd.nextInt(10000);
//            int[] data = new int[len];
//
//            for(int j=0; j < len; j++){
//                data[j] = rnd.nextInt(10000);
//            }
//
//            cache.put(i, data);
//        }
//    }
}
