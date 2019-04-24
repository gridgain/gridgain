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

package com.gridgain.experiment.loader.ignite;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.resources.IgniteInstanceResource;

public class AdHocClient {
    private static int counter = 0;

    public static void main(String[] args) {
        G.setClientMode(true);
        try (Ignite cli = G.start("config/client.xml")) {
            cli.cache("abbr5").query(new SqlFieldsQuery("select * from abbr5 limit 10"))
                .forEach(System.err::println);

            long t0 = System.currentTimeMillis();

            IgniteFuture<Long> fut = cli.compute(cli.cluster().forServers()).callAsync(new QueryJob1());

            System.err.println("Result: " + fut.get());

            long t1 = System.currentTimeMillis();
            System.err.println("Time: " + (t1 - t0));
        }

        System.err.println(counter);
    }

    public static class QueryJob1 implements IgniteCallable<Long> {
        @IgniteInstanceResource
        private transient Ignite ign;

        @Override public Long call() throws Exception {
            System.err.println("123");
            int buffSz = 1000;
            ArrayList<Object> buff = new ArrayList<>(buffSz);
            long cnt = 0;
            for (int i = 0; i < 10; i++) {
                for (List<?> row : ign.cache("abbr5").query(new SqlFieldsQuery("select abbr7_ocrnc_span_2_to_dt from abbr5"))) {
                    if (buff.size() == buffSz)
                        buff.clear();
                    else
                        buff.add(row);

                    cnt++;
                }
//                    .forEach(row -> counter += row.stream()
//                        .filter(Objects::isNull)
//                        .count());
            }

            return cnt;
        }
    }
}
