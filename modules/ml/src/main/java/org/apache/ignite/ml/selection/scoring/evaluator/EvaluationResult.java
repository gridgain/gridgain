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

package org.apache.ignite.ml.selection.scoring.evaluator;

import java.io.PrintStream;
import java.util.Map;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.ml.selection.scoring.evaluator.metric.MetricName;

public class EvaluationResult {
    private final Map<MetricName, Double> values;

    public EvaluationResult(Map<MetricName, Double> values) {
        this.values = values;
    }

    public double get(MetricName name) {
        return values.getOrDefault(name, Double.NaN);
    }

    public Iterable<Map.Entry<MetricName, Double>> getAll() {
        return values.entrySet();
    }

    public void print() {
        print(3, System.out);
    }

    public void print(PrintStream out) {
        print(3, out);
    }

    public void print(int precition) {
        print(precition, System.out);
    }

    public void print(int precition, PrintStream out) {
        A.ensure(precition > 0, "precition > 0");
        StringBuilder sb = new StringBuilder();
        values.forEach((k, v) -> {
            sb.append(String.format("%s = %." + precition + "f\n", k.getPrettyName(), v));
        });
        out.println(sb);
    }
}
