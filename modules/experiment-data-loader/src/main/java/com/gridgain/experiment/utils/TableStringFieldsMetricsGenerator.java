package com.gridgain.experiment.utils;

import com.gridgain.experiment.model.abbr1CdRecord;
import com.gridgain.experiment.model.abbr2e4tStusNpiRecord;
import com.gridgain.experiment.model.abbr3RfrncRecord;
import com.gridgain.experiment.model.abbr4ProvierUpicRecord;
import com.gridgain.experiment.model.abbr5Key;
import com.gridgain.experiment.model.abbr5Record;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;

public class TableStringFieldsMetricsGenerator {

    public static void main(String[] args) {

        if (args.length != 4) {
            throw new IllegalArgumentException("Not all parameters");
        }

        String configUrl = args[0];

        String storeDir = args[1];

        int lines = Integer.valueOf(args[2]);

        int expSize = Integer.valueOf(args[3]);

        try (Ignite ignite = Ignition.start(configUrl)) {
            generateMetrics(storeDir, "abbr5", "\"abbr5\".abbr5", lines, expSize, abbr5Key.class, abbr5Record.class, ignite);
            generateMetrics(storeDir,"abbr4_p10c", "\"abbr4_p10c\".abbr4_p10c", lines, expSize, null, abbr4ProvierUpicRecord.class, ignite);
            generateMetrics(storeDir,"abbr3_RFRNC", "\"abbr3_RFRNC\".abbr3_rfrnc", lines, expSize,  null, abbr3RfrncRecord.class, ignite);
            generateMetrics(storeDir,"abbr2_e4t_STUS_NPI", "\"abbr2_e4t_STUS_NPI\".abbr2_e4t_stus_npi", lines, expSize, null, abbr2e4tStusNpiRecord.class, ignite);
            generateMetrics(storeDir,"abbr1_CD", "\"abbr1_CD\".abbr1_cd", lines, expSize,  null, abbr1CdRecord.class, ignite);
        }

    }

    private static <K, V> void generateMetrics(String storeDir, String cacheName, String tableName, int limit, int expSize, Class<K> k, Class<V> v, Ignite ignite) {
        IgniteCache<K, V> cache = ignite.cache(cacheName);

        Map <String, AverageCountHolder> map = new HashMap<>();

        List<String> fields = new ArrayList<>();

        if (k != null) {
            for (Field field : k.getDeclaredFields()) {
                Class<?> type = field.getType();

                if (type.equals(String.class))
                    fields.add(field.getName());
            }
        }

        for (Field field : v.getDeclaredFields()) {
            Class<?> type = field.getType();

            if (type.equals(String.class))
                fields.add(field.getName());
        }

        String query = buildQuery(tableName, limit, fields);

        // Iterate over the result set.
        try (QueryCursor<List<?>> cursor = cache.query(new SqlFieldsQuery(query))) {
            for (List<?> row : cursor) {
                for (int j = 0; j < row.size(); j++) {
                    Object current = row.get(j);

                    if (current instanceof String) {
                        String str = (String) current;

                        AverageCountHolder averageCountHolder = !map.containsKey(fields.get(j)) ?
                                new AverageCountHolder(20,fields.get(j), cacheName) : map.get(fields.get(j));

                        averageCountHolder.updateAverage(str.length());

                        map.put(fields.get(j), averageCountHolder);
                    }
                }
            }
        }

        StringBuilder stringBuilder = new StringBuilder("Table - " + cacheName + "\n");

        for (Map.Entry<String, AverageCountHolder> entry : map.entrySet()) {
            stringBuilder.append(entry.getValue().shortOutput() + "\n");
        }

        stringBuilder.append("Full output: \n");

        for (Map.Entry<String, AverageCountHolder> entry : map.entrySet()) {
            stringBuilder.append(entry.getValue().toString() + "\n");
        }

        try {
            Files.write(new File(storeDir + "/" + cacheName + ".txt").toPath(), stringBuilder.toString().getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static String buildQuery(String tableName, Integer limit, List<String> fields) {
        StringBuilder stringBuilder = new StringBuilder();

        stringBuilder.append("select ");

        int size = 0;

        for (String field : fields) {
            if (size != 0) {
                stringBuilder.append(", ");
            }

            stringBuilder.append(field);

            size++;
        }

        stringBuilder.append(" FROM " + tableName);
        stringBuilder.append(" LIMIT ");
        stringBuilder.append(limit);

        return stringBuilder.toString();
    }

    private static class AverageCountHolder {
        private double maxLength = 0;

        private double average = 0;

        private int count = 0;

        private int maxSize;

        private int[] percentiles = new int[11];

        private String fieldName;

        private String cacheName;

        public AverageCountHolder(int maxExpectedSize, String fieldName,String cacheName) {
            this.fieldName = fieldName;
            this.maxSize = maxExpectedSize;
            this.cacheName = cacheName;
        }

        public void updateAverage(long newValue) {
            count++;

            average = average + (newValue - average) / count;

            if (newValue > maxLength) {
                maxLength = newValue;
            }

            int percent = (int)((newValue * 10.0f) / maxSize);

            percent = percent > 10 ? 10 : percent;

            percentiles[percent]++;
        }

        public double getAverage() {
            return average;
        }

        public int getCount() {
            return count;
        }

        public String shortOutput() {
            return fieldName + "=" + average;
        }

        @Override
        public String toString() {
            String res = "//-----------------------------------------\n" +
                        "Field name: " + cacheName + "." + fieldName + "\n" +
                        "Total lines: " + count + "\n" +
                        "Max length: " + maxLength + "\n" +
                        "Average length: " + average + "\n" +
                        "Max value for percentiles calculation: " + maxSize + "\n" +
                        "=>0-<10% - " + percentiles[0] + "\n" +
                        "=>10-<20% - " + percentiles[1] + "\n" +
                        "=>20-<30% - " + percentiles[2] + "\n" +
                        "=>30-<40% - " + percentiles[3] + "\n" +
                        "=>40-<50% - " + percentiles[4] + "\n" +
                        "=>50-<60% - " + percentiles[5] + "\n" +
                        "=>60-<70% - " + percentiles[6] + "\n" +
                        "=>70-<80% - " + percentiles[7] + "\n" +
                        "=>80-<90% - " + percentiles[8] + "\n" +
                        "=>90-<100% - " + percentiles[9] + "\n" +
                        "=>100+ - " + percentiles[10];

            return res;
        }
    }
}
