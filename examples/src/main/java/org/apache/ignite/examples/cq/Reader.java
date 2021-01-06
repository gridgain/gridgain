package org.apache.ignite.examples.cq;

import org.apache.ignite.IgniteJdbcThinDriver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Reader {
    private static final String[] HEADERS = new String[] {
            "cacheName", "localListener", "remoteFilter", "remoteTransformer", "localTransformedListener",
            "lastSendTime", "autoUnsubscribe", "bufferSize", "delayedRegister", "interval", "isEvents", "isMessaging",
            "isQuery", "keepBinary", "nodeId", "notifyExisting", "oldValueRequired", "routineId", "topic"
    };

    public static void main(String[] args) throws Exception {
        run();
    }

    public static void run() throws Exception {
        Map<Integer, Integer> lengths = lengthsMap();
        List<String[]> values = new ArrayList<>();

        Class.forName(IgniteJdbcThinDriver.class.getName());
        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/SYS")) {
            ResultSet rs = conn.prepareStatement("select * from CONTINUOUS_QUERIES").executeQuery();

            fillValuesAndLengths(rs, values, lengths);

            String delimiter = getDelimiter(lengths);

            System.out.println(delimiter);
            System.out.println(getHeader(lengths));
            System.out.println(delimiter);
            System.out.println(delimiter);

            for (String[] ar : values) {
                String row = constructTableRow(ar, lengths);

                System.out.println(row);

                System.out.println(delimiter);
            }
        }
    }

    private static void fillValuesAndLengths(ResultSet rs, List<String[]> list, Map<Integer, Integer> lengths) throws Exception {
        while (rs.next()) {
            String[] ar = new String[HEADERS.length];
            for (int i = 0; i < HEADERS.length; i++) {
                String v = rs.getString(i + 1) + "";
                ar[i] = v;
                Integer len = lengths.getOrDefault(i, 0);
                if (len < v.length())
                    lengths.put(i, v.length());
            }
            list.add(ar);
        }
    }

    private static String constructTableRow(String[] vals, Map<Integer, Integer> lengths) {
        StringBuilder sb = new StringBuilder("| ");
        for (int i = 0; i < vals.length; i++) {
            String vRaw = vals[i];
            Integer len = lengths.get(i);
            String v = normLen(vRaw, len);
            sb.append(v);
            sb.append(" | ");
        }

        return sb.toString();
    }

    private static String getDelimiter(Map<Integer, Integer> lengths) {
        StringBuilder line = new StringBuilder("+");
        for (int i = 0; i < HEADERS.length; i++) {
            Integer len = lengths.get(i);
            line.append(normLen("-", len + 2, "-"));
            line.append("+");
        }

        return line.toString();
    }

    private static String normLen(String v, int l, String s) {
        StringBuilder sb = new StringBuilder(v);
        while (l > sb.length())
            sb.append(s);

        return sb.toString();
    }

    private static String normLen(String v, int l) {
        return normLen(v, l," ");
    }

    private static Map<Integer, Integer> lengthsMap() {
        Map<Integer, Integer> map = new HashMap<>();
        for (int i = 0; i < HEADERS.length; i++)
            map.put(i, HEADERS[i].length());

        return map;
    }

    private static String getHeader(Map<Integer, Integer> lengths) {
        StringBuilder sb = new StringBuilder("| ");

        for (int i = 0; i < HEADERS.length; i++) {
            Integer len = lengths.get(i);
            String v = normLen(HEADERS[i], len);
            sb.append(v);
            sb.append(" | ");
        }

        return sb.toString();
    }
}
