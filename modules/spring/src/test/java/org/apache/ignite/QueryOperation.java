package org.apache.ignite;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.G;

public class QueryOperation extends CombineIgniteOperation<Void> {
    public static void main(String[] args) throws Exception {
        Ignite ign = G.start("modules/spring/client_base.xml");

        ign.cluster().active(true);

        new QueryOperation().evaluate(ign);
    }

    private final String name = "query";
    private String[] types = {"int primary key", "number", "varchar", "varchar2"};
    private String[] columnsA = {"colA", "colB", "colC", "colD"};
    private String[] columnsB = {"colBA", "colBB", "colBC", "colBD"};

    @Override
    protected Void evaluate0() throws Exception {

        while (true) {
            try {
                String tableA = getString(name);
                String tableB = getString(name);

                createTable(tableA, columnsA);
//                checkFirstLine(name);
                createTable(tableB, columnsB);
                List<Object[]> someRows = fillTable(tableA, columnsA, 100);
                fillTable(tableB, columnsB, 200, someRows);

                select(tableA, tableB);

                delete(tableA, columnsA, someRows);
                delete(tableB, columnsB, someRows);

                dropTable(tableA);
                dropTable(tableB);
            } catch (Exception e) {
                handle(e, name);
            }

        }
    }

    private void dropTable(String tableA) {
        exec(String.format("DROP TABLE %s", tableA));
    }

    private void delete(String table, String[] cols, List<Object[]> rows) {
        rows.forEach((row) -> {
            int randCol = rand.nextInt(row.length);
            Object val = row[randCol];
            val = val instanceof String ? String.format("'%s'", val) : val;
            exec(String.format("DELETE FROM %s WHERE %s=%s", table, cols[randCol], val));
        });

    }

    private void select(String tableA, String tableB) {
        IntStream.range(0, columnsA.length).forEach((i) -> {
            exec(String.format("SELECT * FROM %s A JOIN %s B ON A.%s = B.%s ORDER BY A.%s",
                    tableA, tableB, columnsA[i], columnsB[i], columnsA[i]));
            exec(String.format("SELECT * FROM %s A JOIN %s B ON A.%s = B.%s ORDER BY A.%s",
                    tableB, tableA, columnsB[i], columnsA[i], columnsB[i]));
        });
    }

    private List<Object[]> fillTable(String table, String[] cols, Integer rowsCount) {

        List<Object[]> storeSomeValues = new ArrayList<>();
        for (int i = 0; i < rowsCount; i++) {

            Object[] values = {rand.nextInt(), rand.nextInt(), getString(name), getString(name)};
            if (rand.nextBoolean() && rand.nextBoolean()) {
                storeSomeValues.add(values);
            }

            exec(String.format("INSERT INTO %s(%s) VALUES (%s)", table, getCols(cols), getVals(values)));
        }
        return storeSomeValues;
    }

    private void fillTable(String table, String[] cols, Integer rowsCount, List<Object[]> additionalRows) {
        fillTable(table, cols, rowsCount);
        additionalRows.stream().forEach((row) ->
                exec(String.format("INSERT INTO %s(%s) VALUES (%s)", table, getCols(cols), getVals(row))));
    }


    private String getVals(Object[] values) {
        return Arrays.stream(values).map((val) -> {
            if (val instanceof String) return String.format("'%s'", val);
            return val;
        }).map(Object::toString).collect(Collectors.joining(", "));
    }


    private void createTable(String table, String[] cols) {
        exec(String.format("CREATE TABLE %s(%s)", table, getFullCols(cols, types)));
    }

    private String getFullCols(String[] cols, String[] types) {
        StringBuilder res = new StringBuilder();
        IntStream.range(0, cols.length).forEachOrdered(i -> {
            res.append(cols[i]).append(" ").append(types[i]);
            if (i + 1 != cols.length) res.append(",");
        });
        return res.toString();
    }

    private String getCols(String[] cols) {
        return String.join(", ", cols);
    }


    protected void exec(String sql) {
        ((IgniteEx) ignite).context().query().querySqlFields(new SqlFieldsQuery(sql), true);
    }
}
