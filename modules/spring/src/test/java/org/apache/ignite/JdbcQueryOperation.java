package org.apache.ignite;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.internal.util.typedef.G;

public class JdbcQueryOperation extends CombineIgniteOperation<Void> {
    public static void main(String[] args) throws Exception {
        Ignite ign = G.start("modules/spring/client_base.xml");

        ign.cluster().active(true);

        new JdbcQueryOperation().evaluate(ign);
    }

    private final String name = "jdbc";
    private String[] types = {"int primary key", "number", "varchar", "varchar2"};
    private String[] columnsA = {"colA", "colB", "colC", "colD"};
    private String[] columnsB = {"colBA", "colBB", "colBC", "colBD"};

    @Override
    protected Void evaluate0() throws Exception {

        while (true) {
            long randomSkip = rand.nextInt(ignite.cluster().forServers().nodes().size());
            String firstAddress = ignite.cluster()
                    .forServers()
                    .nodes().stream().skip(randomSkip).findFirst().get()
                    .addresses().stream().findAny().get();
            String port = String.format("1080%s", randomSkip);
            try (Connection conn = DriverManager.getConnection(String.format("jdbc:ignite:thin://%s:%s/", firstAddress, port))) {
                try (Statement stmt = conn.createStatement()) {
                    String tableA = getString(name);
                    String tableB = getString(name);

                    createTable(tableA, columnsA, stmt);
//                    checkFirstLine(name);
                    createTable(tableB, columnsB, stmt);
                    List<Object[]> someRows = fillTable(tableA, columnsA, 100, stmt);
                    fillTable(tableB, columnsB, 200, someRows, stmt);

                    select(tableA, tableB, stmt);

                    delete(tableA, columnsA, someRows, stmt);
                    delete(tableB, columnsB, someRows, stmt);

                    dropTable(tableA, stmt);
                    dropTable(tableB, stmt);
                }
            } catch (Exception e) {
                handle(e, name);
            }
        }
    }

    private void dropTable(String tableA, Statement stmt) throws SQLException {
        exec(String.format("DROP TABLE %s", tableA), stmt);
    }

    private void delete(String table, String[] cols, List<Object[]> rows, Statement stmt) throws SQLException {
        for (Object[] row : rows) {
            int randCol = rand.nextInt(row.length);
            Object val = row[randCol];
            val = val instanceof String ? String.format("'%s'", val) : val;
            exec(String.format("DELETE FROM %s WHERE %s=%s", table, cols[randCol], val), stmt);
        }

    }

    private void select(String tableA, String tableB, Statement stmt) throws SQLException {
        int bound = columnsA.length;
        for (int i = 0; i < bound; i++) {
            exec(String.format("SELECT * FROM %s A JOIN %s B ON A.%s = B.%s ORDER BY A.%s",
                    tableA, tableB, columnsA[i], columnsB[i], columnsA[i]), stmt);
            exec(String.format("SELECT * FROM %s A JOIN %s B ON A.%s = B.%s ORDER BY A.%s",
                    tableB, tableA, columnsB[i], columnsA[i], columnsB[i]), stmt);
        }
    }

    private List<Object[]> fillTable(String table, String[] cols, Integer rowsCount, Statement stmt) throws SQLException {

        List<Object[]> storeSomeValues = new ArrayList<>();
        for (int i = 0; i < rowsCount; i++) {

            Object[] values = {rand.nextInt(), rand.nextInt(), getString(name), getString(name)};
            if (rand.nextBoolean() && rand.nextBoolean()) {
                storeSomeValues.add(values);
            }

            exec(String.format("INSERT INTO %s(%s) VALUES (%s)", table, getCols(cols), getVals(values)), stmt);
        }
        return storeSomeValues;
    }

    private void fillTable(String table, String[] cols, Integer rowsCount, List<Object[]> additionalRows, Statement stmt) throws SQLException {
        fillTable(table, cols, rowsCount, stmt);
        for (Object[] row : additionalRows) {
            exec(String.format("INSERT INTO %s(%s) VALUES (%s)", table, getCols(cols), getVals(row)), stmt);
        }
    }


    private String getVals(Object[] values) {
        return Arrays.stream(values).map((val) -> {
            if (val instanceof String) return String.format("'%s'", val);
            return val;
        }).map(Object::toString).collect(Collectors.joining(", "));
    }


    private void createTable(String table, String[] cols, Statement stmt) throws SQLException {
        exec(String.format("CREATE TABLE %s(%s)", table, getFullCols(cols, types)), stmt);
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


    protected void exec(String sql, Statement stmt) throws SQLException {
        stmt.execute(sql);
    }
}
