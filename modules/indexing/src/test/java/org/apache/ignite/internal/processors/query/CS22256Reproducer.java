package org.apache.ignite.internal.processors.query;

import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.junit.Test;

import java.util.List;

public class CS22256Reproducer extends AbstractIndexingCommonTest {
    //templates names
    private static final String TEMPLATE_MEMORY_PARTITIONED = "memory_partitioned";
    private static final String TEMPLATE_MEMORY_REPLICATED = "memory_replicated";

    //tables names
    private static final String TABLE_CIF_CUSTOMER = "CIF_CUSTOMER";
    private static final String TABLE_IDS_RELATIONSHIP = "IDS_RELATIONSHIP";
    private static final String TABLE_AMF_SOURCES_SUMMARY = "AMF_SOURCES_SUMMARY";

    @Override
    protected void beforeTest() throws Exception {
        super.beforeTest();

        IgniteEx igniteEx = startGrids(2);

        igniteEx.addCacheConfiguration(createTemplateCfg(TEMPLATE_MEMORY_PARTITIONED, true));
        igniteEx.addCacheConfiguration(createTemplateCfg(TEMPLATE_MEMORY_REPLICATED, false));
    }

    private CacheConfiguration createTemplateCfg(String name, boolean partitionMode) {
        CacheConfiguration cacheCfg = new CacheConfiguration(name);

        if (partitionMode) {
            cacheCfg.setCacheMode(CacheMode.PARTITIONED);
            cacheCfg.setBackups(1);
        } else {
            cacheCfg.setCacheMode(CacheMode.REPLICATED);
        }

        return cacheCfg;
    }

    @Override
    protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTest();
    }

    @Test
    public void reproduce() {
        createTables();

        pupulateData();

        executeJoin();

        checkJoinResult();
    }

    private void createTables() {
        sql("CREATE TABLE IF NOT EXISTS " + TABLE_CIF_CUSTOMER + " (\n" +
                "    M_CIF_CUST_ID CHAR(9),\n" +
                "    M_CIF_TIE_BREAKER_N SMALLINT,\n" +
                "    M_CIF_COID_N SMALLINT,\n" +
                "    M_CIF_BK_SER_1 CHAR(3),\n" +
                "    M_CIF_BK_SER_2 CHAR(3),\n" +
                "    M_CIF_BK_SER_3 CHAR(3),\n" +
                "    PRIMARY KEY (M_CIF_COID_N, M_CIF_CUST_ID, M_CIF_TIE_BREAKER_N)\n" +
                ") WITH \"\n" +
                "    TEMPLATE=" + TEMPLATE_MEMORY_REPLICATED + ",\n" +
                "    KEY_TYPE=com.model.CIFCustomerKey,\n" +
                "    VALUE_TYPE=com.model.CIFCustomerValue\n" +
                "\";");

        sql("CREATE TABLE IF NOT EXISTS " + TABLE_IDS_RELATIONSHIP + " (\n" +
                "    M_CO_ID_N SMALLINT,\n" +
                "    ACCT_NO DECIMAL(15,0) ,\n" +
                "    M_PRDCT_CD_N smallint ,\n" +
                "    M_CIF_COID_N SMALLINT,\n" +
                "    M_CIF_CUST_ID CHAR(9),\n" +
                "    M_CIF_TIE_BREAKER_N smallint,\n" +
                "    M_AIF_ACT_TO_CUST_REL_CODE CHAR(3),\n" +
                "    M_AIF_ACT_TO_CUST_PRIM_IND CHAR(1),\n" +
                "    M_AIF_ACT_TO_CUST_ANAL_IND CHAR(1),\n" +
                "    M_AIF_ACT_TO_CUST_PRIC_IND CHAR(1),\n" +
                "    PRIMARY KEY (M_CO_ID_N, acct_no, M_PRDCT_CD_N , M_CIF_COID_N, M_CIF_CUST_ID, M_CIF_TIE_BREAKER_N)\n" +
                ") WITH \"\n" +
                "    TEMPLATE=" + TEMPLATE_MEMORY_REPLICATED + ",\n" +
                "    KEY_TYPE=com.model.IDSRelationshipKey,\n" +
                "    VALUE_TYPE=com.model.IDSRelationshipValue\n" +
                "\";");

        sql("CREATE TABLE IF NOT EXISTS " + TABLE_AMF_SOURCES_SUMMARY + " (\n" +
                "    M_CO_ID_N INT,\n" +
                "    ACCT_NO DECIMAL(15,0),\n" +
                "    M_PRDCT_CD_N INT,\n" +
                "    M_CURR_BAL DECIMAL(13,2),\n" +
                "    M_CURR_COLL_BAL DECIMAL(13,2),\n" +
                "    M_AMT_REST DECIMAL(13,2),\n" +
                "PRIMARY KEY (M_CO_ID_N, ACCT_NO, M_PRDCT_CD_N)\n" +
                ")WITH \"\n" +
                "    AFFINITY_KEY=ACCT_NO,\n" +
                "    TEMPLATE=" + TEMPLATE_MEMORY_PARTITIONED + ",\n" +
                "    KEY_TYPE=com.model.AMFKey,\n" +
                "    VALUE_TYPE=com.model.AMFSummaryValue\n" +
                "\";");

        //TODO: what's with the key_type and value_type?

    }

    private void pupulateData() {
        //TODO: implement
    }

    private void executeJoin() {
        //TODO: implement
    }

    private void checkJoinResult() {
        //TODO: implement
    }

    /**
     * @param sql SQL query.
     * @param args Query parameters.
     * @return Results cursor.
     */
    private FieldsQueryCursor<List<?>> sql(String sql, Object... args) {
        return grid(0).context().query().querySqlFields(new SqlFieldsQuery(sql)
                .setArgs(args), false);
    }
}
