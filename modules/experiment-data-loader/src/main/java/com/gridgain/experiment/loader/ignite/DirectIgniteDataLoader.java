package com.gridgain.experiment.loader.ignite;

import com.gridgain.experiment.gen.ConditionalObjectOverride;
import com.gridgain.experiment.gen.CustomObjectRandomizer;
import com.gridgain.experiment.model.abbr5Key;
import com.gridgain.experiment.model.abbr5Record;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;

public class DirectIgniteDataLoader {

    private static final int OBJ_COUNT = 20_000;

    private static final double abbr3_MULTIPLIER = 1.0;

    private static final double abbr4_MULTIPLIER = 15.0;

    private static final double abbr1_MULTIPLIER = 1.25;

    private static final double abbr2_MULTIPLIER = 2.0;

    private static final double abbr6_MULTIPLIER = 500;

    private static ConditionalObjectOverride dataSetClauseConditions;

    private static Map<String, Integer> lengths = new HashMap<>();

    public static void main(String[] args) throws IllegalAccessException, InstantiationException {

//        int abbr5Size = 80_000;
        int abbr5Size = 500_000;
        if (args.length > 0)
            abbr5Size = Integer.valueOf(args[0]);


        initializeConditionalOverrides();

        try (Ignite ignite = Ignition.start("config/client.xml")) {
            populateKeyValueCache("abbr5", abbr5Key.class, abbr5Record.class, ignite, abbr5Size);
            //if (args.length > 1) {
//                populateCache("abbr4_p10c", abbr4ProvierUpicRecord.class, ignite, 2_600_000);
//                populateCache("abbr3_RFRNC", abbr3RfrncRecord.class, ignite, 206_000);
//                populateCache("abbr2_e4t_STUS_NPI", abbr2e4tStusNpiRecord.class, ignite, 3_300_000);
//                populateCache("abbr1_CD", abbr1CdRecord.class, ignite, 23_600);
            //}
        }
    }

    public static <K, V> void populateKeyValueCache(String cacheName,Class<K> k,  Class<V> v, Ignite ignite, int size) throws InstantiationException, IllegalAccessException {
        try(IgniteDataStreamer<K, V> dataStreamer = ignite.dataStreamer(cacheName)) {
            dataStreamer.perNodeBufferSize(IgniteDataStreamer.DFLT_PER_NODE_BUFFER_SIZE);
            dataStreamer.perNodeParallelOperations(IgniteDataStreamer.DFLT_PARALLEL_OPS_MULTIPLIER);

            dataStreamer.allowOverwrite(false);
            dataStreamer.skipStore(true);

            CustomObjectRandomizer keyRandomizer = new CustomObjectRandomizer();
            CustomObjectRandomizer recordRandomizer = new CustomObjectRandomizer();
            keyRandomizer.setConditionalObjectOverride(dataSetClauseConditions);
            recordRandomizer.setConditionalObjectOverride(dataSetClauseConditions);

            keyRandomizer.init(k, null);
            recordRandomizer.init(v, null);

            keyRandomizer.setStrLengthCap(20);
            recordRandomizer.setStrLengthCap(20);

            for ( long i = 0; i < size; i++) {
                K key = keyRandomizer.generateRandomObject(i);
                V record = recordRandomizer.generateRandomObject(i);

                dataStreamer.addData(key, record);

                if (i % 10000 == 0)
                    System.out.println("Loaded to ignite - " + i);
            }

            System.out.println(cacheName + " Done");
        }
    }

    public static <T> void populateCache(String cacheName, Class<T> cls, Ignite ignite, int size) throws InstantiationException, IllegalAccessException {
        try(IgniteDataStreamer<UUID, T> dataStreamer = ignite.dataStreamer(cacheName)) {
            dataStreamer.perNodeBufferSize(IgniteDataStreamer.DFLT_PER_NODE_BUFFER_SIZE);
            dataStreamer.perNodeParallelOperations(IgniteDataStreamer.DFLT_PARALLEL_OPS_MULTIPLIER);

            dataStreamer.allowOverwrite(false);
            dataStreamer.skipStore(true);
            CustomObjectRandomizer recordRandomizer = new CustomObjectRandomizer();
            recordRandomizer.init(cls, null);

            recordRandomizer.setConditionalObjectOverride(dataSetClauseConditions);

            recordRandomizer.setStrLengthCap(10);

            for ( long i = 0; i < size; i++) {
                T record = recordRandomizer.generateRandomObject(i);

                dataStreamer.addData(UUID.randomUUID(), record);

                if (i % 10000 == 0)
                    System.out.println("Loaded to ignite - " + i);
            }

            System.out.println(cacheName + " Done");
        }
    }

    private static void initializeConditionalOverrides() {
        ConditionalObjectOverride dataSetConditions = new ConditionalObjectOverride();

        // abbr7_TYPE_CD 2700, 2081, 2082 // distinct count abbr7_type_cd	26

        // abbr7_LINE_abbr1_CD
        // '','','','','','','', '','','',''
        // '91010' AND '96120' btw
        // '70010' AND '78816' btw
        // distinct count abbr7_line_abbr1_cd	55720

        // abbr7_src_type A,B,DME // distinct count abbr7_src_type  3

        // rfrnc_data_id 'DGNS_CD' // distinct count rfrnc_data_id 27

        // WHERE clauses with hardcoded values

        ArrayList<String> rfrncDataIdPredefinedSet = new ArrayList<>();
        rfrncDataIdPredefinedSet.add("DGNS_CD");
        dataSetConditions.setWhereClauseConditionOnColumn("rfrnc_data_id", rfrncDataIdPredefinedSet, 27);

        ArrayList<String> abbr7SrcTypePredefinedSet = new ArrayList<>();
        //abbr7SrcTypePredefinedSet.add("A");
        abbr7SrcTypePredefinedSet.add("B");
        //abbr7SrcTypePredefinedSet.add("DME");
        dataSetConditions.setWhereClauseConditionOnColumn("abbr7_src_type", abbr7SrcTypePredefinedSet, 1);

        ArrayList<String> abbr7TypeCdPredefinedSet = new ArrayList<>();
        abbr7TypeCdPredefinedSet.add("2700");
        abbr7TypeCdPredefinedSet.add("2081");
        abbr7TypeCdPredefinedSet.add("2082");
        dataSetConditions.setWhereClauseConditionOnColumn("abbr7_type_cd", abbr7TypeCdPredefinedSet, 1);

        ArrayList<String> abbr7Lineabbr1CdPredefinedSet = new ArrayList<>();
        abbr7Lineabbr1CdPredefinedSet.add("G0106");
        abbr7Lineabbr1CdPredefinedSet.add("G0130");
        abbr7Lineabbr1CdPredefinedSet.add("G0202");
        abbr7Lineabbr1CdPredefinedSet.add("G0204");
        abbr7Lineabbr1CdPredefinedSet.add("G0206");
        abbr7Lineabbr1CdPredefinedSet.add("G0248");
        abbr7Lineabbr1CdPredefinedSet.add("G0249");
        abbr7Lineabbr1CdPredefinedSet.add("G0279");
        abbr7Lineabbr1CdPredefinedSet.add("G0398");
        abbr7Lineabbr1CdPredefinedSet.add("G0399");
        abbr7Lineabbr1CdPredefinedSet.add("G0400");

        for (int i = 70010; i < 78817; i++)
            abbr7Lineabbr1CdPredefinedSet.add(String.valueOf(i));

        for (int i = 91010; i < 96121; i++)
            abbr7Lineabbr1CdPredefinedSet.add(String.valueOf(i));

        dataSetConditions.setWhereClauseConditionOnColumn("abbr7_line_abbr1_cd", abbr7Lineabbr1CdPredefinedSet, 55720);

        // JOIN columns

        setDefaultPredefinedRangedData(dataSetConditions, "prvdr_npi_num", 200000);
        dataSetConditions.addFieldAlias("abbr7_rfrg_prvdr_npi_num", "prvdr_npi_num");
        dataSetConditions.addFieldAlias("abbr7_rndrg_prvdr_npi_num", "prvdr_npi_num");

        //

        setDefaultPredefinedRangedData(dataSetConditions, "cd", 201357);
        dataSetConditions.addFieldAlias("abbr7_prncpl_dgns_cd", "cd");
        dataSetConditions.addFieldAlias("abbr7_dgns_1_cd", "cd");

        ArrayList<String> abbr7FinlActnPredefinedSet = new ArrayList<>();
        abbr7FinlActnPredefinedSet.add("Y");
        abbr7FinlActnPredefinedSet.add("N");
        dataSetConditions.setWhereClauseConditionOnColumn("abbr7_finl_actn_ind", abbr7FinlActnPredefinedSet, 2);

        setDefaultPredefinedRangedData(dataSetConditions, "abbr7_prncpl_dgns_cd", 34564);
        setDefaultPredefinedRangedData(dataSetConditions, "abbr7_line_rev_cntr_cd", 102);
        setDefaultPredefinedRangedData(dataSetConditions, "abbr7_line_rev_unit_num", 10);
        setDefaultPredefinedRangedData(dataSetConditions, "abbr7_from_dt", 351);

        setDefaultPredefinedRangedData(dataSetConditions, "abbr7_line_abbr1_cd", 16226);
        setDefaultPredefinedRangedData(dataSetConditions, "abbr8_sk", 12642822);
        setDefaultPredefinedRangedData(dataSetConditions, "abbr7_line_from_dt", 10);
        setDefaultPredefinedRangedData(dataSetConditions, "abbr7_blg_prvdr_npi_num", 386440);
        setDefaultPredefinedRangedData(dataSetConditions, "abbr7_rndrg_prvdr_npi_num", 1006018);
        setDefaultPredefinedRangedData(dataSetConditions, "abbr7_line_prvdr_pmt_amt", 254400);
        setDefaultPredefinedRangedData(dataSetConditions, "e4t_id", 3248761);
        // WHERE IN clauses columns

        // TODO

        dataSetClauseConditions = dataSetConditions;
    }

    private static void setDefaultPredefinedRangedData(ConditionalObjectOverride coo, String colName, int selectivity) {
        ArrayList<String> predefinedSet = new ArrayList<>();
        if (colName.endsWith("_dt")) {
            long step = (long) ((1553197056413l - 1422819456413l) * 1.0 / selectivity);
            for (long i = 1422819456413l; i < 1553197056413l; i=i+step)
                predefinedSet.add(String.valueOf(i));
        } else {
            for (int i = 0; i < selectivity; i++)
                predefinedSet.add(String.valueOf(i));
        }
        coo.setWhereClauseConditionOnColumn(colName, predefinedSet, selectivity);
    }

    private static void loadObjectInfo(String fileName) throws IOException {
        for (String s : Files.readAllLines(Paths.get("generator/" + fileName))) {
            String[] info = s.split("=");

            lengths.put(info[0], new BigDecimal(info[1]).intValue());
        }
    }
}

