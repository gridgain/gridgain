package com.gridgain.experiment;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.DataRegionMetrics;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.binary.nextgen.BikeConverterRegistry;

public class ExperimentServerNode {
    public static void main(String[] args) throws Exception {
        System.setProperty("bike.row.format", "true");
//        System.setProperty("h2.objectCache", "false");

        Ignite ignite = Ignition.start("config/server.xml");

//        ignite.cluster().active(true);

        Thread spaceMetricsThread = new Thread(() -> {
            try {
                for (int i = 0; i <= Integer.MAX_VALUE; i++) {
                    TimeUnit.SECONDS.sleep(50);

                    DataRegionMetrics m = ignite.dataRegionMetrics("Default_Region");

                    System.err.println("Used space: " + m.getUsedSpaceEx());
                    System.err.println("Fill factor: " + m.getPagesFillFactor());
                    BikeConverterRegistry.binStat.dump();
                    BikeConverterRegistry.bikeStat.dump();
                }
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
        spaceMetricsThread.setDaemon(true);
        spaceMetricsThread.start();

        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        String sql;
        while ((sql = br.readLine()) != null) {
            if (sql.isEmpty())
                sql = "select abbr7_extract_cyc_dt, abbr7_abbr6_file_name, abbr7_submsn_dt, abbr7_cwf_rqst_rec_id, abbr7_line_abbr1_cd, abbr1_1_mdfr_cd, abbr1_2_mdfr_cd, abbr1_3_mdfr_cd, abbr1_4_mdfr_cd, abbr1_5_mdfr_cd, abbr7_line_srvc_unit_qty, abbr7_prncpl_dgns_cd, abbr7_dgns_1_cd, abbr7_dgns_2_cd, abbr7_dgns_3_cd, abbr7_dgns_4_cd, abbr7_dgns_5_cd, abbr7_dgns_6_cd, abbr7_dgns_7_cd, abbr7_dgns_8_cd, abbr7_dgns_9_cd, abbr7_dgns_10_cd, abbr7_dgns_11_cd, abbr7_dgns_12_cd, abbr7_dgns_13_cd, abbr7_dgns_14_cd, abbr7_dgns_15_cd, abbr7_dgns_16_cd, abbr7_dgns_17_cd, abbr7_dgns_18_cd, abbr7_dgns_19_cd, abbr7_dgns_20_cd, abbr7_dgns_21_cd, abbr7_dgns_22_cd, abbr7_dgns_23_cd, abbr7_dgns_24_cd, abbr7_dgns_25_cd, abbr7_type_srvc_cd, abbr7_line_from_dt, abbr7_line_thru_dt, abbr7_pos_cd, abbr7_line_pos_zip_cd, abbr7_fac_prvdr_zip5_cd, abbr7_fac_prvdr_zip4_cd, abbr7_bill_fac_type_cd, abbr7_bill_clsfctn_cd, abbr7_bill_freq_cd, abbr7_type_cd, abbr7_xovr_cd, abbr7_bill_type_cd, abbr7_line_rev_cntr_cd, abbr7_line_rev_unit_num, abbr7_from_dt, abbr7_thru_dt, abbr7_ncvrd_chrg_amt, abbr7_line_ncvrd_chrg_amt, abbr7_rlt_cond_cd_grp, abbr7_actv_care_from_dt, abbr7_dschrg_dt, abbr7_ocrnc_span_1_cd, abbr7_ocrnc_span_1_from_dt, abbr7_ocrnc_span_1_to_dt, abbr7_ocrnc_span_2_cd, abbr7_ocrnc_span_2_from_dt, abbr7_ocrnc_span_2_to_dt, abbr7_ocrnc_span_3_cd, abbr7_ocrnc_span_3_from_dt, abbr7_ocrnc_span_3_to_dt, abbr7_ocrnc_span_4_cd, abbr7_ocrnc_span_4from_dt, abbr7_ocrnc_span_4_to_dt, abbr7_ocrnc_span_5_cd, abbr7_ocrnc_span_5_from_dt, abbr7_ocrnc_span_5_to_dt, abbr7_ocrnc_span_6_cd, abbr7_ocrnc_span_6_from_dt, abbr7_ocrnc_span_6_to_dt, abbr7_ocrnc_span_7_cd, abbr7_ocrnc_span_7_from_dt, abbr7_ocrnc_span_7_to_dt, abbr7_ocrnc_span_8_cd, abbr7_ocrnc_span_8_from_dt, abbr7_ocrnc_span_8_to_dt, abbr7_ocrnc_span_9_cd, abbr7_ocrnc_span_9_from_dt, abbr7_ocrnc_span_9_to_dt, abbr7_ocrnc_span_10_cd, abbr7_ocrnc_span_10_from_dt, abbr7_ocrnc_span_10_to_dt, abbr7_rev_apc_hipps_cd, abbr8_ptnt_stus_cd, abbr7_cwf_actn_cd, abbr7_prcdr_cd_grp, abbr7_sbmt_chrg_amt, abbr7_line_tot_chrg_amt, abbr7_prvdr_pmt_amt, abbr7_pmt_amt, abbr7_line_prvdr_pmt_amt, abbr7_alowd_chrg_amt, abbr7_line_alowd_chrg_amt, abbr7_rndrg_prvdr_npi_num, prvdr_mdcr_id, abbr7_prvdr_tax_num, abbr7_atndg_prvdr_npi_num, abbr7_atndg_fed_prvdr_spclty_cd, abbr7_oprtg_prvdr_npi_num, abbr7_othr_prvdr_npi_num, abbr7_othr_prvdr_2_npi_num, abbr7_blg_prvdr_npi_num, abbr7_blg_prvdr_pin_num, abbr7_rndrg_fed_prvdr_spclty_cd, abbr7_fac_prvdr_txnmy_cd, abbr7_rfrg_prvdr_npi_num, abbr7_rfrg_fed_prvdr_spclty_cd, abbr7_hic_num, abbr7_insrd_birth_dt, abbr7_ptnt_death_dt, abbr8_rrb_num, abbr7_crnt_stus_cd, abbr7_pd_stus_cd, abbr7_admtg_dgns_cd, abbr7_ptnt_visit_rsn_1_cd, abbr7_ptnt_visit_rsn_2_cd, abbr7_ptnt_visit_rsn_3_cd, abbr7_abbr8_state_cd, abbr7_admsn_type_cd, abbr7_blg_fed_prvdr_spclty_cd, abbr7_blg_prvdr_oscar_num, abbr7_fac_prvdr_oscar_num, abbr7_fac_prvdr_emer_ind, abbr7_prvdr_fac_cd, abbr7_harmonized_cntrctr_num, abbr7_idr_ld_dt, abbr7_finl_actn_ind, abbr7_line_ndc_qty, abbr7_line_ndc_qty_qlfyr_cd, abbr7_line_abbr8_pd_amt, abbr7_line_othr_tp_pd_amt, abbr7_line_cvrd_pd_amt, abbr7_line_alowd_unit_qty, abbr7_line_ansthsa_unit_cnt, abbr7_line_instnl_adjstd_amt, abbr7_line_instnl_apc_bufr_cd, abbr7_line_instnl_rev_ctr_dt, abbr7_rev_pricng_ind_cd, abbr7_val_cd, abbr7_val_amt, abbr7_dschrg_hr, abbr7_instnl_cvrd_day_cnt, abbr7_instnl_drg_outlier_day_cnt, abbr7_instnl_drg_outlier_amt, abbr7_drop_off_zip4_cd, abbr7_pckp_zip4_cd, abbr7_prcdr_cd, abbr7_prcdr_1_cd, abbr7_prcdr_2_cd, abbr7_prcdr_3_cd, abbr7_prcdr_4_cd, abbr7_prcdr_5_cd, abbr7_prcdr_6_cd, dgns_drg_cd, abbr7_line_ndc_cd, abbr7_drop_off_zip5_cd, abbr7_pckp_zip5_cd, abbr7_line_dgns_cd, abbr7_line_pos_last_name, abbr7_line_pos_1st_name, abbr7_line_pos_mdl_name, abbr7_line_pos_line_1_adr, abbr7_line_pos_line_2_adr, abbr7_line_pos_city_name, abbr7_line_pos_state_cd, abbr7_abbr8_zip_cd, abbr7_blg_prvdr_usps_state_cd, abbr7_dgns_e_cd, abbr7_line_mdcr_ddctbl_amt, abbr8_hic_num, abbr8_eqtbl_bic_hicn_num, abbr7_fac_prvdr_npi_num, abbr7_dgns_prcdr_icd_ind, abbr7_mbi, sub_bid_ind, abbr7_pd_dt, abbr7_abbr8_prcng_state_cd, abbr7_line_rndrg_prvdr_npi_num, abbr7_blg_prvdr_npi_num_orig, abbr7_prvdr_sbmtd_npi_num, abbr7_sbmtr_id, abbr7_abbr8_pd_amt, abbr7_prcr_rtrn_cd, abbr7_admsn_src_cd, abbr7_poa_ind, abbr7_line_stus_ind from abbr5";

            IgniteCache<Object, Object> cache = ignite.cache("abbr5");
            try {
                int iters = 10;
                long[] cnts = new long[iters];
                for (int i = 0; i < iters; i++) {
                    System.gc();
                    long t0 = System.currentTimeMillis();
                    long cnt = 0;

                    for (List<?> row : cache.query(new SqlFieldsQuery(sql).setLazy(true)))
                        cnt += row.size();

                    long t1 = System.currentTimeMillis();
                    System.err.println("Time: " + (t1 - t0));

                    cnts[i] = cnt;
                }

                System.err.println(Arrays.toString(cnts));
            }
            catch (Throwable e) {
                e.printStackTrace();
            }
        }
    }
}
