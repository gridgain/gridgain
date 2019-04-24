package com.gridgain.experiment.utils;

import com.gridgain.experiment.model.abbr1CdRecord;
import com.gridgain.experiment.model.abbr2e4tStusNpiRecord;
import com.gridgain.experiment.model.abbr3RfrncRecord;
import com.gridgain.experiment.model.abbr4ProvierUpicRecord;
import com.gridgain.experiment.model.abbr5Key;
import com.gridgain.experiment.model.abbr5Record;
import java.io.IOException;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryObjectExImpl;
import org.apache.ignite.lang.IgniteBiTuple;

/**
 *
 */
public class ObjectGenerator {
    static Map<String, Integer> lengths = new HashMap<>();

    public static void main(String[] args) throws SQLException, IOException {
        loadObjectInfo("abbr5.txt");
        loadObjectInfo("abbr4_p10c.txt");
        loadObjectInfo("abbr3_RFRNC.txt");
        loadObjectInfo("abbr1_CD.txt");
        loadObjectInfo("abbr2_e4t_STUS_NPI.txt");

        checkSize();
    }

    private static void checkSize() throws SQLException {
        IgniteConfiguration cfg = new IgniteConfiguration();

        try (Ignite ignite = Ignition.start(cfg)) {
            Map.Entry e = buildabbr5Record(new ResultSet());

            ignite.getOrCreateCache("a").put(e.getKey(), e.getValue());

            System.out.println("SIZE:" + ((BinaryObjectExImpl)ignite.getOrCreateCache("a").withKeepBinary().get(e.getKey())).array().length);
        }
    }

    private static <T> Object buildCacheRecord(ResultSet rs, Class<T> cls) throws SQLException {
        if (abbr2e4tStusNpiRecord.class.equals(cls))
            return buildabbr2e4tStusNpiRecord(rs);
        else if (abbr3RfrncRecord.class.equals(cls))
            return buildabbr3RfrncRecord(rs);
        else if (abbr1CdRecord.class.equals(cls))
            return buildabbr1CdRecord(rs);
        else if (abbr4ProvierUpicRecord.class.equals(cls))
            return buildabbr4ProvierUpicRecord(rs);
        return null;
    }

    private static void loadObjectInfo(String fileName) throws IOException {
        for (String s : Files.readAllLines(Paths.get("generator/" + fileName))) {
            String[] info = s.split("=");

            lengths.put(info[0], new BigDecimal(info[1]).intValue());
        }
    }

    private static Map.Entry<Object, Object> buildabbr5Record(ResultSet rs) throws SQLException {
        abbr5Key currentKey = new abbr5Key();
        abbr5Record currentValue = new abbr5Record();

        currentKey.setabbr7_cntl_num(rs.getString("abbr7_cntl_num"));
        currentKey.setabbr7_line_num(rs.getInt("abbr7_line_num"));
        currentKey.setabbr7_cntrctr_num(rs.getString("abbr7_cntrctr_num"));
        currentKey.setabbr7_src_type(rs.getString("abbr7_src_type"));
        currentKey.setabbr8_sk(rs.getString("abbr8_sk"));

        if (rs.getDate("abbr7_extract_cyc_dt") != null)
            currentValue.setabbr7_extract_cyc_dt(rs.getDate("abbr7_extract_cyc_dt").getTime());
        currentValue.setabbr7_abbr6_file_name(rs.getString("abbr7_abbr6_file_name"));
        if (rs.getDate("abbr7_submsn_dt") != null)
            currentValue.setabbr7_submsn_dt(rs.getDate("abbr7_submsn_dt").getTime());
        currentValue.setabbr7_cwf_rqst_rec_id(rs.getString("abbr7_cwf_rqst_rec_id"));
        currentValue.setabbr7_line_abbr1_cd(rs.getString("abbr7_line_abbr1_cd"));
        currentValue.setabbr1_1_mdfr_cd(rs.getString("abbr1_1_mdfr_cd"));
        currentValue.setabbr1_2_mdfr_cd(rs.getString("abbr1_2_mdfr_cd"));
        currentValue.setabbr1_3_mdfr_cd(rs.getString("abbr1_3_mdfr_cd"));
        currentValue.setabbr1_4_mdfr_cd(rs.getString("abbr1_4_mdfr_cd"));
        currentValue.setabbr1_5_mdfr_cd(rs.getString("abbr1_5_mdfr_cd"));
        currentValue.setabbr7_line_srvc_unit_qty(rs.getBigDecimal("abbr7_line_srvc_unit_qty"));
        currentValue.setabbr7_prncpl_dgns_cd(rs.getString("abbr7_prncpl_dgns_cd"));
        currentValue.setabbr7_dgns_1_cd(rs.getString("abbr7_dgns_1_cd"));
        currentValue.setabbr7_dgns_2_cd(rs.getString("abbr7_dgns_2_cd"));
        currentValue.setabbr7_dgns_3_cd(rs.getString("abbr7_dgns_3_cd"));
        currentValue.setabbr7_dgns_4_cd(rs.getString("abbr7_dgns_4_cd"));
        currentValue.setabbr7_dgns_5_cd(rs.getString("abbr7_dgns_5_cd"));
        currentValue.setabbr7_dgns_6_cd(rs.getString("abbr7_dgns_6_cd"));
        currentValue.setabbr7_dgns_7_cd(rs.getString("abbr7_dgns_7_cd"));
        currentValue.setabbr7_dgns_8_cd(rs.getString("abbr7_dgns_8_cd"));
        currentValue.setabbr7_dgns_9_cd(rs.getString("abbr7_dgns_9_cd"));
        currentValue.setabbr7_dgns_11_cd(rs.getString("abbr7_dgns_11_cd"));
        currentValue.setabbr7_dgns_12_cd(rs.getString("abbr7_dgns_12_cd"));
        currentValue.setabbr7_dgns_13_cd(rs.getString("abbr7_dgns_13_cd"));
        currentValue.setabbr7_dgns_14_cd(rs.getString("abbr7_dgns_14_cd"));
        currentValue.setabbr7_dgns_15_cd(rs.getString("abbr7_dgns_15_cd"));
        currentValue.setabbr7_dgns_16_cd(rs.getString("abbr7_dgns_16_cd"));
        currentValue.setabbr7_dgns_17_cd(rs.getString("abbr7_dgns_17_cd"));
        currentValue.setabbr7_dgns_18_cd(rs.getString("abbr7_dgns_18_cd"));
        currentValue.setabbr7_dgns_19_cd(rs.getString("abbr7_dgns_19_cd"));
        currentValue.setabbr7_dgns_20_cd(rs.getString("abbr7_dgns_20_cd"));
        currentValue.setabbr7_dgns_21_cd(rs.getString("abbr7_dgns_21_cd"));
        currentValue.setabbr7_dgns_22_cd(rs.getString("abbr7_dgns_22_cd"));
        currentValue.setabbr7_dgns_23_cd(rs.getString("abbr7_dgns_23_cd"));
        currentValue.setabbr7_dgns_24_cd(rs.getString("abbr7_dgns_24_cd"));
        currentValue.setabbr7_dgns_25_cd(rs.getString("abbr7_dgns_25_cd"));
        currentValue.setabbr7_type_srvc_cd(rs.getString("abbr7_type_srvc_cd"));
        if (rs.getDate("abbr7_line_from_dt") != null)
            currentValue.setabbr7_line_from_dt(rs.getDate("abbr7_line_from_dt").getTime());
        if (rs.getDate("abbr7_line_thru_dt") != null)
            currentValue.setabbr7_line_thru_dt(rs.getDate("abbr7_line_thru_dt").getTime());
        currentValue.setabbr7_pos_cd(rs.getString("abbr7_pos_cd"));
        currentValue.setabbr7_line_pos_zip_cd(rs.getString("abbr7_line_pos_zip_cd"));
        currentValue.setabbr7_fac_prvdr_zip5_cd(rs.getString("abbr7_fac_prvdr_zip5_cd"));
        currentValue.setabbr7_fac_prvdr_zip4_cd(rs.getString("abbr7_fac_prvdr_zip4_cd"));
        currentValue.setabbr7_bill_fac_type_cd(rs.getString("abbr7_bill_fac_type_cd"));
        currentValue.setabbr7_bill_clsfctn_cd(rs.getInt("abbr7_bill_clsfctn_cd"));
        currentValue.setabbr7_bill_freq_cd(rs.getString("abbr7_bill_freq_cd"));
        currentValue.setabbr7_type_cd(rs.getString("abbr7_type_cd"));
        currentValue.setabbr7_xovr_cd(rs.getString("abbr7_xovr_cd"));
        currentValue.setabbr7_bill_type_cd(rs.getString("abbr7_bill_type_cd"));
        currentValue.setabbr7_line_rev_cntr_cd(rs.getString("abbr7_line_rev_cntr_cd"));
        currentValue.setabbr7_line_rev_unit_num(rs.getBigDecimal("abbr7_line_rev_unit_num"));
        if (rs.getDate("abbr7_from_dt") != null)
            currentValue.setabbr7_from_dt(rs.getDate("abbr7_from_dt").getTime());
        if (rs.getDate("abbr7_thru_dt") != null)
            currentValue.setabbr7_thru_dt(rs.getDate("abbr7_thru_dt").getTime());
        currentValue.setabbr7_ncvrd_chrg_amt(rs.getBigDecimal("abbr7_ncvrd_chrg_amt"));
        currentValue.setabbr7_line_ncvrd_chrg_amt(rs.getBigDecimal("abbr7_line_ncvrd_chrg_amt"));
        currentValue.setabbr7_rlt_cond_cd_grp(rs.getString("abbr7_rlt_cond_cd_grp"));
        if (rs.getDate("abbr7_actv_care_from_dt") != null)
            currentValue.setabbr7_actv_care_from_dt(rs.getDate("abbr7_actv_care_from_dt").getTime());
        if (rs.getDate("abbr7_dschrg_dt") != null)
            currentValue.setabbr7_dschrg_dt(rs.getDate("abbr7_dschrg_dt").getTime());
        currentValue.setabbr7_ocrnc_span_1_cd(rs.getString("abbr7_ocrnc_span_1_cd"));
        if (rs.getDate("abbr7_ocrnc_span_1_from_dt") != null)
            currentValue.setabbr7_ocrnc_span_1_from_dt(rs.getDate("abbr7_ocrnc_span_1_from_dt").getTime());
        if (rs.getDate("abbr7_ocrnc_span_1_to_dt") != null)
            currentValue.setabbr7_ocrnc_span_1_to_dt(rs.getDate("abbr7_ocrnc_span_1_to_dt").getTime());
        currentValue.setabbr7_ocrnc_span_2_cd(rs.getString("abbr7_ocrnc_span_2_cd"));
        if (rs.getDate("abbr7_ocrnc_span_2_from_dt") != null)
            currentValue.setabbr7_ocrnc_span_2_from_dt(rs.getDate("abbr7_ocrnc_span_2_from_dt").getTime());
        if (rs.getDate("abbr7_ocrnc_span_2_to_dt") != null)
            currentValue.setabbr7_ocrnc_span_2_to_dt(rs.getDate("abbr7_ocrnc_span_2_to_dt").getTime());
        currentValue.setabbr7_ocrnc_span_3_cd(rs.getString("abbr7_ocrnc_span_3_cd"));
        if (rs.getDate("abbr7_ocrnc_span_3_from_dt") != null)
            currentValue.setabbr7_ocrnc_span_3_from_dt(rs.getDate("abbr7_ocrnc_span_3_from_dt").getTime());
        if (rs.getDate("abbr7_ocrnc_span_3_to_dt") != null)
            currentValue.setabbr7_ocrnc_span_3_to_dt(rs.getDate("abbr7_ocrnc_span_3_to_dt").getTime());
        currentValue.setabbr7_ocrnc_span_4_cd(rs.getString("abbr7_ocrnc_span_4_cd"));
        if (rs.getDate("abbr7_ocrnc_span_4_from_dt") != null)
            currentValue.setabbr7_ocrnc_span_4from_dt(rs.getDate("abbr7_ocrnc_span_4_from_dt").getTime());
        if (rs.getDate("abbr7_ocrnc_span_4_to_dt") != null)
            currentValue.setabbr7_ocrnc_span_4_to_dt(rs.getDate("abbr7_ocrnc_span_4_to_dt").getTime());
        currentValue.setabbr7_ocrnc_span_5_cd(rs.getString("abbr7_ocrnc_span_5_cd"));
        if (rs.getDate("abbr7_ocrnc_span_5_from_dt") != null)
            currentValue.setabbr7_ocrnc_span_5_from_dt(rs.getDate("abbr7_ocrnc_span_5_from_dt").getTime());
        if (rs.getDate("abbr7_ocrnc_span_5_to_dt") != null)
            currentValue.setabbr7_ocrnc_span_5_to_dt(rs.getDate("abbr7_ocrnc_span_5_to_dt").getTime());
        currentValue.setabbr7_ocrnc_span_6_cd(rs.getString("abbr7_ocrnc_span_6_cd"));
        if (rs.getDate("abbr7_ocrnc_span_6_from_dt") != null)
            currentValue.setabbr7_ocrnc_span_6_from_dt(rs.getDate("abbr7_ocrnc_span_6_from_dt").getTime());
        if (rs.getDate("abbr7_ocrnc_span_6_to_dt") != null)
            currentValue.setabbr7_ocrnc_span_6_to_dt(rs.getDate("abbr7_ocrnc_span_6_to_dt").getTime());
        currentValue.setabbr7_ocrnc_span_7_cd(rs.getString("abbr7_ocrnc_span_7_cd"));
        if (rs.getDate("abbr7_ocrnc_span_7_from_dt") != null)
            currentValue.setabbr7_ocrnc_span_7_from_dt(rs.getDate("abbr7_ocrnc_span_7_from_dt").getTime());
        if (rs.getDate("abbr7_ocrnc_span_7_to_dt") != null)
            currentValue.setabbr7_ocrnc_span_7_to_dt(rs.getDate("abbr7_ocrnc_span_7_to_dt").getTime());
        currentValue.setabbr7_ocrnc_span_8_cd(rs.getString("abbr7_ocrnc_span_8_cd"));
        if (rs.getDate("abbr7_ocrnc_span_8_from_dt") != null)
            currentValue.setabbr7_ocrnc_span_8_from_dt(rs.getDate("abbr7_ocrnc_span_8_from_dt").getTime());
        if (rs.getDate("abbr7_ocrnc_span_8_to_dt") != null)
            currentValue.setabbr7_ocrnc_span_8_to_dt(rs.getDate("abbr7_ocrnc_span_8_to_dt").getTime());
        currentValue.setabbr7_ocrnc_span_9_cd(rs.getString("abbr7_ocrnc_span_9_cd"));
        if (rs.getDate("abbr7_ocrnc_span_9_from_dt") != null)
            currentValue.setabbr7_ocrnc_span_9_from_dt(rs.getDate("abbr7_ocrnc_span_9_from_dt").getTime());
        if (rs.getDate("abbr7_ocrnc_span_9_to_dt") != null)
            currentValue.setabbr7_ocrnc_span_9_to_dt(rs.getDate("abbr7_ocrnc_span_9_to_dt").getTime());
        currentValue.setabbr7_ocrnc_span_10_cd(rs.getString("abbr7_ocrnc_span_10_cd"));
        if (rs.getDate("abbr7_ocrnc_span_10_from_dt") != null)
            currentValue.setabbr7_ocrnc_span_10_from_dt(rs.getDate("abbr7_ocrnc_span_10_from_dt").getTime());
        if (rs.getDate("abbr7_ocrnc_span_10_to_dt") != null)
            currentValue.setabbr7_ocrnc_span_10_to_dt(rs.getDate("abbr7_ocrnc_span_10_to_dt").getTime());
        currentValue.setabbr7_rev_apc_hipps_cd(rs.getString("abbr7_rev_apc_hipps_cd"));
        currentValue.setabbr8_ptnt_stus_cd(rs.getString("abbr8_ptnt_stus_cd"));
        currentValue.setabbr7_cwf_actn_cd(rs.getString("abbr7_cwf_actn_cd"));
        currentValue.setabbr7_prcdr_cd_grp(rs.getString("abbr7_prcdr_cd_grp"));
        currentValue.setabbr7_sbmt_chrg_amt(rs.getBigDecimal("abbr7_sbmt_chrg_amt"));
        currentValue.setabbr7_line_tot_chrg_amt(rs.getBigDecimal("abbr7_line_tot_chrg_amt"));
        currentValue.setabbr7_prvdr_pmt_amt(rs.getBigDecimal("abbr7_prvdr_pmt_amt"));
        currentValue.setabbr7_pmt_amt(rs.getBigDecimal("abbr7_pmt_amt"));
        currentValue.setabbr7_line_prvdr_pmt_amt(rs.getBigDecimal("abbr7_line_prvdr_pmt_amt"));
        currentValue.setabbr7_alowd_chrg_amt(rs.getBigDecimal("abbr7_alowd_chrg_amt"));
        currentValue.setabbr7_line_alowd_chrg_amt(rs.getBigDecimal("abbr7_line_alowd_chrg_amt"));
        currentValue.setabbr7_rndrg_prvdr_npi_num(rs.getString("abbr7_rndrg_prvdr_npi_num"));
        currentValue.setPrvdr_mdcr_id(rs.getString("prvdr_mdcr_id"));
        currentValue.setabbr7_prvdr_tax_num(rs.getString("abbr7_prvdr_tax_num"));
        currentValue.setabbr7_atndg_prvdr_npi_num(rs.getString("abbr7_atndg_prvdr_npi_num"));
        currentValue.setabbr7_atndg_fed_prvdr_spclty_cd(rs.getString("abbr7_atndg_fed_prvdr_spclty_cd"));
        currentValue.setabbr7_oprtg_prvdr_npi_num(rs.getString("abbr7_oprtg_prvdr_npi_num"));
        currentValue.setabbr7_othr_prvdr_npi_num(rs.getString("abbr7_othr_prvdr_npi_num"));
        currentValue.setabbr7_othr_prvdr_2_npi_num(rs.getString("abbr7_othr_prvdr_2_npi_num"));
        currentValue.setabbr7_blg_prvdr_npi_num(rs.getString("abbr7_blg_prvdr_npi_num"));
        currentValue.setabbr7_blg_prvdr_pin_num(rs.getString("abbr7_blg_prvdr_pin_num"));
        currentValue.setabbr7_rndrg_fed_prvdr_spclty_cd(rs.getString("abbr7_rndrg_fed_prvdr_spclty_cd"));
        currentValue.setabbr7_fac_prvdr_txnmy_cd(rs.getString("abbr7_fac_prvdr_txnmy_cd"));
        currentValue.setabbr7_rfrg_prvdr_npi_num(rs.getString("abbr7_rfrg_prvdr_npi_num"));
        currentValue.setabbr7_rfrg_fed_prvdr_spclty_cd(rs.getString("abbr7_rfrg_fed_prvdr_spclty_cd"));
        currentValue.setabbr7_hic_num(rs.getString("abbr7_hic_num"));
        if (rs.getDate("abbr7_insrd_birth_dt") != null)
            currentValue.setabbr7_insrd_birth_dt(rs.getDate("abbr7_insrd_birth_dt").getTime());
        if (rs.getDate("abbr7_ptnt_death_dt") != null)
            currentValue.setabbr7_ptnt_death_dt(rs.getDate("abbr7_ptnt_death_dt").getTime());
        currentValue.setabbr8_rrb_num(rs.getString("abbr8_rrb_num"));
        currentValue.setabbr7_crnt_stus_cd(rs.getString("abbr7_crnt_stus_cd"));
        currentValue.setabbr7_pd_stus_cd(rs.getString("abbr7_pd_stus_cd"));
        currentValue.setabbr7_admtg_dgns_cd(rs.getString("abbr7_admtg_dgns_cd"));
        currentValue.setabbr7_ptnt_visit_rsn_1_cd(rs.getString("abbr7_ptnt_visit_rsn_1_cd"));
        currentValue.setabbr7_ptnt_visit_rsn_2_cd(rs.getString("abbr7_ptnt_visit_rsn_2_cd"));
        currentValue.setabbr7_ptnt_visit_rsn_3_cd(rs.getString("abbr7_ptnt_visit_rsn_3_cd"));
        currentValue.setabbr7_abbr8_state_cd(rs.getString("abbr7_abbr8_state_cd"));
        currentValue.setabbr7_admsn_type_cd(rs.getString("abbr7_admsn_type_cd"));
        currentValue.setabbr7_blg_fed_prvdr_spclty_cd(rs.getString("abbr7_blg_fed_prvdr_spclty_cd"));
        currentValue.setabbr7_blg_prvdr_oscar_num(rs.getString("abbr7_blg_prvdr_oscar_num"));
        currentValue.setabbr7_fac_prvdr_oscar_num(rs.getString("abbr7_fac_prvdr_oscar_num"));
        currentValue.setabbr7_fac_prvdr_emer_ind(rs.getString("abbr7_fac_prvdr_emer_ind"));
        currentValue.setabbr7_prvdr_fac_cd(rs.getString("abbr7_prvdr_fac_cd"));
        currentValue.setabbr7_harmonized_cntrctr_num(rs.getString("abbr7_harmonized_cntrctr_num"));
        if (rs.getDate("abbr7_idr_ld_dt") != null)
            currentValue.setabbr7_idr_ld_dt(rs.getDate("abbr7_idr_ld_dt").getTime());
        currentValue.setabbr7_finl_actn_ind(rs.getString("abbr7_finl_actn_ind"));
        currentValue.setabbr7_line_ndc_qty(rs.getBigDecimal("abbr7_line_ndc_qty"));
        currentValue.setabbr7_line_ndc_qty_qlfyr_cd(rs.getString("abbr7_line_ndc_qty_qlfyr_cd"));
        currentValue.setabbr7_line_abbr8_pd_amt(rs.getBigDecimal("abbr7_line_abbr8_pd_amt"));
        currentValue.setabbr7_line_othr_tp_pd_amt(rs.getBigDecimal("abbr7_line_othr_tp_pd_amt"));
        currentValue.setabbr7_line_cvrd_pd_amt(rs.getBigDecimal("abbr7_line_cvrd_pd_amt"));
        currentValue.setabbr7_line_alowd_unit_qty(rs.getBigDecimal("abbr7_line_alowd_unit_qty"));
        currentValue.setabbr7_line_ansthsa_unit_cnt(rs.getBigDecimal("abbr7_line_ansthsa_unit_cnt"));
        currentValue.setabbr7_line_instnl_adjstd_amt(rs.getBigDecimal("abbr7_line_instnl_adjstd_amt"));
        currentValue.setabbr7_line_instnl_apc_bufr_cd(rs.getString("abbr7_line_instnl_apc_bufr_cd"));
        if (rs.getDate("abbr7_line_instnl_rev_ctr_dt") != null)
            currentValue.setabbr7_line_instnl_rev_ctr_dt(rs.getDate("abbr7_line_instnl_rev_ctr_dt").getTime());
        currentValue.setabbr7_rev_pricng_ind_cd(rs.getString("abbr7_rev_pricng_ind_cd"));
        currentValue.setabbr7_val_cd(rs.getString("abbr7_val_cd"));
        currentValue.setabbr7_val_amt(rs.getString("abbr7_val_amt"));
        currentValue.setabbr7_dschrg_hr(rs.getBigDecimal("abbr7_dschrg_hr"));
        currentValue.setabbr7_instnl_cvrd_day_cnt(rs.getBigDecimal("abbr7_instnl_cvrd_day_cnt"));
        currentValue.setabbr7_instnl_drg_outlier_day_cnt(rs.getBigDecimal("abbr7_instnl_drg_outlier_day_cnt"));
        currentValue.setabbr7_instnl_drg_outlier_amt(rs.getBigDecimal("abbr7_instnl_drg_outlier_amt"));
        currentValue.setabbr7_drop_off_zip4_cd(rs.getString("abbr7_drop_off_zip4_cd"));
        currentValue.setabbr7_pckp_zip4_cd(rs.getString("abbr7_pckp_zip4_cd"));
        currentValue.setabbr7_prcdr_cd(rs.getString("abbr7_prcdr_cd"));
        currentValue.setabbr7_prcdr_1_cd(rs.getString("abbr7_prcdr_1_cd"));
        currentValue.setabbr7_prcdr_2_cd(rs.getString("abbr7_prcdr_2_cd"));
        currentValue.setabbr7_prcdr_3_cd(rs.getString("abbr7_prcdr_3_cd"));
        currentValue.setabbr7_prcdr_4_cd(rs.getString("abbr7_prcdr_4_cd"));
        currentValue.setabbr7_prcdr_5_cd(rs.getString("abbr7_prcdr_5_cd"));
        currentValue.setabbr7_prcdr_6_cd(rs.getString("abbr7_prcdr_6_cd"));
        currentValue.setDgns_drg_cd(rs.getBigDecimal("dgns_drg_cd"));
        currentValue.setabbr7_line_ndc_cd(rs.getString("abbr7_line_ndc_cd"));
        currentValue.setabbr7_drop_off_zip5_cd(rs.getString("abbr7_drop_off_zip5_cd"));
        currentValue.setabbr7_pckp_zip5_cd(rs.getString("abbr7_pckp_zip5_cd"));
        currentValue.setabbr7_line_dgns_cd(rs.getString("abbr7_line_dgns_cd"));
        currentValue.setabbr7_line_pos_last_name(rs.getString("abbr7_line_pos_last_name"));
        currentValue.setabbr7_line_pos_1st_name(rs.getString("abbr7_line_pos_1st_name"));
        currentValue.setabbr7_line_pos_mdl_name(rs.getString("abbr7_line_pos_mdl_name"));
        currentValue.setabbr7_line_pos_line_1_adr(rs.getString("abbr7_line_pos_line_1_adr"));
        currentValue.setabbr7_line_pos_line_2_adr(rs.getString("abbr7_line_pos_line_2_adr"));
        currentValue.setabbr7_line_pos_city_name(rs.getString("abbr7_line_pos_city_name"));
        currentValue.setabbr7_line_pos_state_cd(rs.getString("abbr7_line_pos_state_cd"));
        currentValue.setabbr7_abbr8_zip_cd(rs.getString("abbr7_abbr8_zip_cd"));
        currentValue.setabbr7_blg_prvdr_usps_state_cd(rs.getString("abbr7_blg_prvdr_usps_state_cd"));
        currentValue.setabbr7_dgns_e_cd(rs.getString("abbr7_dgns_e_cd"));
        currentValue.setabbr7_line_mdcr_ddctbl_amt(rs.getBigDecimal("abbr7_line_mdcr_ddctbl_amt"));
        currentValue.setabbr8_hic_num(rs.getString("abbr8_hic_num"));
        currentValue.setabbr8_eqtbl_bic_hicn_num(rs.getString("abbr8_eqtbl_bic_hicn_num"));
        currentValue.setabbr7_fac_prvdr_npi_num(rs.getString("abbr7_fac_prvdr_npi_num"));
        currentValue.setabbr7_dgns_prcdr_icd_ind(rs.getString("abbr7_dgns_prcdr_icd_ind"));
        currentValue.setabbr7_mbi(rs.getString("abbr7_mbi"));
        currentValue.setSub_bid_ind(rs.getString("sub_bid_ind"));
        if (rs.getDate("abbr7_pd_dt") != null)
            currentValue.setabbr7_pd_dt(rs.getDate("abbr7_pd_dt").getTime());
        currentValue.setabbr7_abbr8_prcng_state_cd(rs.getString("abbr7_abbr8_prcng_state_cd"));
        currentValue.setabbr7_line_rndrg_prvdr_npi_num(rs.getString("abbr7_line_rndrg_prvdr_npi_num"));
        currentValue.setabbr7_blg_prvdr_npi_num_orig(rs.getString("abbr7_blg_prvdr_npi_num_orig"));
        currentValue.setabbr7_prvdr_sbmtd_npi_num(rs.getString("abbr7_prvdr_sbmtd_npi_num"));
        currentValue.setabbr7_sbmtr_id(rs.getString("abbr7_sbmtr_id"));
        currentValue.setabbr7_abbr8_pd_amt(rs.getBigDecimal("abbr7_abbr8_pd_amt"));
        currentValue.setabbr7_prcr_rtrn_cd(rs.getString("abbr7_prcr_rtrn_cd"));
        currentValue.setabbr7_admsn_src_cd(rs.getString("abbr7_admsn_src_cd"));
        currentValue.setabbr7_poa_ind(rs.getString("abbr7_poa_ind"));
        currentValue.setabbr7_line_stus_ind(rs.getString("abbr7_line_stus_ind"));

        return new IgniteBiTuple<>(currentKey, currentValue);
    }


    private static abbr4ProvierUpicRecord buildabbr4ProvierUpicRecord(ResultSet rs) throws SQLException {
        abbr4ProvierUpicRecord current = new abbr4ProvierUpicRecord();

        current.setPrvdr_prctc_state_upic_cd(rs.getString("prvdr_prctc_state_upic_cd"));
        current.setPrvdr_prctc_state_desc(rs.getString("prvdr_prctc_state_desc"));
        current.setPrvdr_sk(rs.getLong("prvdr_sk"));
        current.setPrvdr_gnrc_id_num(rs.getString("prvdr_gnrc_id_num"));
        current.setPrvdr_id_qlfyr_cd(rs.getString("prvdr_id_qlfyr_cd"));
        current.setPrvdr_id_qlfyr_cd_desc(rs.getString("prvdr_id_qlfyr_cd_desc"));
        if (rs.getDate("prvdr_birth_dt") != null)
            current.setPrvdr_birth_dt(rs.getDate("prvdr_birth_dt").getTime());
        current.setPrvdr_1st_name(rs.getString("prvdr_1st_name"));
        current.setPrvdr_mdl_name(rs.getString("prvdr_mdl_name"));
        current.setPrvdr_last_name(rs.getString("prvdr_last_name"));
        current.setPrvdr_name(rs.getString("prvdr_name"));
        current.setPrvdr_lgl_name(rs.getString("prvdr_lgl_name"));
        current.setPrvdr_drvd_npi_name(rs.getString("prvdr_drvd_npi_name"));
        current.setPrvdr_npi_num(rs.getString("prvdr_npi_num"));
        current.setPrvdr_emplr_id_num(rs.getString("prvdr_emplr_id_num"));
        current.setPrvdr_mlg_line_1_adr(rs.getString("prvdr_mlg_line_1_adr"));
        current.setPrvdr_mlg_line_2_adr(rs.getString("prvdr_mlg_line_2_adr"));
        current.setPrvdr_mlg_plc_name(rs.getString("prvdr_mlg_plc_name"));
        current.setPrvdr_mlg_zip_cd(rs.getString("prvdr_mlg_zip_cd"));
        current.setPrvdr_mlg_state_cd(rs.getString("prvdr_mlg_state_cd"));
        current.setGeo_prvdr_mlg_sk(rs.getInt("geo_prvdr_mlg_sk"));
        current.setPrvdr_mlg_zip4_cd(rs.getString("prvdr_mlg_zip4_cd"));
        current.setPrvdr_mlg_tel_num(rs.getString("prvdr_mlg_tel_num"));
        current.setPrvdr_prctc_line_1_adr(rs.getString("prvdr_prctc_line_1_adr"));
        current.setPrvdr_prctc_line_2_adr(rs.getString("prvdr_prctc_line_2_adr"));
        current.setPrvdr_prctc_plc_name(rs.getString("prvdr_prctc_plc_name"));
        current.setPrvdr_prctc_zip_cd(rs.getString("prvdr_prctc_zip_cd"));
        current.setPrvdr_prctc_state_cd(rs.getString("prvdr_prctc_state_cd"));
        current.setGeo_prvdr_prctc_sk(rs.getInt("geo_prvdr_prctc_sk"));
        current.setPrvdr_prctc_zip4_cd(rs.getString("prvdr_prctc_zip4_cd"));
        current.setPrvdr_prctc_tel_num(rs.getString("prvdr_prctc_tel_num"));
        current.setPrvdr_src_id(rs.getString("prvdr_src_id"));
        current.setPrvdr_txnmy_cmpst_cd(rs.getString("prvdr_txnmy_cmpst_cd"));
        current.setPrvdr_txnmy_1_cd(rs.getString("prvdr_txnmy_1_cd"));
        current.setPrvdr_txnmy_2_cd(rs.getString("prvdr_txnmy_2_cd"));
        current.setPrvdr_txnmy_3_cd(rs.getString("prvdr_txnmy_3_cd"));
        current.setPrvdr_txnmy_4_cd(rs.getString("prvdr_txnmy_4_cd"));
        current.setPrvdr_txnmy_5_cd(rs.getString("prvdr_txnmy_5_cd"));
        current.setPrvdr_txnmy_6_cd(rs.getString("prvdr_txnmy_6_cd"));
        current.setPrvdr_txnmy_7_cd(rs.getString("prvdr_txnmy_7_cd"));
        current.setPrvdr_txnmy_8_cd(rs.getString("prvdr_txnmy_8_cd"));
        current.setPrvdr_txnmy_9_cd(rs.getString("prvdr_txnmy_9_cd"));
        current.setPrvdr_txnmy_10_cd(rs.getString("prvdr_txnmy_10_cd"));
        current.setPrvdr_txnmy_11_cd(rs.getString("prvdr_txnmy_11_cd"));
        current.setPrvdr_txnmy_12_cd(rs.getString("prvdr_txnmy_12_cd"));
        current.setPrvdr_txnmy_13_cd(rs.getString("prvdr_txnmy_13_cd"));
        current.setPrvdr_txnmy_14_cd(rs.getString("prvdr_txnmy_14_cd"));
        current.setPrvdr_txnmy_15_cd(rs.getString("prvdr_txnmy_15_cd"));
        current.setMeta_sk(rs.getInt("meta_sk"));
        current.setMeta_lst_updt_sk(rs.getInt("meta_lst_updt_sk"));
        current.setMeta_src_sk(rs.getShort("meta_src_sk"));
        current.setMeta_lst_updt_src_sk(rs.getShort("meta_lst_updt_src_sk"));
        current.setPrvdr_type_cd(rs.getString("prvdr_type_cd"));
        current.setPrvdr_dme_pos_num(rs.getString("prvdr_dme_pos_num"));
        current.setPrvdr_state_lcns_num(rs.getString("prvdr_state_lcns_num"));
        current.setPrvdr_ncpdp_id(rs.getString("prvdr_ncpdp_id"));
        current.setPrvdr_oscar_num(rs.getString("prvdr_oscar_num"));
        current.setPrvdr_pin_num(rs.getString("prvdr_pin_num"));
        current.setPrvdr_pin_grp_num(rs.getString("prvdr_pin_grp_num"));
        current.setPrvdr_upin_num(rs.getString("prvdr_upin_num"));
        current.setPrvdr_upin_grp_num(rs.getString("prvdr_upin_grp_num"));
        current.setPrvdr_othr_id(rs.getString("prvdr_othr_id"));

        return current;
    }

    private static abbr3RfrncRecord buildabbr3RfrncRecord(ResultSet rs) throws SQLException {
        abbr3RfrncRecord current = new abbr3RfrncRecord();

        current.setCd(rs.getString("cd"));
        current.setShrt_desc(rs.getString("shrt_desc"));
        current.setLong_desc(rs.getString("long_desc"));
        current.setDgns_icd_ind(rs.getInt("dgns_icd_ind"));
        current.setClndr_yr_num(rs.getInt("clndr_yr_num"));
        current.setRfrnc_data_id(rs.getString("rfrnc_data_id"));

        return current;
    }

    private static abbr1CdRecord buildabbr1CdRecord(ResultSet rs) throws SQLException {
        abbr1CdRecord current = new abbr1CdRecord();

        current.setabbr1_cd(rs.getString("abbr1_cd"));
        current.setClndr_abbr1_yr_num(rs.getBigDecimal("clndr_abbr1_yr_num"));
        current.setabbr1_actn_cd(rs.getString("abbr1_actn_cd"));
        if (rs.getDate("abbr1_actn_efctv_dt") != null)
            current.setabbr1_actn_efctv_dt(rs.getDate("abbr1_actn_efctv_dt").getTime());
        current.setabbr1_ansthsa_unit_qty(rs.getBigDecimal("abbr1_ansthsa_unit_qty"));
        current.setabbr1_asc_ind_cd(rs.getString("abbr1_asc_ind_cd"));
        current.setabbr1_asc_pmt_grp_cd(rs.getString("abbr1_asc_pmt_grp_cd"));
        current.setabbr1_betos_cd(rs.getString("abbr1_betos_cd"));
        current.setabbr1_betos_cd_desc(rs.getString("abbr1_betos_cd_desc"));
        current.setabbr1_betos_ctgry_cd(rs.getString("abbr1_betos_ctgry_cd"));
        current.setabbr1_betos_ctgry_clsfctn_cd(rs.getString("abbr1_betos_ctgry_clsfctn_cd"));
        if (rs.getDate("abbr1_cd_add_dt") != null)
            current.setabbr1_cd_add_dt(rs.getDate("abbr1_cd_add_dt").getTime());
        current.setabbr1_cd_desc(rs.getString("abbr1_cd_desc"));
        current.setabbr1_cd_shrt_desc(rs.getString("abbr1_cd_shrt_desc"));
        current.setabbr1_ctgry_cd(rs.getString("abbr1_ctgry_cd"));
        current.setabbr1_cvrg_cd(rs.getString("abbr1_cvrg_cd"));
        current.setabbr1_dme_plcy_grp_cd(rs.getString("abbr1_dme_plcy_grp_cd"));
        current.setabbr1_dme_plcy_grp_cd_desc(rs.getString("abbr1_dme_plcy_grp_cd_desc"));
        if (rs.getDate("abbr1_end_dt") != null)
            current.setabbr1_end_dt(rs.getDate("abbr1_end_dt").getTime());
        current.setabbr1_lvl_cd(rs.getString("abbr1_lvl_cd"));
        current.setabbr1_mog_pmt_cd(rs.getString("abbr1_mog_pmt_cd"));
        if (rs.getDate("abbr1_mog_pmt_efctv_dt") != null)
            current.setabbr1_mog_pmt_efctv_dt(rs.getDate("abbr1_mog_pmt_efctv_dt").getTime());
        current.setabbr1_mog_pmt_grp_cd(rs.getString("abbr1_mog_pmt_grp_cd"));
        current.setabbr1_nmrc_ind_cd(rs.getString("abbr1_nmrc_ind_cd"));
        if (rs.getDate("abbr1_pmt_grp_bgn_dt") != null)
            current.setabbr1_pmt_grp_bgn_dt(rs.getDate("abbr1_pmt_grp_bgn_dt").getTime());
        current.setabbr1_prcng_cd(rs.getString("abbr1_prcng_cd"));
        current.setabbr1_prcng_cd_desc(rs.getString("abbr1_prcng_cd_desc"));
        current.setabbr1_prcsg_note_num(rs.getString("abbr1_prcsg_note_num"));
        current.setabbr1_statute_num(rs.getString("abbr1_statute_num"));
        current.setMeta_sk(rs.getInt("meta_sk"));
        current.setMeta_src_sk(rs.getInt("meta_src_sk"));

        return current;
    }

    private static abbr2e4tStusNpiRecord buildabbr2e4tStusNpiRecord(ResultSet rs) throws SQLException {
        abbr2e4tStusNpiRecord current = new abbr2e4tStusNpiRecord();

        current.setPrvdr_npi_num(rs.getString("prvdr_npi_num"));
        current.sete4t_stus_sk(rs.getBigDecimal("e4t_stus_sk"));
        current.sete4t_id(rs.getString("e4t_id"));
        if (rs.getDate("e4t_finl_dt") != null)
            current.sete4t_finl_dt(rs.getDate("e4t_finl_dt").getTime());
        current.setPac_id(rs.getString("pac_id"));
        current.setCntrctr_id(rs.getString("cntrctr_id"));
        if (rs.getTimestamp("cntct_ts") != null)
            current.setCntct_ts(rs.getTimestamp("cntct_ts").getTime());
        current.sete4t_stus_rsn_cd(rs.getString("e4t_stus_rsn_cd"));
        current.sete4t_stus_rsn_desc(rs.getString("e4t_stus_rsn_desc"));
        current.setOthr_rsn_txt(rs.getString("othr_rsn_txt"));
        current.setStus_cd(rs.getString("stus_cd"));
        current.setStus_desc(rs.getString("stus_desc"));
        current.sete4t_stus_cmt_txt(rs.getString("e4t_stus_cmt_txt"));
        if (rs.getTimestamp("stus_eff_ts") != null)
            current.setStus_eff_ts(rs.getTimestamp("stus_eff_ts").getTime());
        if (rs.getTimestamp("stus_end_ts") != null)
            current.setStus_end_ts(rs.getTimestamp("stus_end_ts").getTime());
        current.setFaair_type_cd(rs.getString("faair_type_cd"));
        current.setPrvdr_dba_name(rs.getString("prvdr_dba_name"));
        current.setProc_cntl_dtl_insrt_sk(rs.getLong("proc_cntl_dtl_insrt_sk"));
        current.setProc_cntl_dtl_last_updt_sk(rs.getLong("proc_cntl_dtl_last_updt_sk"));
        current.setRec_stus_cd(rs.getString("rec_stus_cd"));
        if (rs.getTimestamp("rec_add_ts") != null)
            current.setRec_add_ts(rs.getTimestamp("rec_add_ts").getTime());
        if (rs.getTimestamp("rec_updt_ts") != null)
            current.setRec_updt_ts(rs.getTimestamp("rec_updt_ts").getTime());
        current.setPrvdr_e4t_stus_cnt(rs.getLong("prvdr_e4t_stus_cnt"));
        current.setRow_num(rs.getLong("row_num"));
        if (rs.getTimestamp("audt_insrt_ts") != null)
            current.setAudt_insrt_ts(rs.getTimestamp("audt_insrt_ts").getTime());
        if (rs.getTimestamp("audt_updt_ts") != null)
            current.setAudt_updt_ts(rs.getTimestamp("audt_updt_ts").getTime());

        return current;
    }

    public static class ResultSet {

        String getString(String a) {
            Integer l = lengths.get(a);

            if (l != null && l > 0)
                return StringUtils.repeat("*", l);
            else
                return null;
        }

        Date getDate(String a) {
            return new Date(234);
        }

        Timestamp getTimestamp(String a) {
            return new Timestamp(234);
        }

        BigDecimal getBigDecimal(String a) {
            return new BigDecimal(1324);
        }

        int getInt(String a) {
            return 42;
        }

        Short getShort(String a) {
            return 42;
        }

        long getLong(String a) {
            return 42l;
        }
    }

    private static void generateWriter(Class clazz) {
        for (Field f: clazz.getDeclaredFields()) {
            System.out.println("if (" +  f.getName() + " != null)");
            if (f.getType() == Long.class)
                System.out.println("    writer.writeLong(\"" + f.getName() + "\", " +  f.getName() + ");");
            if (f.getType() == String.class)
                System.out.println("    writer.writeString(\"" + f.getName() + "\", " +  f.getName() + ");");
            if (f.getType() == Integer.class)
                System.out.println("    writer.writeInt(\"" + f.getName() + "\", " +  f.getName() + ");");
            if (f.getType() == BigDecimal.class)
                System.out.println("    writer.writeDecimal(\"" + f.getName() + "\", " +  f.getName() + ");");
        }
    }

    private static void generateReader(Class clazz) {
        for (Field f: clazz.getDeclaredFields()) {
            if (f.getType() == Long.class)
                System.out.println("" + f.getName() + " = " + "reader.readLong(\"" + f.getName() + "\");");
            if (f.getType() == String.class)
                System.out.println("" + f.getName() + " = " + "reader.readString(\"" + f.getName() + "\");");
            if (f.getType() == Integer.class)
                System.out.println("" + f.getName() + " = " + "reader.readInt(\"" + f.getName() + "\");");
            if (f.getType() == BigDecimal.class)
                System.out.println("" + f.getName() + " = " + "reader.readDecimal(\"" + f.getName() + "\");");
        }
    }
}
