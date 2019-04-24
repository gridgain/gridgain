package com.gridgain.experiment.model;

import java.math.BigDecimal;
import java.util.Objects;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

public class abbr1CdRecord {
    @QuerySqlField(index = true) private java.lang.String abbr1_cd;
    @QuerySqlField private java.math.BigDecimal clndr_abbr1_yr_num;
    @QuerySqlField private java.lang.String abbr1_actn_cd;
    @QuerySqlField private java.lang.Long abbr1_actn_efctv_dt;
    @QuerySqlField private java.math.BigDecimal abbr1_ansthsa_unit_qty;
    @QuerySqlField private java.lang.String abbr1_asc_ind_cd;
    @QuerySqlField private java.lang.String abbr1_asc_pmt_grp_cd;
    @QuerySqlField private java.lang.String abbr1_betos_cd;
    @QuerySqlField private java.lang.String abbr1_betos_cd_desc;
    @QuerySqlField private java.lang.String abbr1_betos_ctgry_cd;
    @QuerySqlField private java.lang.String abbr1_betos_ctgry_clsfctn_cd;
    @QuerySqlField private java.lang.Long abbr1_cd_add_dt;
    @QuerySqlField private java.lang.String abbr1_cd_desc;
    @QuerySqlField private java.lang.String abbr1_cd_shrt_desc;
    @QuerySqlField private java.lang.String abbr1_ctgry_cd;
    @QuerySqlField private java.lang.String abbr1_cvrg_cd;
    @QuerySqlField private java.lang.String abbr1_dme_plcy_grp_cd;
    @QuerySqlField private java.lang.String abbr1_dme_plcy_grp_cd_desc;
    @QuerySqlField private java.lang.Long abbr1_end_dt;
    @QuerySqlField private java.lang.String abbr1_lvl_cd;
    @QuerySqlField private java.lang.String abbr1_mog_pmt_cd;
    @QuerySqlField private java.lang.Long abbr1_mog_pmt_efctv_dt;
    @QuerySqlField private java.lang.String abbr1_mog_pmt_grp_cd;
    @QuerySqlField private java.lang.String abbr1_nmrc_ind_cd;
    @QuerySqlField private java.lang.Long abbr1_pmt_grp_bgn_dt;
    @QuerySqlField private java.lang.String abbr1_prcng_cd;
    @QuerySqlField private java.lang.String abbr1_prcng_cd_desc;
    @QuerySqlField private java.lang.String abbr1_prcsg_note_num;
    @QuerySqlField private java.lang.String abbr1_statute_num;
    @QuerySqlField private java.lang.Integer meta_sk;
    @QuerySqlField private java.lang.Integer meta_src_sk;

    @Override public String toString() {
        return "abbr1CdRecord{" +
                "abbr1_cd='" + abbr1_cd + '\'' +
                ", clndr_abbr1_yr_num=" + clndr_abbr1_yr_num +
                ", abbr1_actn_cd='" + abbr1_actn_cd + '\'' +
                ", abbr1_actn_efctv_dt=" + abbr1_actn_efctv_dt +
                ", abbr1_ansthsa_unit_qty=" + abbr1_ansthsa_unit_qty +
                ", abbr1_asc_ind_cd='" + abbr1_asc_ind_cd + '\'' +
                ", abbr1_asc_pmt_grp_cd='" + abbr1_asc_pmt_grp_cd + '\'' +
                ", abbr1_betos_cd='" + abbr1_betos_cd + '\'' +
                ", abbr1_betos_cd_desc='" + abbr1_betos_cd_desc + '\'' +
                ", abbr1_betos_ctgry_cd='" + abbr1_betos_ctgry_cd + '\'' +
                ", abbr1_betos_ctgry_clsfctn_cd='" + abbr1_betos_ctgry_clsfctn_cd + '\'' +
                ", abbr1_cd_add_dt=" + abbr1_cd_add_dt +
                ", abbr1_cd_desc='" + abbr1_cd_desc + '\'' +
                ", abbr1_cd_shrt_desc='" + abbr1_cd_shrt_desc + '\'' +
                ", abbr1_ctgry_cd='" + abbr1_ctgry_cd + '\'' +
                ", abbr1_cvrg_cd='" + abbr1_cvrg_cd + '\'' +
                ", abbr1_dme_plcy_grp_cd='" + abbr1_dme_plcy_grp_cd + '\'' +
                ", abbr1_dme_plcy_grp_cd_desc='" + abbr1_dme_plcy_grp_cd_desc + '\'' +
                ", abbr1_end_dt=" + abbr1_end_dt +
                ", abbr1_lvl_cd='" + abbr1_lvl_cd + '\'' +
                ", abbr1_mog_pmt_cd='" + abbr1_mog_pmt_cd + '\'' +
                ", abbr1_mog_pmt_efctv_dt=" + abbr1_mog_pmt_efctv_dt +
                ", abbr1_mog_pmt_grp_cd='" + abbr1_mog_pmt_grp_cd + '\'' +
                ", abbr1_nmrc_ind_cd='" + abbr1_nmrc_ind_cd + '\'' +
                ", abbr1_pmt_grp_bgn_dt=" + abbr1_pmt_grp_bgn_dt +
                ", abbr1_prcng_cd='" + abbr1_prcng_cd + '\'' +
                ", abbr1_prcng_cd_desc='" + abbr1_prcng_cd_desc + '\'' +
                ", abbr1_prcsg_note_num='" + abbr1_prcsg_note_num + '\'' +
                ", abbr1_statute_num='" + abbr1_statute_num + '\'' +
                ", meta_sk=" + meta_sk +
                ", meta_src_sk=" + meta_src_sk +
                '}';
    }

    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        abbr1CdRecord record = (abbr1CdRecord)o;
        return Objects.equals(abbr1_cd, record.abbr1_cd) &&
                Objects.equals(clndr_abbr1_yr_num, record.clndr_abbr1_yr_num) &&
                Objects.equals(abbr1_actn_cd, record.abbr1_actn_cd) &&
                Objects.equals(abbr1_actn_efctv_dt, record.abbr1_actn_efctv_dt) &&
                Objects.equals(abbr1_ansthsa_unit_qty, record.abbr1_ansthsa_unit_qty) &&
                Objects.equals(abbr1_asc_ind_cd, record.abbr1_asc_ind_cd) &&
                Objects.equals(abbr1_asc_pmt_grp_cd, record.abbr1_asc_pmt_grp_cd) &&
                Objects.equals(abbr1_betos_cd, record.abbr1_betos_cd) &&
                Objects.equals(abbr1_betos_cd_desc, record.abbr1_betos_cd_desc) &&
                Objects.equals(abbr1_betos_ctgry_cd, record.abbr1_betos_ctgry_cd) &&
                Objects.equals(abbr1_betos_ctgry_clsfctn_cd, record.abbr1_betos_ctgry_clsfctn_cd) &&
                Objects.equals(abbr1_cd_add_dt, record.abbr1_cd_add_dt) &&
                Objects.equals(abbr1_cd_desc, record.abbr1_cd_desc) &&
                Objects.equals(abbr1_cd_shrt_desc, record.abbr1_cd_shrt_desc) &&
                Objects.equals(abbr1_ctgry_cd, record.abbr1_ctgry_cd) &&
                Objects.equals(abbr1_cvrg_cd, record.abbr1_cvrg_cd) &&
                Objects.equals(abbr1_dme_plcy_grp_cd, record.abbr1_dme_plcy_grp_cd) &&
                Objects.equals(abbr1_dme_plcy_grp_cd_desc, record.abbr1_dme_plcy_grp_cd_desc) &&
                Objects.equals(abbr1_end_dt, record.abbr1_end_dt) &&
                Objects.equals(abbr1_lvl_cd, record.abbr1_lvl_cd) &&
                Objects.equals(abbr1_mog_pmt_cd, record.abbr1_mog_pmt_cd) &&
                Objects.equals(abbr1_mog_pmt_efctv_dt, record.abbr1_mog_pmt_efctv_dt) &&
                Objects.equals(abbr1_mog_pmt_grp_cd, record.abbr1_mog_pmt_grp_cd) &&
                Objects.equals(abbr1_nmrc_ind_cd, record.abbr1_nmrc_ind_cd) &&
                Objects.equals(abbr1_pmt_grp_bgn_dt, record.abbr1_pmt_grp_bgn_dt) &&
                Objects.equals(abbr1_prcng_cd, record.abbr1_prcng_cd) &&
                Objects.equals(abbr1_prcng_cd_desc, record.abbr1_prcng_cd_desc) &&
                Objects.equals(abbr1_prcsg_note_num, record.abbr1_prcsg_note_num) &&
                Objects.equals(abbr1_statute_num, record.abbr1_statute_num) &&
                Objects.equals(meta_sk, record.meta_sk) &&
                Objects.equals(meta_src_sk, record.meta_src_sk);
    }

    @Override public int hashCode() {

        return Objects.hash(abbr1_cd, clndr_abbr1_yr_num, abbr1_actn_cd, abbr1_actn_efctv_dt, abbr1_ansthsa_unit_qty, abbr1_asc_ind_cd, abbr1_asc_pmt_grp_cd, abbr1_betos_cd, abbr1_betos_cd_desc, abbr1_betos_ctgry_cd, abbr1_betos_ctgry_clsfctn_cd, abbr1_cd_add_dt, abbr1_cd_desc, abbr1_cd_shrt_desc, abbr1_ctgry_cd, abbr1_cvrg_cd, abbr1_dme_plcy_grp_cd, abbr1_dme_plcy_grp_cd_desc, abbr1_end_dt, abbr1_lvl_cd, abbr1_mog_pmt_cd, abbr1_mog_pmt_efctv_dt, abbr1_mog_pmt_grp_cd, abbr1_nmrc_ind_cd, abbr1_pmt_grp_bgn_dt, abbr1_prcng_cd, abbr1_prcng_cd_desc, abbr1_prcsg_note_num, abbr1_statute_num, meta_sk, meta_src_sk);
    }

    public String getabbr1_cd() {
        return abbr1_cd;
    }

    public void setabbr1_cd(String abbr1_cd) {
        this.abbr1_cd = abbr1_cd;
    }

    public BigDecimal getClndr_abbr1_yr_num() {
        return clndr_abbr1_yr_num;
    }

    public void setClndr_abbr1_yr_num(BigDecimal clndr_abbr1_yr_num) {
        this.clndr_abbr1_yr_num = clndr_abbr1_yr_num;
    }

    public String getabbr1_actn_cd() {
        return abbr1_actn_cd;
    }

    public void setabbr1_actn_cd(String abbr1_actn_cd) {
        this.abbr1_actn_cd = abbr1_actn_cd;
    }

    public Long getabbr1_actn_efctv_dt() {
        return abbr1_actn_efctv_dt;
    }

    public void setabbr1_actn_efctv_dt(Long abbr1_actn_efctv_dt) {
        this.abbr1_actn_efctv_dt = abbr1_actn_efctv_dt;
    }

    public BigDecimal getabbr1_ansthsa_unit_qty() {
        return abbr1_ansthsa_unit_qty;
    }

    public void setabbr1_ansthsa_unit_qty(BigDecimal abbr1_ansthsa_unit_qty) {
        this.abbr1_ansthsa_unit_qty = abbr1_ansthsa_unit_qty;
    }

    public String getabbr1_asc_ind_cd() {
        return abbr1_asc_ind_cd;
    }

    public void setabbr1_asc_ind_cd(String abbr1_asc_ind_cd) {
        this.abbr1_asc_ind_cd = abbr1_asc_ind_cd;
    }

    public String getabbr1_asc_pmt_grp_cd() {
        return abbr1_asc_pmt_grp_cd;
    }

    public void setabbr1_asc_pmt_grp_cd(String abbr1_asc_pmt_grp_cd) {
        this.abbr1_asc_pmt_grp_cd = abbr1_asc_pmt_grp_cd;
    }

    public String getabbr1_betos_cd() {
        return abbr1_betos_cd;
    }

    public void setabbr1_betos_cd(String abbr1_betos_cd) {
        this.abbr1_betos_cd = abbr1_betos_cd;
    }

    public String getabbr1_betos_cd_desc() {
        return abbr1_betos_cd_desc;
    }

    public void setabbr1_betos_cd_desc(String abbr1_betos_cd_desc) {
        this.abbr1_betos_cd_desc = abbr1_betos_cd_desc;
    }

    public String getabbr1_betos_ctgry_cd() {
        return abbr1_betos_ctgry_cd;
    }

    public void setabbr1_betos_ctgry_cd(String abbr1_betos_ctgry_cd) {
        this.abbr1_betos_ctgry_cd = abbr1_betos_ctgry_cd;
    }

    public String getabbr1_betos_ctgry_clsfctn_cd() {
        return abbr1_betos_ctgry_clsfctn_cd;
    }

    public void setabbr1_betos_ctgry_clsfctn_cd(String abbr1_betos_ctgry_clsfctn_cd) {
        this.abbr1_betos_ctgry_clsfctn_cd = abbr1_betos_ctgry_clsfctn_cd;
    }

    public Long getabbr1_cd_add_dt() {
        return abbr1_cd_add_dt;
    }

    public void setabbr1_cd_add_dt(Long abbr1_cd_add_dt) {
        this.abbr1_cd_add_dt = abbr1_cd_add_dt;
    }

    public String getabbr1_cd_desc() {
        return abbr1_cd_desc;
    }

    public void setabbr1_cd_desc(String abbr1_cd_desc) {
        this.abbr1_cd_desc = abbr1_cd_desc;
    }

    public String getabbr1_cd_shrt_desc() {
        return abbr1_cd_shrt_desc;
    }

    public void setabbr1_cd_shrt_desc(String abbr1_cd_shrt_desc) {
        this.abbr1_cd_shrt_desc = abbr1_cd_shrt_desc;
    }

    public String getabbr1_ctgry_cd() {
        return abbr1_ctgry_cd;
    }

    public void setabbr1_ctgry_cd(String abbr1_ctgry_cd) {
        this.abbr1_ctgry_cd = abbr1_ctgry_cd;
    }

    public String getabbr1_cvrg_cd() {
        return abbr1_cvrg_cd;
    }

    public void setabbr1_cvrg_cd(String abbr1_cvrg_cd) {
        this.abbr1_cvrg_cd = abbr1_cvrg_cd;
    }

    public String getabbr1_dme_plcy_grp_cd() {
        return abbr1_dme_plcy_grp_cd;
    }

    public void setabbr1_dme_plcy_grp_cd(String abbr1_dme_plcy_grp_cd) {
        this.abbr1_dme_plcy_grp_cd = abbr1_dme_plcy_grp_cd;
    }

    public String getabbr1_dme_plcy_grp_cd_desc() {
        return abbr1_dme_plcy_grp_cd_desc;
    }

    public void setabbr1_dme_plcy_grp_cd_desc(String abbr1_dme_plcy_grp_cd_desc) {
        this.abbr1_dme_plcy_grp_cd_desc = abbr1_dme_plcy_grp_cd_desc;
    }

    public Long getabbr1_end_dt() {
        return abbr1_end_dt;
    }

    public void setabbr1_end_dt(Long abbr1_end_dt) {
        this.abbr1_end_dt = abbr1_end_dt;
    }

    public String getabbr1_lvl_cd() {
        return abbr1_lvl_cd;
    }

    public void setabbr1_lvl_cd(String abbr1_lvl_cd) {
        this.abbr1_lvl_cd = abbr1_lvl_cd;
    }

    public String getabbr1_mog_pmt_cd() {
        return abbr1_mog_pmt_cd;
    }

    public void setabbr1_mog_pmt_cd(String abbr1_mog_pmt_cd) {
        this.abbr1_mog_pmt_cd = abbr1_mog_pmt_cd;
    }

    public Long getabbr1_mog_pmt_efctv_dt() {
        return abbr1_mog_pmt_efctv_dt;
    }

    public void setabbr1_mog_pmt_efctv_dt(Long abbr1_mog_pmt_efctv_dt) {
        this.abbr1_mog_pmt_efctv_dt = abbr1_mog_pmt_efctv_dt;
    }

    public String getabbr1_mog_pmt_grp_cd() {
        return abbr1_mog_pmt_grp_cd;
    }

    public void setabbr1_mog_pmt_grp_cd(String abbr1_mog_pmt_grp_cd) {
        this.abbr1_mog_pmt_grp_cd = abbr1_mog_pmt_grp_cd;
    }

    public String getabbr1_nmrc_ind_cd() {
        return abbr1_nmrc_ind_cd;
    }

    public void setabbr1_nmrc_ind_cd(String abbr1_nmrc_ind_cd) {
        this.abbr1_nmrc_ind_cd = abbr1_nmrc_ind_cd;
    }

    public Long getabbr1_pmt_grp_bgn_dt() {
        return abbr1_pmt_grp_bgn_dt;
    }

    public void setabbr1_pmt_grp_bgn_dt(Long abbr1_pmt_grp_bgn_dt) {
        this.abbr1_pmt_grp_bgn_dt = abbr1_pmt_grp_bgn_dt;
    }

    public String getabbr1_prcng_cd() {
        return abbr1_prcng_cd;
    }

    public void setabbr1_prcng_cd(String abbr1_prcng_cd) {
        this.abbr1_prcng_cd = abbr1_prcng_cd;
    }

    public String getabbr1_prcng_cd_desc() {
        return abbr1_prcng_cd_desc;
    }

    public void setabbr1_prcng_cd_desc(String abbr1_prcng_cd_desc) {
        this.abbr1_prcng_cd_desc = abbr1_prcng_cd_desc;
    }

    public String getabbr1_prcsg_note_num() {
        return abbr1_prcsg_note_num;
    }

    public void setabbr1_prcsg_note_num(String abbr1_prcsg_note_num) {
        this.abbr1_prcsg_note_num = abbr1_prcsg_note_num;
    }

    public String getabbr1_statute_num() {
        return abbr1_statute_num;
    }

    public void setabbr1_statute_num(String abbr1_statute_num) {
        this.abbr1_statute_num = abbr1_statute_num;
    }

    public Integer getMeta_sk() {
        return meta_sk;
    }

    public void setMeta_sk(Integer meta_sk) {
        this.meta_sk = meta_sk;
    }

    public Integer getMeta_src_sk() {
        return meta_src_sk;
    }

    public void setMeta_src_sk(Integer meta_src_sk) {
        this.meta_src_sk = meta_src_sk;
    }
}
