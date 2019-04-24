package com.gridgain.experiment.model;

import java.math.BigDecimal;
import java.util.Objects;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

public class abbr2e4tStusNpiRecord {
    @QuerySqlField private java.lang.String prvdr_npi_num;
    @QuerySqlField private java.math.BigDecimal e4t_stus_sk;
    @QuerySqlField private java.lang.String e4t_id;
    @QuerySqlField private java.lang.Long e4t_finl_dt;
    @QuerySqlField private java.lang.String pac_id;
    @QuerySqlField private java.lang.String cntrctr_id;
    @QuerySqlField private java.lang.Long cntct_ts;
    @QuerySqlField private java.lang.String e4t_stus_rsn_cd;
    @QuerySqlField private java.lang.String e4t_stus_rsn_desc;
    @QuerySqlField private java.lang.String othr_rsn_txt;
    @QuerySqlField private java.lang.String stus_cd;
    @QuerySqlField private java.lang.String stus_desc;
    @QuerySqlField private java.lang.String e4t_stus_cmt_txt;
    @QuerySqlField private java.lang.Long stus_eff_ts;
    @QuerySqlField private java.lang.Long stus_end_ts;
    @QuerySqlField private java.lang.String faair_type_cd;
    @QuerySqlField private java.lang.String prvdr_dba_name;
    @QuerySqlField private java.lang.Long proc_cntl_dtl_insrt_sk;
    @QuerySqlField private java.lang.Long proc_cntl_dtl_last_updt_sk;
    @QuerySqlField private java.lang.String rec_stus_cd;
    @QuerySqlField private java.lang.Long rec_add_ts;
    @QuerySqlField private java.lang.Long rec_updt_ts;
    @QuerySqlField private java.lang.Long prvdr_e4t_stus_cnt;
    @QuerySqlField private java.lang.Long row_num;
    @QuerySqlField private java.lang.Long audt_insrt_ts;
    @QuerySqlField private java.lang.Long audt_updt_ts;

    @Override public String toString() {
        return "abbr2e4tStusNpiRecord{" +
                "prvdr_npi_num='" + prvdr_npi_num + '\'' +
                ", e4t_stus_sk=" + e4t_stus_sk +
                ", e4t_id='" + e4t_id + '\'' +
                ", e4t_finl_dt=" + e4t_finl_dt +
                ", pac_id='" + pac_id + '\'' +
                ", cntrctr_id='" + cntrctr_id + '\'' +
                ", cntct_ts=" + cntct_ts +
                ", e4t_stus_rsn_cd='" + e4t_stus_rsn_cd + '\'' +
                ", e4t_stus_rsn_desc='" + e4t_stus_rsn_desc + '\'' +
                ", othr_rsn_txt='" + othr_rsn_txt + '\'' +
                ", stus_cd='" + stus_cd + '\'' +
                ", stus_desc='" + stus_desc + '\'' +
                ", e4t_stus_cmt_txt='" + e4t_stus_cmt_txt + '\'' +
                ", stus_eff_ts=" + stus_eff_ts +
                ", stus_end_ts=" + stus_end_ts +
                ", faair_type_cd='" + faair_type_cd + '\'' +
                ", prvdr_dba_name='" + prvdr_dba_name + '\'' +
                ", proc_cntl_dtl_insrt_sk=" + proc_cntl_dtl_insrt_sk +
                ", proc_cntl_dtl_last_updt_sk=" + proc_cntl_dtl_last_updt_sk +
                ", rec_stus_cd='" + rec_stus_cd + '\'' +
                ", rec_add_ts=" + rec_add_ts +
                ", rec_updt_ts=" + rec_updt_ts +
                ", prvdr_e4t_stus_cnt=" + prvdr_e4t_stus_cnt +
                ", row_num=" + row_num +
                ", audt_insrt_ts=" + audt_insrt_ts +
                ", audt_updt_ts=" + audt_updt_ts +
                '}';
    }

    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        abbr2e4tStusNpiRecord record = (abbr2e4tStusNpiRecord)o;
        return Objects.equals(prvdr_npi_num, record.prvdr_npi_num) &&
                Objects.equals(e4t_stus_sk, record.e4t_stus_sk) &&
                Objects.equals(e4t_id, record.e4t_id) &&
                Objects.equals(e4t_finl_dt, record.e4t_finl_dt) &&
                Objects.equals(pac_id, record.pac_id) &&
                Objects.equals(cntrctr_id, record.cntrctr_id) &&
                Objects.equals(cntct_ts, record.cntct_ts) &&
                Objects.equals(e4t_stus_rsn_cd, record.e4t_stus_rsn_cd) &&
                Objects.equals(e4t_stus_rsn_desc, record.e4t_stus_rsn_desc) &&
                Objects.equals(othr_rsn_txt, record.othr_rsn_txt) &&
                Objects.equals(stus_cd, record.stus_cd) &&
                Objects.equals(stus_desc, record.stus_desc) &&
                Objects.equals(e4t_stus_cmt_txt, record.e4t_stus_cmt_txt) &&
                Objects.equals(stus_eff_ts, record.stus_eff_ts) &&
                Objects.equals(stus_end_ts, record.stus_end_ts) &&
                Objects.equals(faair_type_cd, record.faair_type_cd) &&
                Objects.equals(prvdr_dba_name, record.prvdr_dba_name) &&
                Objects.equals(proc_cntl_dtl_insrt_sk, record.proc_cntl_dtl_insrt_sk) &&
                Objects.equals(proc_cntl_dtl_last_updt_sk, record.proc_cntl_dtl_last_updt_sk) &&
                Objects.equals(rec_stus_cd, record.rec_stus_cd) &&
                Objects.equals(rec_add_ts, record.rec_add_ts) &&
                Objects.equals(rec_updt_ts, record.rec_updt_ts) &&
                Objects.equals(prvdr_e4t_stus_cnt, record.prvdr_e4t_stus_cnt) &&
                Objects.equals(row_num, record.row_num) &&
                Objects.equals(audt_insrt_ts, record.audt_insrt_ts) &&
                Objects.equals(audt_updt_ts, record.audt_updt_ts);
    }

    @Override public int hashCode() {

        return Objects.hash(prvdr_npi_num, e4t_stus_sk, e4t_id, e4t_finl_dt, pac_id, cntrctr_id, cntct_ts, e4t_stus_rsn_cd, e4t_stus_rsn_desc, othr_rsn_txt, stus_cd, stus_desc, e4t_stus_cmt_txt, stus_eff_ts, stus_end_ts, faair_type_cd, prvdr_dba_name, proc_cntl_dtl_insrt_sk, proc_cntl_dtl_last_updt_sk, rec_stus_cd, rec_add_ts, rec_updt_ts, prvdr_e4t_stus_cnt, row_num, audt_insrt_ts, audt_updt_ts);
    }

    public String getPrvdr_npi_num() {
        return prvdr_npi_num;
    }

    public void setPrvdr_npi_num(String prvdr_npi_num) {
        this.prvdr_npi_num = prvdr_npi_num;
    }

    public BigDecimal gete4t_stus_sk() {
        return e4t_stus_sk;
    }

    public void sete4t_stus_sk(BigDecimal e4t_stus_sk) {
        this.e4t_stus_sk = e4t_stus_sk;
    }

    public String gete4t_id() {
        return e4t_id;
    }

    public void sete4t_id(String e4t_id) {
        this.e4t_id = e4t_id;
    }

    public Long gete4t_finl_dt() {
        return e4t_finl_dt;
    }

    public void sete4t_finl_dt(Long e4t_finl_dt) {
        this.e4t_finl_dt = e4t_finl_dt;
    }

    public String getPac_id() {
        return pac_id;
    }

    public void setPac_id(String pac_id) {
        this.pac_id = pac_id;
    }

    public String getCntrctr_id() {
        return cntrctr_id;
    }

    public void setCntrctr_id(String cntrctr_id) {
        this.cntrctr_id = cntrctr_id;
    }

    public Long getCntct_ts() {
        return cntct_ts;
    }

    public void setCntct_ts(Long cntct_ts) {
        this.cntct_ts = cntct_ts;
    }

    public String gete4t_stus_rsn_cd() {
        return e4t_stus_rsn_cd;
    }

    public void sete4t_stus_rsn_cd(String e4t_stus_rsn_cd) {
        this.e4t_stus_rsn_cd = e4t_stus_rsn_cd;
    }

    public String gete4t_stus_rsn_desc() {
        return e4t_stus_rsn_desc;
    }

    public void sete4t_stus_rsn_desc(String e4t_stus_rsn_desc) {
        this.e4t_stus_rsn_desc = e4t_stus_rsn_desc;
    }

    public String getOthr_rsn_txt() {
        return othr_rsn_txt;
    }

    public void setOthr_rsn_txt(String othr_rsn_txt) {
        this.othr_rsn_txt = othr_rsn_txt;
    }

    public String getStus_cd() {
        return stus_cd;
    }

    public void setStus_cd(String stus_cd) {
        this.stus_cd = stus_cd;
    }

    public String getStus_desc() {
        return stus_desc;
    }

    public void setStus_desc(String stus_desc) {
        this.stus_desc = stus_desc;
    }

    public String gete4t_stus_cmt_txt() {
        return e4t_stus_cmt_txt;
    }

    public void sete4t_stus_cmt_txt(String e4t_stus_cmt_txt) {
        this.e4t_stus_cmt_txt = e4t_stus_cmt_txt;
    }

    public Long getStus_eff_ts() {
        return stus_eff_ts;
    }

    public void setStus_eff_ts(Long stus_eff_ts) {
        this.stus_eff_ts = stus_eff_ts;
    }

    public Long getStus_end_ts() {
        return stus_end_ts;
    }

    public void setStus_end_ts(Long stus_end_ts) {
        this.stus_end_ts = stus_end_ts;
    }

    public String getFaair_type_cd() {
        return faair_type_cd;
    }

    public void setFaair_type_cd(String faair_type_cd) {
        this.faair_type_cd = faair_type_cd;
    }

    public String getPrvdr_dba_name() {
        return prvdr_dba_name;
    }

    public void setPrvdr_dba_name(String prvdr_dba_name) {
        this.prvdr_dba_name = prvdr_dba_name;
    }

    public Long getProc_cntl_dtl_insrt_sk() {
        return proc_cntl_dtl_insrt_sk;
    }

    public void setProc_cntl_dtl_insrt_sk(Long proc_cntl_dtl_insrt_sk) {
        this.proc_cntl_dtl_insrt_sk = proc_cntl_dtl_insrt_sk;
    }

    public Long getProc_cntl_dtl_last_updt_sk() {
        return proc_cntl_dtl_last_updt_sk;
    }

    public void setProc_cntl_dtl_last_updt_sk(Long proc_cntl_dtl_last_updt_sk) {
        this.proc_cntl_dtl_last_updt_sk = proc_cntl_dtl_last_updt_sk;
    }

    public String getRec_stus_cd() {
        return rec_stus_cd;
    }

    public void setRec_stus_cd(String rec_stus_cd) {
        this.rec_stus_cd = rec_stus_cd;
    }

    public Long getRec_add_ts() {
        return rec_add_ts;
    }

    public void setRec_add_ts(Long rec_add_ts) {
        this.rec_add_ts = rec_add_ts;
    }

    public Long getRec_updt_ts() {
        return rec_updt_ts;
    }

    public void setRec_updt_ts(Long rec_updt_ts) {
        this.rec_updt_ts = rec_updt_ts;
    }

    public Long getPrvdr_e4t_stus_cnt() {
        return prvdr_e4t_stus_cnt;
    }

    public void setPrvdr_e4t_stus_cnt(Long prvdr_e4t_stus_cnt) {
        this.prvdr_e4t_stus_cnt = prvdr_e4t_stus_cnt;
    }

    public Long getRow_num() {
        return row_num;
    }

    public void setRow_num(Long row_num) {
        this.row_num = row_num;
    }

    public Long getAudt_insrt_ts() {
        return audt_insrt_ts;
    }

    public void setAudt_insrt_ts(Long audt_insrt_ts) {
        this.audt_insrt_ts = audt_insrt_ts;
    }

    public Long getAudt_updt_ts() {
        return audt_updt_ts;
    }

    public void setAudt_updt_ts(Long audt_updt_ts) {
        this.audt_updt_ts = audt_updt_ts;
    }
}
