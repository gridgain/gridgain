package com.gridgain.experiment.model;

import java.util.Objects;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

public class abbr3RfrncRecord {
    @QuerySqlField(index = true)
    private java.lang.String cd;
    @QuerySqlField
    private java.lang.String shrt_desc;
    @QuerySqlField
    private java.lang.String long_desc;
    @QuerySqlField
    private java.lang.Integer dgns_icd_ind;
    @QuerySqlField
    private java.lang.Integer clndr_yr_num;
    @QuerySqlField
    private java.lang.String rfrnc_data_id;

    @Override public String toString() {
        return "abbr3RfrncRecord{" +
                "cd='" + cd + '\'' +
                ", shrt_desc='" + shrt_desc + '\'' +
                ", long_desc='" + long_desc + '\'' +
                ", dgns_icd_ind=" + dgns_icd_ind +
                ", clndr_yr_num=" + clndr_yr_num +
                ", rfrnc_data_id='" + rfrnc_data_id + '\'' +
                '}';
    }

    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        abbr3RfrncRecord record = (abbr3RfrncRecord)o;
        return Objects.equals(cd, record.cd) &&
                Objects.equals(shrt_desc, record.shrt_desc) &&
                Objects.equals(long_desc, record.long_desc) &&
                Objects.equals(dgns_icd_ind, record.dgns_icd_ind) &&
                Objects.equals(clndr_yr_num, record.clndr_yr_num) &&
                Objects.equals(rfrnc_data_id, record.rfrnc_data_id);
    }

    @Override public int hashCode() {

        return Objects.hash(cd, shrt_desc, long_desc, dgns_icd_ind, clndr_yr_num, rfrnc_data_id);
    }

    public String getCd() {
        return cd;
    }

    public void setCd(String cd) {
        this.cd = cd;
    }

    public String getShrt_desc() {
        return shrt_desc;
    }

    public void setShrt_desc(String shrt_desc) {
        this.shrt_desc = shrt_desc;
    }

    public String getLong_desc() {
        return long_desc;
    }

    public void setLong_desc(String long_desc) {
        this.long_desc = long_desc;
    }

    public Integer getDgns_icd_ind() {
        return dgns_icd_ind;
    }

    public void setDgns_icd_ind(Integer dgns_icd_ind) {
        this.dgns_icd_ind = dgns_icd_ind;
    }

    public Integer getClndr_yr_num() {
        return clndr_yr_num;
    }

    public void setClndr_yr_num(Integer clndr_yr_num) {
        this.clndr_yr_num = clndr_yr_num;
    }

    public String getRfrnc_data_id() {
        return rfrnc_data_id;
    }

    public void setRfrnc_data_id(String rfrnc_data_id) {
        this.rfrnc_data_id = rfrnc_data_id;
    }
}
