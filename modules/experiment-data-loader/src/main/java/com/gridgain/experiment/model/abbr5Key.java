package com.gridgain.experiment.model;

import java.util.Objects;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

public class abbr5Key {
    @QuerySqlField
    private java.lang.String abbr7_cntl_num;
    @QuerySqlField
    private java.lang.Integer abbr7_line_num;
    @QuerySqlField
    private java.lang.String abbr7_cntrctr_num;
    @QuerySqlField
    private java.lang.String abbr7_src_type;
    @QuerySqlField
    private java.lang.String abbr8_sk;

    public String getabbr7_cntl_num() {
        return abbr7_cntl_num;
    }

    public void setabbr7_cntl_num(String abbr7_cntl_num) {
        this.abbr7_cntl_num = abbr7_cntl_num;
    }

    public Integer getabbr7_line_num() {
        return abbr7_line_num;
    }

    public void setabbr7_line_num(Integer abbr7_line_num) {
        this.abbr7_line_num = abbr7_line_num;
    }

    public String getabbr7_cntrctr_num() {
        return abbr7_cntrctr_num;
    }

    public void setabbr7_cntrctr_num(String abbr7_cntrctr_num) {
        this.abbr7_cntrctr_num = abbr7_cntrctr_num;
    }

    public String getabbr7_src_type() {
        return abbr7_src_type;
    }

    public void setabbr7_src_type(String abbr7_src_type) {
        this.abbr7_src_type = abbr7_src_type;
    }

    public String getabbr8_sk() {
        return abbr8_sk;
    }

    public void setabbr8_sk(String abbr8_sk) {
        this.abbr8_sk = abbr8_sk;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        abbr5Key abbr5Key = (abbr5Key) o;
        return Objects.equals(abbr7_cntl_num, abbr5Key.abbr7_cntl_num) &&
                Objects.equals(abbr7_line_num, abbr5Key.abbr7_line_num) &&
                Objects.equals(abbr7_cntrctr_num, abbr5Key.abbr7_cntrctr_num) &&
                Objects.equals(abbr7_src_type, abbr5Key.abbr7_src_type) &&
                Objects.equals(abbr8_sk, abbr5Key.abbr8_sk);
    }

    @Override
    public int hashCode() {
        return Objects.hash(abbr7_cntl_num, abbr7_line_num, abbr7_cntrctr_num, abbr7_src_type, abbr8_sk);
    }

    @Override
    public String toString() {
        return "abbr5Key{" +
                "abbr7_cntl_num='" + abbr7_cntl_num + '\'' +
                ", abbr7_line_num=" + abbr7_line_num +
                ", abbr7_cntrctr_num='" + abbr7_cntrctr_num + '\'' +
                ", abbr7_src_type='" + abbr7_src_type + '\'' +
                ", abbr8_sk='" + abbr8_sk + '\'' +
                '}';
    }
}
