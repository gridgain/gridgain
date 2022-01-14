package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.cache.query.annotations.QuerySqlField;

import java.util.Objects;

public class City {
    @QuerySqlField
    public String name;

    @QuerySqlField
    public int code;

    public City(String name) {
        this.name = name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        City city = (City) o;
        return code == city.code && Objects.equals(name, city.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, code);
    }
}
