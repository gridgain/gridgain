package org.apache.ignite.cs16644;

import java.io.Serializable;

public class Person implements Serializable {
    /** Indexed field. Will be visible to the SQL engine. */
//        @QuerySqlField(index = true)
    private Long id;

    /** Queryable field. Will be visible to the SQL engine. */
//        @QuerySqlField
    private String name;

//        /** Will NOT be visible to the SQL engine. */
//        private int age;

    public Person() {
    }

    public Person(Long id, String name) {
        this.id = id;
        this.name = name;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    //        /**
//         * Indexed field sorted in descending order. Will be visible to the SQL engine.
//         */
//        @QuerySqlField(index = true, descending = true)
//        private float salary;
}
