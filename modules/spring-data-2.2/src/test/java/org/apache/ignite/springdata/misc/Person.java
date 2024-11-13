/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.springdata.misc;

import java.time.LocalDateTime;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cache.query.annotations.QueryTextField;

import java.util.Date;
import java.util.Objects;

/**
 * DTO class.
 */
public class Person {
    /** First name. */
    @QuerySqlField(index = true)
    @QueryTextField
    private String firstName;

    /** Second name. */
    @QuerySqlField(index = true)
    private String secondName;

    /** Birthday. */
    @QuerySqlField
    private Date birthday;

    @QuerySqlField
    private LocalDateTime createdAt;

    /**
     * @param firstName First name.
     * @param secondName Second name.
     */
    public Person(String firstName, String secondName) {
        this.firstName = firstName;
        this.secondName = secondName;
        birthday = new Date();
    }

    public Person(String firstName, String secondName, LocalDateTime createdAt) {
        this.firstName = firstName;
        this.secondName = secondName;
        birthday = new Date();
        this.createdAt = createdAt;
    }

    /**
     * @return First name.
     */
    public String getFirstName() {
        return firstName;
    }

    /**
     * @param firstName First name.
     */
    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    /**
     * @return Second name.
     */
    public String getSecondName() {
        return secondName;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    /**
     * @param secondName Second name.
     */
    public void setSecondName(String secondName) {
        this.secondName = secondName;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "Person{" +
            "firstName='" + firstName + '\'' +
            ", secondName='" + secondName + '\'' +
            ", birthday='" + birthday + '\'' +
            '}';
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        Person person = (Person)o;

        return Objects.equals(firstName, person.firstName) &&
            Objects.equals(secondName, person.secondName) &&
            Objects.equals(birthday, person.birthday);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(firstName, secondName, birthday);
    }
}
