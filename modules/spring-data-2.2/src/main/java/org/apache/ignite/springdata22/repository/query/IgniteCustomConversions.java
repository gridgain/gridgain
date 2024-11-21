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

package org.apache.ignite.springdata22.repository.query;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.mapping.model.SimpleTypeHolder;

/**
 * Custom conversions implementation.
 * An application can define its own converter by defining the following bean:
 * <pre>
 * {@code
 *     @Bean
 *     public CustomConversions customConversions() {
 *         return new IgniteCustomConversions(Arrays.asList(new LocalDateTimeWriteConverter()));
 *     }
 * }
 * </pre>
 */
public class IgniteCustomConversions extends org.springframework.data.convert.CustomConversions  {

    private static final List<Object> STORE_CONVERTERS;

    private static final StoreConversions STORE_CONVERSIONS;

    static {
        List<Object> converters = new ArrayList<>();
        converters.add(new LocalDateTimeWriteConverter());
        converters.add(new DateWriteConverter());

        STORE_CONVERTERS = Collections.unmodifiableList(converters);
        STORE_CONVERSIONS = StoreConversions.of(SimpleTypeHolder.DEFAULT, STORE_CONVERTERS);
    }

    public IgniteCustomConversions() {
        this(Collections.emptyList());
    }


    public IgniteCustomConversions(List<?> converters) {
        super(STORE_CONVERSIONS, converters);
    }

    static class LocalDateTimeWriteConverter implements Converter<Timestamp, LocalDateTime> {

        @Override public LocalDateTime convert(Timestamp source) {
            LocalDateTime localDateTime = source.toInstant()
                .atZone(ZoneId.systemDefault())
                .toLocalDateTime();
            return localDateTime;
        }
    }

    static class DateWriteConverter implements Converter<Timestamp, Date> {

        @Override public Date convert(Timestamp source) {
            return new java.util.Date((source).getTime());
        }
    }
}
