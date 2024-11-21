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

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Arrays;
import org.apache.ignite.springdata22.repository.query.IgniteCustomConversions;
import org.springframework.context.annotation.Bean;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.CustomConversions;

/**
 *  Test configuration that overrides default converter for LocalDateTime type.
 */
public class CustomConvertersApplicationConfiguration extends ApplicationConfiguration {

    @Bean
    public CustomConversions customConversions() {
        return new IgniteCustomConversions(Arrays.asList(new LocalDateTimeWriteConverter()));
    }

    static class LocalDateTimeWriteConverter implements Converter<Timestamp, LocalDateTime> {

        @Override public LocalDateTime convert(Timestamp source) {
            return null;
        }
    }
}
