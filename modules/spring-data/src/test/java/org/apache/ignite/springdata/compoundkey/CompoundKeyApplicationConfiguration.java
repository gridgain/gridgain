/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.springdata.compoundkey;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.springdata.repository.config.EnableIgniteRepositories;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

/**
 * Spring application configuration
 * */
@Configuration
@EnableIgniteRepositories
public class CompoundKeyApplicationConfiguration {
    /**
     * Ignite instance bean
     * */
    @Bean
    public Ignite igniteInstance() {
        return Ignition.start("/home/kazakov/Documents/apache/ignite/examples/config/example-ignite.xml");
    }

    public static void main(String[] args) throws Exception {
        AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
        ctx.register(CompoundKeyApplicationConfiguration.class);
        ctx.refresh();

        Ignite ignite =  ctx.getBean(Ignite.class);

        System.out.println(ignite.cacheNames());

        ignite.destroyCache("City");

        System.out.println(ignite.cacheNames());

        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/")) {
            Statement st = conn.createStatement();

            st.execute("DROP TABLE IF EXISTS City");
            st.execute("CREATE TABLE City (ID INT, Name VARCHAR, CountryCode CHAR(3), District VARCHAR, Population INT, PRIMARY KEY (ID, CountryCode)) WITH \"template=partitioned, backups=1, affinityKey=CountryCode, CACHE_NAME=City, KEY_TYPE=org.apache.ignite.springdata.compoundkey.CityKey, VALUE_TYPE=org.apache.ignite.springdata.compoundkey.City\"");
            st.execute("SET STREAMING ON;");
            st.execute("INSERT INTO City(ID, Name, CountryCode, District, Population) VALUES (1,'Kabul','AFG','Kabol',1780000)");
            st.execute("INSERT INTO City(ID, Name, CountryCode, District, Population) VALUES (2,'Qandahar','AFG','Qandahar',237500)");
            st.execute("INSERT INTO City(ID, Name, CountryCode, District, Population) VALUES (3,'Herat','AFG','Herat',186800)");
            st.execute("INSERT INTO City(ID, Name, CountryCode, District, Population) VALUES (4,'Mazar-e-Sharif','AFG','Balkh',127800)");
            st.execute("INSERT INTO City(ID, Name, CountryCode, District, Population) VALUES (5,'Amsterdam','NLD','Noord-Holland',731200)");
        }

        System.out.println(ignite.cacheNames());



        CityRepository repo = ctx.getBean(CityRepository.class);
        System.out.println(repo.findByCountryCode("AFG"));


        //CityRepository rep = ctx.getBean(CityRepository.class);
        //System.out.println(rep.findById(1));

    }
}
