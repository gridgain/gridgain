/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.console.notification.config;

import javax.activation.MimeType;
import javax.mail.internet.MimeMessage;
import org.apache.ignite.console.notification.services.IMailService;
import org.apache.ignite.console.notification.services.MailService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.AnyNestedCondition;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.MessageSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.mail.MailSender;
import org.springframework.mail.javamail.JavaMailSender;

@Configuration
@Conditional(MailServiceAutoConfiguration.MailSenderCondition.class)
@ConditionalOnClass({ MimeMessage.class, MimeType.class, MailSender.class, MailService.class })
@EnableConfigurationProperties(MailPropertiesEx.class)
public class MailServiceAutoConfiguration {
    /** Message source. */
    private MessageSource msgSrc;

    /** JavaMail sender. */
    private JavaMailSender mailSnd;

    /** Message properties. */
    MailPropertiesEx props;

    /**
     * Condition to trigger the creation of a {@link MailSender}. This kicks in if either
     * the host or jndi name property is set.
     */
    static class MailSenderCondition extends AnyNestedCondition {
        /**
         * Default constructor.
         */
        MailSenderCondition() {
            super(ConfigurationPhase.PARSE_CONFIGURATION);
        }

        @ConditionalOnProperty(prefix = "spring.mail", name = "host")
        static class HostProperty {

        }

        @ConditionalOnProperty(prefix = "spring.mail", name = "jndi-name")
        static class JndiNameProperty {

        }
    }

    public MailServiceAutoConfiguration(MessageSource msgSrc, JavaMailSender mailSnd, MailPropertiesEx props) {
        this.msgSrc = msgSrc;
        this.mailSnd = mailSnd;
        this.props = props;
    }

    @Bean
    @Primary
    public IMailService mailService() {
        return new MailService(msgSrc, mailSnd, props);
    }
}
