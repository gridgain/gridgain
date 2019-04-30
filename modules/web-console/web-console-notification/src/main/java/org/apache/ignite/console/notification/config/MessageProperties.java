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

package org.apache.ignite.console.notification.config;

import java.util.Map;
import org.apache.ignite.console.notification.model.INotificationDescriptor;
import org.apache.ignite.console.notification.model.NotificationDescriptor;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Mail configuration.
 */
@Component
@ConfigurationProperties(prefix = "spring.mail.templates")
public class MessageProperties {
    /** Default template path. */
    private String dfltTemplatePath;

    /** Templates path. */
    private Map<NotificationDescriptor, String> templates;

    /**
     * @return Default template path.
     */
    public String getDefaultTemplatePath() {
        return dfltTemplatePath;
    }

    /**
     * @param dfltTemplatePath Default template path.
     */
    public void setDefaultTemplatePath(String dfltTemplatePath) {
        this.dfltTemplatePath = dfltTemplatePath;
    }

    /**
     * @param desc Notification type.
     */
    public String getTemplatePath(INotificationDescriptor desc) {
        return templates == null ? dfltTemplatePath : templates.getOrDefault(desc, dfltTemplatePath);
    }

    /**
     * @return Templates path.
     */
    public Map<NotificationDescriptor, String> getTemplates() {
        return templates;
    }

    /**
     * @param templates New templates path.
     */
    public void setTemplates(Map<NotificationDescriptor, String> templates) {
        this.templates = templates;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(MessageProperties.class, this);
    }
}
