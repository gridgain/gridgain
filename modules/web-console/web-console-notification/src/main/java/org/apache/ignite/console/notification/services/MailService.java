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

package org.apache.ignite.console.notification.services;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Locale;
import javax.mail.MessagingException;
import javax.mail.internet.MimeMessage;
import org.apache.ignite.console.notification.config.MessageProperties;
import org.apache.ignite.console.notification.model.INotificationDescriptor;
import org.apache.ignite.console.notification.model.Notification;
import org.apache.ignite.console.notification.model.IRecipient;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.MessageSource;
import org.springframework.core.io.ClassPathResource;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.common.TemplateParserContext;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.stereotype.Service;

/**
 * Mail sending service.
 */
@Service
public class MailService {
    /** From email. */
    @Value("${spring.mail.username:''}")
    private String from;

    /** From alias. */
    @Value("${spring.mail.from.alias:''}")
    private String fromAlias;

    /** Expression parser. */
    private static final ExpressionParser parser = new SpelExpressionParser();

    /** Template parser context. */
    private static final TemplateParserContext templateParserCtx = new TemplateParserContext("${", "}");

    /** Message source. */
    private MessageSource msgSrc;

    /** JavaMail sender */
    private JavaMailSender mailSnd;

    /** Mail config. */
    private MessageProperties cfg;

    /**
     * Default constructor.
     */
    public MailService() {
    }

    /**
     * @param msgSrc Message source.
     * @param mailSnd Mail sender.
     * @param cfg Mail properties.
     */
    public MailService(MessageSource msgSrc, JavaMailSender mailSnd, MessageProperties cfg) {
        this.msgSrc = msgSrc;
        this.mailSnd = mailSnd;
        this.cfg = cfg;
    }

    /**
     * Send email.
     *
     * @param notification Notification.
     */
    public void send(Notification notification) throws IOException, MessagingException, URISyntaxException {
        NotificationWrapper ctxObj = new NotificationWrapper(notification);

        EvaluationContext ctx = createContext(ctxObj);

        INotificationDescriptor desc = notification.getDescriptor();

        ctxObj.setSubject(processExpressions(getMessage(desc.subjectCode()), ctx));
        ctxObj.setMessage(processExpressions(getMessage(desc.messageCode()), ctx));

        String template = loadMessageTemplate(notification.getDescriptor());

        MimeMessage msg = mailSnd.createMimeMessage();

        MimeMessageHelper msgHelper = new MimeMessageHelper(msg);

        msgHelper.setTo(notification.getRecipient().getEmail());
        msgHelper.setFrom(from, fromAlias);
        msgHelper.setSubject(ctxObj.getSubject());
        msgHelper.setText(template == null ? ctxObj.getMessage() : processExpressions(template, ctx), true);

        mailSnd.send(msg);
    }

    /**
     * @param desc Notification type.
     * @return Message template or empty string.
     */
    private String loadMessageTemplate(INotificationDescriptor desc) throws IOException, URISyntaxException {
        String path = cfg.getTemplatePath(desc);

        if (path == null)
            return null;

        URL url;

        try {
            url = new URL(path);
        }
        catch (MalformedURLException e) {
            url = U.resolveIgniteUrl(path);

            if (url == null)
                url = new ClassPathResource(path).getURL();
        }

        return new String(Files.readAllBytes(Paths.get(url.toURI())));
    }

    /**
     * @param rootObj Root object to use.
     * @return Context.
     */
    private EvaluationContext createContext(Object rootObj) {
        return new StandardEvaluationContext(rootObj);
    }

    /**
     * @param expression Raw expression to parse.
     * @param ctx Context.
     */
    private String processExpressions(String expression, EvaluationContext ctx) {
        return parser.parseExpression(expression, templateParserCtx).getValue(ctx, String.class);
    }

    /**
     * Try to resolve the message.
     *
     * @param code Code.
     * @return Message.
     */
    private String getMessage(String code) {
        return msgSrc.getMessage(code, null, code, Locale.US);
    }

    /**
     * Context for email templates.
     */
    private static class NotificationWrapper extends StandardEvaluationContext {
        /** Origin. */
        private String origin;

        /** Recipient. */
        private IRecipient rcpt;

        /** Subject. */
        private String subject;

        /** Message. */
        private String msg;

        /**
         * @param notification Notification.
         */
        private NotificationWrapper(Notification notification) {
            this.origin = notification.getOrigin();
            this.rcpt = notification.getRecipient();
        }

        /**
         * @return Origin.
         */
        public String getOrigin() {
            return origin;
        }

        /**
         * @return Recipient.
         */
        public IRecipient getRecipient() {
            return rcpt;
        }

        /**
         * @return Subject.
         */
        public String getSubject() {
            return subject;
        }

        /**
         * @param subject Subject.
         */
        public void setSubject(String subject) {
            this.subject = subject;
        }

        /**
         * @return Message.
         */
        public String getMessage() {
            return msg;
        }

        /**
         * @param msg Message.
         */
        public void setMessage(String msg) {
            this.msg = msg;
        }
    }
}
