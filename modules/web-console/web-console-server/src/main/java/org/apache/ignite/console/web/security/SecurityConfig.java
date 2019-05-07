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

package org.apache.ignite.console.web.security;

import java.io.IOException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.console.services.AccountsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpStatus;
import org.springframework.security.authentication.dao.DaoAuthenticationProvider;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.builders.WebSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.userdetails.UserDetailsChecker;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;
import org.springframework.security.web.authentication.logout.HttpStatusReturningLogoutSuccessHandler;
import org.springframework.security.web.csrf.CookieCsrfTokenRepository;
import org.springframework.security.web.util.matcher.AntPathRequestMatcher;
import org.springframework.session.ExpiringSession;
import org.springframework.session.MapSession;
import org.springframework.session.SessionRepository;
import org.springframework.session.config.annotation.web.http.EnableSpringHttpSession;

import static org.apache.ignite.console.websocket.WebSocketConsts.BROWSERS_PATH;

/**
 * Security settings provider.
 */
@EnableWebSecurity
@EnableSpringHttpSession
public class SecurityConfig extends WebSecurityConfigurerAdapter {
    /** Sign in route. */
    private static final String SIGN_IN_ROUTE = "/api/v1/signin";

    /** Sign up route. */
    private static final String SIGN_UP_ROUTE = "/api/v1/signup";

    /** Logout route. */
    private static final String LOGOUT_ROUTE = "/api/v1/logout";

    /** Forgot password route. */
    private static final String FORGOT_PASSWORD_ROUTE = "/api/v1/password/forgot";

    /** Reset password route. */
    private static final String RESET_PASSWORD_ROUTE = "/api/v1/password/reset";

    /** Resend activation token. */
    private static final String ACTIVATION_RESEND = "/api/v1/activation/resend/";

    /** Public routes. */
    private static final String[] PUBLIC_ROUTES = new String[] {
        SIGN_IN_ROUTE, SIGN_UP_ROUTE,
        FORGOT_PASSWORD_ROUTE, RESET_PASSWORD_ROUTE, ACTIVATION_RESEND
    };

    /** */
    @Value("${app.activation.enabled:false}")
    private boolean activationEnabled;

    /** */
    @Value("${app.activation.timeout:1800000}")
    private long activationTimeout;

    /** */
    private final AccountsService accountsSrvc;

    /** */
    private final PasswordEncoder encoder;

    /** */
    private UserDetailsChecker userDetailsChecker;

    /**
     * @param encoder Service for encoding user passwords.
     * @param userDetailsChecker Service to check the status of the loaded <tt>UserDetails</tt> object.
     * @param accountsSrvc User details service.
     */
    @Autowired
    public SecurityConfig(PasswordEncoder encoder, UserDetailsChecker userDetailsChecker, AccountsService accountsSrvc) {
        this.encoder = encoder;
        this.userDetailsChecker = userDetailsChecker;
        this.accountsSrvc = accountsSrvc;
    }

    /** {@inheritDoc} */
    @Override protected void configure(HttpSecurity http) throws Exception {
        http
            .csrf()
            .csrfTokenRepository(CookieCsrfTokenRepository.withHttpOnlyFalse())
            .and()
            .authorizeRequests()
            .antMatchers(PUBLIC_ROUTES).anonymous()
            .antMatchers("/api/v1/admin/**").hasRole("ADMIN")
            .antMatchers("/api/v1/**", BROWSERS_PATH).hasRole("USER")
            .and()
            .addFilterBefore(authenticationFilter(), UsernamePasswordAuthenticationFilter.class)
            .logout()
            .logoutUrl(LOGOUT_ROUTE)
            .deleteCookies("JSESSIONID")
            .logoutSuccessHandler(new HttpStatusReturningLogoutSuccessHandler(HttpStatus.OK));
    }

    /**
     * @param auth Auth.
     */
    @Autowired
    public void configureGlobal(AuthenticationManagerBuilder auth) {
        configure(auth);
    }

    /** {@inheritDoc} */
    @Override public void configure(WebSecurity web) {
        web.ignoring().antMatchers(
            "/v2/api-docs",
            "/configuration/ui",
            "/swagger-resources",
            "/swagger-resources/configuration/ui",
            "/swagger-resources/configuration/security",
            "/configuration/security",
            "/swagger-ui.html",
            "/webjars/**"
        );
    }

    /** {@inheritDoc} */
    @Override protected void configure(AuthenticationManagerBuilder auth) {
        DaoAuthenticationProvider authProvider = activationEnabled
            ? new CustomAuthenticationProvider(activationTimeout)
            : new DaoAuthenticationProvider();

        authProvider.setPreAuthenticationChecks(userDetailsChecker);
        authProvider.setUserDetailsService(accountsSrvc);
        authProvider.setPasswordEncoder(encoder);

        auth.authenticationProvider(authProvider);
    }

    /**
     * @param req Request.
     * @param res Response.
     * @param authentication Authentication.
     */
    private void loginSuccessHandler(
        HttpServletRequest req,
        HttpServletResponse res,
        Authentication authentication
    ) throws IOException {
        res.setStatus(HttpServletResponse.SC_OK);

        res.getWriter().flush();
    }

    /**
     * @param ignite Ignite.
     */
    @Bean
    public SessionRepository<ExpiringSession> sessionRepository(@Autowired Ignite ignite) {
        CacheConfiguration<String, MapSession> cfg = new CacheConfiguration<String, MapSession>()
            .setName("sessions")
            .setCacheMode(CacheMode.REPLICATED);

        return new IgniteSessionRepository(ignite.getOrCreateCache(cfg));
    }

    /**
     * Custom filter for retrieve credentials.
     */
    @Bean
    public RequestBodyReaderAuthenticationFilter authenticationFilter() throws Exception {
        RequestBodyReaderAuthenticationFilter authenticationFilter = new RequestBodyReaderAuthenticationFilter();

        authenticationFilter.setAuthenticationManager(authenticationManagerBean());
        authenticationFilter.setRequiresAuthenticationRequestMatcher(new AntPathRequestMatcher(SIGN_IN_ROUTE, "POST"));
        authenticationFilter.setAuthenticationSuccessHandler(this::loginSuccessHandler);

        return authenticationFilter;
    }
}
