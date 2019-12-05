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

package org.apache.ignite.console.web.controller;

import io.swagger.annotations.ApiOperation;
import javax.validation.Valid;
import org.apache.ignite.console.common.Test;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.console.json.JsonArray;
import org.apache.ignite.console.services.AccountsService;
import org.apache.ignite.console.services.AdminService;
import org.apache.ignite.console.utils.Utils;
import org.apache.ignite.console.web.model.SignUpRequest;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.springframework.http.ResponseEntity;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Controller for test API.
 */
@RestController
@RequestMapping("/api/v1/test")
@Test
public class TestController {
    /** Authentication manager. */
    private AuthenticationManager authMgr;

    /** Accounts service. */
    protected AccountsService accountsSrvc;

    /** Administration service. */
    protected AdminService adminSrv;

    /**
     * @param authMgr Authentication manager.
     * @param accountsSrvc Accounts service.
     * @param adminSrvc Administration service.
     */
    public TestController(AuthenticationManager authMgr, AccountsService accountsSrvc, AdminService adminSrvc) {
        this.authMgr = authMgr;
        this.accountsSrvc = accountsSrvc;
        this.adminSrv = adminSrvc;
    }

    /**
     * @param params SignUp params.
     */
    @ApiOperation(value = "Register and authenticate under admin user.")
    @PutMapping(path = "/admins")
    public ResponseEntity<Void> registerAdmin(@Valid @RequestBody SignUpRequest params) {
        Account acc = adminSrv.registerUser(params);

        accountsSrvc.toggle(acc.getId(), true);

        Authentication authentication = authMgr.authenticate(
            new UsernamePasswordAuthenticationToken(
                params.getEmail(),
                params.getPassword())
        );

        SecurityContextHolder.getContext().setAuthentication(authentication);

        return ResponseEntity.ok().build();
    }

    /**
     * @param email Email mask.
     */
    @ApiOperation(value = "Delete test users by mask.")
    @DeleteMapping(path = "/users/{email}")
    public ResponseEntity<Void> delete(@PathVariable("email") String email) {
        JsonArray accounts = adminSrv.list(U.currentTimeMillis(), U.currentTimeMillis());

        accounts.stream()
            .map(Utils::asJson)
            .filter(acc -> acc.getString("email").contains(email))
            .map(acc -> acc.getUuid("id"))
            .forEach(adminSrv::delete);

        return ResponseEntity.ok().build();
    }
}
