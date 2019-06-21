import {Component, Inject, Output, EventEmitter} from '@angular/core';
import {FormBuilder, FormGroup, Validators} from '@angular/forms';
import IgniteAdminData from '../../../app/core/admin/Admin.data';
import MessagesFactory from '../../../app/services/Messages.service';
import LoadingServiceFactory from '../../../app/modules/loading/loading.service';
import {customRequireValidator} from '../../common/validators';
import {SignupUserInfo} from 'app/modules/user/Auth.service';

@Component({
    selector: 'create-user',
    template: `
        <form [formGroup]="form" id="createUser" (ngSubmit)="createUser()" scrollToFirstInvalid>
            <form-signup [form]="form" [serverError]="serverError"></form-signup>
        </form>
    `
})
export class CreateUserComponent {

    static parameters = [
        [new Inject('$rootScope')],
        [new Inject('IgniteAdminData')],
        [new Inject('IgniteMessages')],
        [new Inject('IgniteLoading')],
        [new Inject(FormBuilder)]
    ];

    @Output() close: EventEmitter<void> = new EventEmitter();

    form: FormGroup;

    serverError: string | null = null;

    isLoading = false;

    constructor(
        private $root: ng.IRootScopeService,
        private AdminData: IgniteAdminData,
        private IgniteMessages: ReturnType<typeof MessagesFactory>,
        private loading: ReturnType<typeof LoadingServiceFactory>,
        private fb: FormBuilder
    ) {
        this.form = fb.group({
            email: ['', [Validators.required, Validators.email]],
            password: ['', customRequireValidator],
            confirm: ['', customRequireValidator],
            firstName: ['', customRequireValidator],
            lastName: ['', customRequireValidator],
            phone: [''],
            country: ['', customRequireValidator],
            company: ['', customRequireValidator]
        });
    }

    setServerError(error: any) {
        this.serverError = error;
    }

    getData(): SignupUserInfo {
        return {
            email: this.form.get('email').value.trim(),
            password: this.form.get('password').value.trim(),
            firstName: this.form.get('firstName').value.trim(),
            lastName: this.form.get('lastName').value.trim(),
            company: this.form.get('company').value.trim(),
            country: this.form.get('country').value.trim()
        };
    }

    createUser() {
        this.isLoading = true;

        this.form.markAsTouched();
        this.form.updateValueAndValidity();

        this.setServerError(null);

        if (!this.form.valid) return;

        this.loading.start('createUser');

        const data = this.getData();

        this.AdminData.registerUser(data)
        .then(() => {
            this.$root.$broadcast('userCreated');
            this.IgniteMessages.showInfo(`User ${data.email} created`);
            this.close.emit();
        })
        .catch((err) => {
            this.loading.finish('createUser');
            this.IgniteMessages.showError(null, err);
            this.setServerError(err.message);
        });
    }
}
