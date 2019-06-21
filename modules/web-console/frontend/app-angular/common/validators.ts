import {FormControl} from '@angular/forms';

export const customRequireValidator = function(control: FormControl) {
    const isWhitespace = (control.value || '').trim().length === 0;
    const isValid = !isWhitespace;
    return isValid ? null : {required: true};
};
