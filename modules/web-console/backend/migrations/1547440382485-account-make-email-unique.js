/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 * 
 * Commons Clause Restriction
 * 
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 * 
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 * 
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

const _ = require('lodash');

const log = require('./migration-utils').log;

function deduplicateAccounts(model) {
    const accountsModel = model('Account');
    const spaceModel = model('Space');

    return accountsModel.aggregate([
        {$group: {_id: '$email', count: {$sum: 1}}},
        {$match: {count: {$gt: 1}}}
    ]).exec()
        .then((accounts) => _.map(accounts, '_id'))
        .then((emails) => Promise.all(
            _.map(emails, (email) => accountsModel.find({email}, {_id: 1, email: 1, lastActivity: 1, lastLogin: 1}).lean().exec())
        ))
        .then((promises) => {
            const duplicates = _.flatMap(promises, (accounts) => _.sortBy(accounts, [(a) => a.lastActivity || '', 'lastLogin']).slice(0, -1));

            if (_.isEmpty(duplicates))
                log('Duplicates not found!');
            else {
                log(`Duplicates found: ${_.size(duplicates)}`);

                _.forEach(duplicates, (dup) => log(`  ID: ${dup._id}, e-mail: ${dup.email}`));
            }

            return _.map(duplicates, '_id');
        })
        .then((accountIds) => {
            if (_.isEmpty(accountIds))
                return Promise.resolve();

            return spaceModel.find({owner: {$in: accountIds}}, {_id: 1}).lean().exec()
                .then((spaces) => _.map(spaces, '_id'))
                .then((spaceIds) =>
                    Promise.all([
                        model('Cluster').remove({space: {$in: spaceIds}}).exec(),
                        model('Cache').remove({space: {$in: spaceIds}}).exec(),
                        model('DomainModel').remove({space: {$in: spaceIds}}).exec(),
                        model('Igfs').remove({space: {$in: spaceIds}}).exec(),
                        model('Notebook').remove({space: {$in: spaceIds}}).exec(),
                        model('Activities').remove({owner: accountIds}).exec(),
                        model('Notifications').remove({owner: accountIds}).exec(),
                        spaceModel.remove({owner: accountIds}).exec(),
                        accountsModel.remove({_id: accountIds}).exec()
                    ])
                )
                .then(() => {
                    const conditions = _.map(accountIds, (accountId) => ({session: {$regex: `"${accountId}"`}}));

                    return accountsModel.db.collection('sessions').deleteMany({$or: conditions});
                });
        });
}

exports.up = function up(done) {
    deduplicateAccounts((name) => this(name))
        .then(() => this('Account').collection.createIndex({email: 1}, {unique: true, background: false}))
        .then(() => done())
        .catch(done);
};

exports.down = function down(done) {
    log('Account migration can not be reverted');

    done();
};
