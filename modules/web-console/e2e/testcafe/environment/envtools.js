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

const request = require('request-promise-native');
const { spawn } = require('child_process');
const url = require('url');

const argv = require('minimist')(process.argv.slice(2));
const start = argv._.includes('start');
const stop = argv._.includes('stop');

const testUser = {
    password: 'a',
    email: 'a@example.com',
    firstName: 'John',
    lastName: 'Doe',
    company: 'TestCompany',
    country: 'Canada'
};

const insertTestUser = () => {
    return request({
        method: 'POST',
        uri: resolveUrl('/api/v1/signup'),
        body: testUser,
        json: true
    })
        .catch((err) => {throw err.message;});
};

const dropTestDB = () => {
    return request({
        method: 'DELETE',
        uri: resolveUrl('/api/v1/test/users/@example.com')
    })
        .catch((err) => {throw err.message;});
};


/**
 * Spawns a new process using the given command.
 * @param command {String} The command to run.
 * @param onResolveString {String} Await string in output.
 * @param cwd {String} Current working directory of the child process.
 * @param env {Object} Environment key-value pairs.
 * @return {Promise<ChildProcess>}
 */
const exec = (command, onResolveString, cwd, env) => {
    return new Promise((resolve) => {
        env = Object.assign({}, process.env, env, { FORCE_COLOR: true });

        const [cmd, ...args] = command.split(' ');

        const detached = process.platform !== 'win32';

        const child = spawn(cmd, args, {cwd, env, detached});

        if (detached) {
            // do something when app is closing
            process.on('exit', () => process.kill(-child.pid));

            // catches ctrl+c event
            process.on('SIGINT', () => process.kill(-child.pid));

            // catches "kill pid" (for example: nodemon restart)
            process.on('SIGUSR1', () => process.kill(-child.pid));
            process.on('SIGUSR2', () => process.kill(-child.pid));

            // catches uncaught exceptions
            process.on('uncaughtException', () => process.kill(-child.pid));
        }

        // Pipe error messages to stdout.
        child.stderr.on('data', (data) => {
            process.stdout.write(data.toString());
        });

        child.stdout.on('data', (data) => {
            process.stdout.write(data.toString());

            if (data.includes(onResolveString))
                resolve(child);
        });
    });
};

const startEnv = (webConsoleRootDirectoryPath = '../../') => {
    return Promise.resolve();

    return new Promise(async(resolve) => {
        const BACKEND_PORT = 3001;
        const command = `${process.platform === 'win32' ? 'npm.cmd' : 'npm'} start`;

        let port = 9001;

        if (process.env.APP_URL)
            port = parseInt(url.parse(process.env.APP_URL).port, 10) || 80;

        const backendInstanceLaunch = exec(command, 'Start listening', `${webConsoleRootDirectoryPath}backend`, {server_port: BACKEND_PORT, mongodb_url: mongoUrl});
        const frontendInstanceLaunch = exec(command, 'Compiled successfully', `${webConsoleRootDirectoryPath}frontend`, {BACKEND_URL: `http://localhost:${BACKEND_PORT}`, PORT: port});

        console.log('Building backend in progress...');
        await backendInstanceLaunch;
        console.log('Building backend done!');

        console.log('Building frontend in progress...');
        await frontendInstanceLaunch;
        console.log('Building frontend done!');

        resolve();
    });
};

if (start) {
    startEnv();

    process.on('SIGINT', async() => {
        await dropTestDB();

        process.exit(0);
    });
}

if (stop) {
    dropTestDB();

    console.log('Cleaning done...');
}


/**
 * @param {string} targetUrl
 * @param {string?} host
 * @returns {string}
 */
const resolveUrl = (targetUrl, host = 'http://localhost:9001') => {
    return url.resolve(process.env.APP_URL || host, targetUrl);
};

module.exports = { startEnv, insertTestUser, dropTestDB, resolveUrl };
