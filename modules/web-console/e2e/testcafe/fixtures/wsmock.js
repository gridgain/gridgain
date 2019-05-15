import { Selector, RequestMock, RequestHook } from 'testcafe';
import {createRegularUser} from '../roles';
import {pageSignin} from '../page-models/pageSignin';
import thebugger from 'thebugger';
import {resolveUrl} from '../environment/envtools';
import {connectedClustersBadge} from '../components/connectedClustersBadge';

const me = createRegularUser('iborisov+1@gridgain.com', '1');

import {WebSocketHook} from '../mocks/WebSocketHook';

const ws = new WebSocketHook();
ws.on('connection', ((client) => {
    ws.emit('agents:stat', {
        count: 2,
        hasDemo: true,
        clusters: [
            {
                id: '70831a7c-2b5e-4c11-8c08-5888911d5962',
                name: 'Cluster 1',
                nids: ['143048f1-b5b8-47d6-9239-fed76222efe3'],
                addresses: {
                    '143048f1-b5b8-47d6-9239-fed76222efe3': '10.0.75.1'
                },
                clients: {
                    '143048f1-b5b8-47d6-9239-fed76222efe3': false
                },
                clusterVersion: '8.8.0-SNAPSHOT',
                active: true,
                secured: false
            },
            {
                id: '70831a7c-2b5e-4c11-8c08-5888911d5963',
                name: 'Cluster 2',
                nids: ['143048f1-b5b8-47d6-9239-fed76222efe4'],
                addresses: {
                    '143048f1-b5b8-47d6-9239-fed76222efe3': '10.0.75.1'
                },
                clients: {
                    '143048f1-b5b8-47d6-9239-fed76222efe3': false
                },
                clusterVersion: '8.8.0-SNAPSHOT',
                active: true,
                secured: false
            }
        ]
    });
}));

fixture('WS mock').requestHooks(ws).after(async(t) => {
    ws.destroy();
});

test('Connected clusters', async(t) => {
    await t
    .useRole(me)
    .navigateTo(resolveUrl('/settings/profile'))
    .expect(connectedClustersBadge.textContent).eql('My Connected Clusters: 2');
});
