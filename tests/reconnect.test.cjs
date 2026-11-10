const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { webcrypto } = require('node:crypto');

const source = fs.readFileSync(path.join(__dirname, '../worker.js'), 'utf8')
    .replace("import { DurableObject } from 'cloudflare:workers';", '')
    .replace('export default', 'const router =').replace('export class', 'class');
const hostToken = '11111111-1111-4111-8111-111111111111';
const guestToken = '22222222-2222-4222-8222-222222222222';

function server() {
    const data = new Map();
    const sockets = [];
    let now = Date.now();
    const room = now.toString(36) + '-12345678-1234-4123-8123-123456789012';
    const ctx = {
        storage: {
            async get(key) { return structuredClone(data.get(key)); },
            async put(key, value) {
                if (typeof key === 'string') data.set(key, structuredClone(value));
                else for (const [k, v] of Object.entries(key)) data.set(k, structuredClone(v));
            },
            async setAlarm(time) { data.set('alarm', time); }, async deleteAll() { data.clear(); },
            async delete(keys) { for (const key of keys) data.delete(key); }
        },
        acceptWebSocket(socket, tags) { socket.tags = tags; sockets.push(socket); },
        getWebSockets(tag) { return sockets.filter(socket => socket.readyState === 1 && (!tag || socket.tags.includes(tag))); }
    };
    class Socket {
        constructor() { this.readyState = 1; this.messages = []; }
        accept() {}
        serializeAttachment(value) { this.attachment = structuredClone(value); }
        deserializeAttachment() { return structuredClone(this.attachment); }
        send(raw) { this.messages.push(JSON.parse(raw)); }
        close(code, reason) { this.readyState = 3; this.closeCode = code; }
    }
    const context = vm.createContext({
        URL, crypto: webcrypto, Date: class extends Date { static now() { return now; } },
        WebSocket: { OPEN: 1 }, WebSocketPair: class { constructor() {
            this[0] = new Socket(); this[1] = new Socket(); this[0].peer = this[1];
        } },
        DurableObject: class { constructor(context) { this.ctx = context; } },
        Response: class { constructor(body, init) { Object.assign(this, init); } }, ctx
    });
    vm.runInContext(source + '\nvar instance = new LiveReviewRoom(ctx);', context);
    const instance = context.instance;
    const connect = async (role, token = role === 'host' ? hostToken : guestToken) => {
        const response = await instance.fetch({ url: `https://room.test/live/${room}/${role}?token=${token}` });
        return { response, socket: sockets.at(-1) };
    };
    const send = (socket, message) => instance.webSocketMessage(socket, JSON.stringify(message));
    return { data, ctx, connect, send, instance, sockets, advance(ms) { now += ms; } };
}

async function paired() {
    const env = server();
    env.host = (await env.connect('host')).socket;
    env.guest = (await env.connect('guest')).socket;
    await env.send(env.host, { type: 'profile', name: 'Host', scenarioEdits: true, inputTurns: true, clientSeq: 1 });
    await env.send(env.guest, { type: 'profile', name: 'Guest', scenarioEdits: true, inputTurns: true, clientSeq: 1 });
    await env.send(env.guest, { type: 'queue', ids: ['1', '2'], clientSeq: 2 });
    await env.send(env.host, { type: 'question', id: '1', values: ['Alice'], revision: 0, clientSeq: 2 });
    return env;
}

function assertEnded(response) {
    assert.equal(response.status, 101);
    assert.equal(response.webSocket.peer.messages[0].type, 'ended');
    assert.equal(response.webSocket.peer.closeCode, 1000);
}

test('reload replaces only the authenticated role and ignores its old close event', async () => {
    const env = await paired();
    const replacement = await env.connect('host');
    assert.equal(replacement.response.status, 101);
    assert.equal(env.host.closeCode, 4001);
    await env.instance.webSocketClose(env.host);
    assert.equal(env.guest.readyState, 1);
    assert.equal(env.data.get('closed'), undefined);
    assert.ok(replacement.socket.messages.some(message => message.type === 'resumed' && message.paired));
    const rejected = await env.connect('host', guestToken);
    assert.equal(rejected.response.status, 403);
    assert.equal(replacement.socket.readyState, 1);
});

test('offline rating replays when one participant reconnects while the other stays', async () => {
    const env = await paired();
    env.guest.close();
    await env.instance.webSocketClose(env.guest);
    assert.equal(env.host.readyState, 1);
    await env.send(env.host, { type: 'rated', id: '1', revision: 0, clientSeq: 3 });
    const guest = await env.connect('guest');
    assert.equal(guest.response.status, 101);
    assert.ok(guest.socket.messages.some(message => message.type === 'rated' && message.serverSeq));
    assert.equal(env.data.get('host').rated, '1');
    assert.equal(env.data.get('host').question, '1');
});

test('the last disconnect ends the room and invalidates both participant links', async () => {
    for (const first of ['host', 'guest']) {
        const env = await paired();
        const second = first === 'host' ? 'guest' : 'host';
        env[first].close();
        await env.instance.webSocketClose(env[first]);
        assert.equal(env.data.get('closed'), undefined);
        assert.equal(env[second].readyState, 1);
        env[second].close();
        await env.instance.webSocketClose(env[second]);
        assert.equal(env.data.get('closed'), true);
        assert.equal(env.data.has('host'), false);
        assert.equal(env.data.has('guest'), false);
        assertEnded((await env.connect('host')).response);
        assertEnded((await env.connect('guest')).response);
        assertEnded((await env.connect('guest', hostToken)).response);
    }
});

test('empty rooms cannot be revived before delayed close callbacks arrive', async () => {
    const env = await paired();
    env.host.close();
    env.guest.close();
    assertEnded((await env.connect('host')).response);
    assert.equal(env.data.get('closed'), true);
    await env.instance.webSocketClose(env.host);
    await env.instance.webSocketClose(env.guest);
    assertEnded((await env.connect('guest')).response);
});

test('a connection error ends an empty room but preserves a connected partner', async () => {
    const env = await paired();
    await env.instance.webSocketError(env.guest);
    assert.equal(env.data.get('closed'), undefined);
    assert.equal(env.host.readyState, 1);
    await env.instance.webSocketError(env.host);
    assert.equal(env.data.get('closed'), true);
    assertEnded((await env.connect('host')).response);
});

test('acknowledged deliveries are removed and retransmitted client messages are deduplicated', async () => {
    const env = await paired();
    const nextDelivery = env.data.get('guest').nextDelivery;
    await env.send(env.host, { type: 'question', id: '1', values: ['Alice'], revision: 0, clientSeq: 2 });
    assert.equal(env.data.get('guest').nextDelivery, nextDelivery);
    await env.send(env.guest, { type: 'ack', serverSeq: nextDelivery });
    const guest = await env.connect('guest');
    assert.deepEqual(guest.socket.messages.map(message => message.type), ['resumed']);
});

test('reconnect works after invite expiry, but an unclaimed guest invitation still expires', async () => {
    const env = await paired();
    env.advance(16 * 60 * 1000);
    assert.equal((await env.connect('guest')).response.status, 101);
    const unclaimed = server();
    await unclaimed.connect('host');
    unclaimed.advance(16 * 60 * 1000);
    assertEnded((await unclaimed.connect('guest')).response);
});

test('explicit leave and room expiry close both sockets and prevent resuming', async () => {
    const env = await paired();
    await env.send(env.host, { type: 'leave' });
    assert.equal(env.host.closeCode, 1000);
    assert.equal(env.guest.closeCode, 1000);
    assertEnded((await env.connect('guest')).response);
    const expired = await paired();
    await expired.instance.alarm();
    assert.equal(expired.host.closeCode, 1000);
    assert.equal(expired.guest.closeCode, 1000);
    assert.equal(expired.data.size, 0);
});

test('rating progress continues after reconnect and still requires both ratings', async () => {
    const env = await paired();
    await env.send(env.host, { type: 'rated', id: '1', revision: 0, clientSeq: 3 });
    const guest = (await env.connect('guest')).socket;
    await env.send(env.host, { type: 'question', id: '2', values: ['School'], revision: 0, clientSeq: 4 });
    assert.equal(env.data.get('host').question, '1');
    await env.send(guest, { type: 'rated', id: '1', revision: 0, clientSeq: 3 });
    await env.send(env.host, { type: 'question', id: '2', values: ['School'], revision: 0, clientSeq: 5 });
    assert.equal(env.data.get('host').question, '2');
    assert.equal(env.data.get('host').rated, null);
    assert.equal(env.data.get('guest').rated, null);
});
