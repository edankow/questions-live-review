import { DurableObject } from 'cloudflare:workers';

const inviteLifetime = 15 * 60 * 1000;
const roomLifetime = 2 * 60 * 60 * 1000;
const roomPath = /^\/live\/([a-z0-9]{8})-([a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12})\/(host|guest)$/;
const questionId = /^[A-Za-z0-9.]{1,32}$/;

function validScenarioValues(values) {
    return Array.isArray(values) && values.length <= 20 &&
        values.every(value => typeof value === 'string' && value.length <= 500);
}

function endedLiveResponse() {
    // A browser cannot read an HTTP error from a failed WebSocket handshake.
    const { 0: client, 1: server } = new WebSocketPair();
    server.accept();
    server.send(JSON.stringify({ type: 'ended' }));
    server.close(1000, 'Room closed');
    return new Response(null, { status: 101, webSocket: client });
}

export default {
    fetch(request, env) {
        if (request.headers.get('Origin') !== env.SITE_ORIGIN) return new Response(null, { status: 403 });
        const path = new URL(request.url).pathname;
        const room = roomPath.exec(path);
        if (request.method !== 'GET' || request.headers.get('Upgrade')?.toLowerCase() !== 'websocket' || !room) {
            return new Response(null, { status: 404 });
        }
        const [, created, id] = room;
        const age = Date.now() - parseInt(created, 36);
        if (age < -60000 || age > roomLifetime) return endedLiveResponse();
        return env.ROOMS.getByName(created + '-' + id).fetch(request);
    }
};

export class LiveReviewRoom extends DurableObject {
    async fetch(request) {
        const url = new URL(request.url);
        const role = url.pathname.endsWith('/host') ? 'host' : 'guest';
        const otherRole = role === 'host' ? 'guest' : 'host';
        const token = url.searchParams.get('token');
        if (token && !/^[a-f0-9-]{36}$/.test(token)) return new Response(null, { status: 400 });
        if (await this.ctx.storage.get('closed')) return endedLiveResponse();
        let state = await this.roleState(role);
        const partner = await this.roleState(otherRole);
        if ((state || partner) && !this.peer('host') && !this.peer('guest')) {
            await this.closeRoom();
            return endedLiveResponse();
        }
        const resumed = Boolean(state);
        if (resumed) {
            if (!state.token || token !== state.token) return new Response(null, { status: 403 });
        } else {
            const created = parseInt(url.pathname.split('/')[2].split('-')[0], 36);
            if (Date.now() - created > inviteLifetime) return endedLiveResponse();
            if (role === 'guest' && !partner) return new Response(null, { status: 409 });
            if (role === 'host' && await this.ctx.storage.get('created')) return new Response(null, { status: 409 });
            state = { role, token, inbox: [], nextDelivery: 0, lastClientSeq: 0 };
        }
        if (role === 'host' && !resumed) {
            await this.ctx.storage.put('created', true);
            await this.ctx.storage.setAlarm(parseInt(url.pathname.split('/')[2].split('-')[0], 36) + roomLifetime);
        }

        const oldSockets = this.ctx.getWebSockets(role);
        const { 0: client, 1: server } = new WebSocketPair();
        state.connection = crypto.randomUUID();
        state.windowStart = Date.now();
        state.count = 0;
        await this.ctx.storage.put(role, state);
        this.ctx.acceptWebSocket(server, [role]);
        server.serializeAttachment({ role, connection: state.connection });
        for (const socket of oldSockets) socket.close(4001, 'Replaced by reconnect');
        const peer = this.peer(otherRole);
        if (resumed) {
            server.send(JSON.stringify({ type: 'resumed', paired: Boolean(partner), partnerOnline: Boolean(peer) }));
            for (const message of state.inbox) server.send(JSON.stringify(message));
            if (peer) peer.send(JSON.stringify({ type: 'partner-reconnected' }));
        } else if (role === 'host') {
            server.send(JSON.stringify({ type: 'inviting' }));
        } else {
            const connected = JSON.stringify({ type: 'connected' });
            if (peer) peer.send(connected);
            server.send(connected);
        }
        return new Response(null, { status: 101, webSocket: client });
    }

    peer(role) {
        return this.ctx.getWebSockets(role).find(socket => socket.readyState === WebSocket.OPEN);
    }

    async roleState(role) {
        let state = await this.ctx.storage.get(role);
        if (state) return state;
        // Preserve rooms opened by the previous Worker version during rollout.
        const socket = this.peer(role);
        const attachment = socket?.deserializeAttachment();
        if (!attachment || attachment.connection) return;
        state = { inbox: [], nextDelivery: 0, lastClientSeq: 0, token: null, ...attachment, connection: crypto.randomUUID() };
        socket.serializeAttachment({ role, connection: state.connection });
        await this.ctx.storage.put(role, state);
        return state;
    }

    async webSocketMessage(socket, raw) {
        const state = await this.roleState(socket.deserializeAttachment().role);
        const attachment = socket.deserializeAttachment();
        if (!state || state.connection !== attachment.connection || await this.ctx.storage.get('closed')) return;
        const now = Date.now();
        if (now - state.windowStart >= 1000) {
            state.windowStart = now;
            state.count = 0;
        }
        if (++state.count > 20 || typeof raw !== 'string' || raw.length > 128000) {
            socket.close(1008, 'Message limit exceeded');
            return;
        }
        await this.ctx.storage.put(state.role, state);
        let input;
        try {
            input = JSON.parse(raw);
        } catch {
            return;
        }
        if (input?.type === 'leave') {
            await this.closeRoom();
            return;
        }
        if (input?.type === 'ack' && Number.isSafeInteger(input.serverSeq) && input.serverSeq <= state.nextDelivery) {
            state.inbox = state.inbox.filter(message => message.serverSeq > input.serverSeq);
            await this.ctx.storage.put(state.role, state);
            return;
        }
        if (state.token) {
            if (!Number.isSafeInteger(input?.clientSeq) || input.clientSeq < 1 || input.clientSeq > state.lastClientSeq + 1) return;
            if (input.clientSeq <= state.lastClientSeq) {
                socket.send(JSON.stringify({ type: 'ack', clientSeq: input.clientSeq }));
                return;
            }
            state.lastClientSeq = input.clientSeq;
        }
        const otherRole = state.role === 'host' ? 'guest' : 'host';
        const partner = await this.roleState(otherRole);
        if (!partner) return;
        const message = this.reviewMessage(state, partner, input);
        if (message && partner.token) {
            message.serverSeq = ++partner.nextDelivery;
            partner.inbox.push(message);
            if (partner.inbox.length > 64 || JSON.stringify(partner).length > 120000) {
                await this.closeRoom();
                return;
            }
        }
        await this.ctx.storage.put({ [state.role]: state, [otherRole]: partner });
        if (state.token) socket.send(JSON.stringify({ type: 'ack', clientSeq: input.clientSeq }));
        if (message) {
            const peer = this.peer(otherRole);
            if (peer) peer.send(JSON.stringify(message));
        }
    }

    reviewMessage(state, partner, input) {
        if (input?.type === 'profile' && !state.sentProfile && typeof input.name === 'string' &&
            input.name.trim() && input.name.length <= 120) {
            state.sentProfile = true;
            state.scenarioEdits = input.scenarioEdits === true;
            state.inputTurns = input.inputTurns === true;
            return { type: 'profile', name: input.name.trim(), scenarioEdits: state.scenarioEdits, inputTurns: state.inputTurns };
        }
        if (input?.type === 'queue' && state.role === 'guest' && !state.sentQueue &&
            Array.isArray(input.ids) && input.ids.length <= 5000 &&
            input.ids.every(id => typeof id === 'string' && questionId.test(id))) {
            state.sentQueue = true;
            return { type: 'queue', ids: input.ids };
        }
        const host = state.role === 'host' ? state : partner;
        const revision = input?.revision ?? 0;
        if (input?.type === 'rated' && !state.rated &&
            typeof input.id === 'string' && input.id === host.question && revision === host.revision) {
            state.rated = input.id;
            return { type: 'rated', id: input.id, revision };
        }
        if (input?.type === 'draft-values' && state.role === 'guest' && typeof input.id === 'string' &&
            input.id === host.draft && state.submittedDraft !== input.id && validScenarioValues(input.values)) {
            state.submittedDraft = input.id;
            return { type: 'draft-values', id: input.id, values: input.values };
        }
        if (state.role !== 'host' || state.finished || !partner.sentQueue) return;

        const roundComplete = !state.question || (state.rated === state.question && partner.rated === state.question);
        if (input?.type === 'draft' && roundComplete && state.inputTurns && partner.inputTurns && !state.draft &&
            typeof input.id === 'string' && questionId.test(input.id) && input.id !== state.question) {
            state.draft = input.id;
            return { type: 'draft', id: input.id };
        }
        if (input?.type === 'done' && roundComplete && !state.draft) {
            state.finished = true;
            return { type: 'done' };
        }
        if (input?.type !== 'question' || typeof input.id !== 'string' || !questionId.test(input.id) ||
            !validScenarioValues(input.values)) return;

        const revised = input.id === state.question && state.scenarioEdits && partner.scenarioEdits &&
            revision === state.revision + 1;
        const draftReady = !state.draft || (state.draft === input.id && partner.submittedDraft === input.id);
        const fresh = input.id !== state.question && revision === 0 && roundComplete && draftReady;
        if (!revised && !fresh) return;

        state.question = input.id;
        state.revision = revision;
        state.rated = null;
        if (fresh) state.draft = null;
        partner.rated = null;
        return { type: 'question', id: input.id, values: input.values, revision };
    }

    async webSocketClose(socket) {
        const attachment = socket.deserializeAttachment();
        if (!attachment.connection) {
            await this.closeRoom();
            return;
        }
        const state = await this.ctx.storage.get(attachment.role);
        if (!state || state.connection !== attachment.connection || await this.ctx.storage.get('closed')) return;
        if (!state.token) {
            await this.closeRoom();
            return;
        }
        const peer = this.peer(state.role === 'host' ? 'guest' : 'host');
        if (peer) peer.send(JSON.stringify({ type: 'partner-disconnected' }));
        else await this.closeRoom();
    }

    async webSocketError(socket) {
        await this.webSocketClose(socket);
        socket.close(1011, 'Connection interrupted');
    }

    async closeRoom() {
        await this.ctx.storage.put('closed', true);
        await this.ctx.storage.delete(['host', 'guest']);
        for (const socket of this.ctx.getWebSockets()) socket.close(1000, 'Room closed');
    }

    async alarm() {
        await this.closeRoom();
        await this.ctx.storage.deleteAll();
    }
}
