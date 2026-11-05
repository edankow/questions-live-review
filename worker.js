import { DurableObject } from 'cloudflare:workers';

const inviteLifetime = 15 * 60 * 1000;
const roomLifetime = 2 * 60 * 60 * 1000;
const roomPath = /^\/live\/([a-z0-9]{8})-([a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12})\/(host|guest)$/;
const questionId = /^[A-Za-z0-9.]{1,32}$/;

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
        if (age < -60000 || age > inviteLifetime) return new Response(null, { status: 410 });
        return env.ROOMS.getByName(created + '-' + id).fetch(request);
    }
};

export class LiveReviewRoom extends DurableObject {
    async fetch(request) {
        const host = new URL(request.url).pathname.endsWith('/host');
        const sockets = this.ctx.getWebSockets();
        if (host) {
            if (sockets.length || await this.ctx.storage.get('created')) return new Response(null, { status: 409 });
            await this.ctx.storage.put('created', true);
            await this.ctx.storage.setAlarm(Date.now() + roomLifetime);
        } else if (sockets.length !== 1 || sockets[0].readyState !== WebSocket.OPEN ||
                   sockets[0].deserializeAttachment()?.role !== 'host' ||
                   await this.ctx.storage.get('joined')) {
            return new Response(null, { status: 409 });
        } else {
            await this.ctx.storage.put('joined', true);
        }

        const { 0: client, 1: server } = new WebSocketPair();
        const role = host ? 'host' : 'guest';
        this.ctx.acceptWebSocket(server, [role]);
        server.serializeAttachment({ role, windowStart: Date.now(), count: 0 });
        if (host) {
            server.send(JSON.stringify({ type: 'inviting' }));
        } else {
            const connected = JSON.stringify({ type: 'connected' });
            sockets[0].send(connected);
            server.send(connected);
        }
        return new Response(null, { status: 101, webSocket: client });
    }

    webSocketMessage(socket, raw) {
        const state = socket.deserializeAttachment();
        const now = Date.now();
        if (now - state.windowStart >= 1000) {
            state.windowStart = now;
            state.count = 0;
        }
        if (++state.count > 20 || typeof raw !== 'string' || raw.length > 128000) {
            socket.close(1008, 'Message limit exceeded');
            return;
        }
        socket.serializeAttachment(state);
        const peer = this.ctx.getWebSockets(state.role === 'host' ? 'guest' : 'host')[0];
        if (!peer || peer.readyState !== WebSocket.OPEN) return;
        let input;
        try {
            input = JSON.parse(raw);
        } catch {
            return;
        }
        const partner = peer.deserializeAttachment();
        const message = this.reviewMessage(state, partner, input);
        socket.serializeAttachment(state);
        if (message) {
            if (message.type === 'question') peer.serializeAttachment(partner);
            peer.send(JSON.stringify(message));
        }
    }

    reviewMessage(state, partner, input) {
        if (input?.type === 'profile' && !state.sentProfile && typeof input.name === 'string' &&
            input.name.trim() && input.name.length <= 120) {
            state.sentProfile = true;
            return { type: 'profile', name: input.name.trim() };
        }
        if (input?.type === 'queue' && state.role === 'guest' && !state.sentQueue &&
            Array.isArray(input.ids) && input.ids.length <= 5000 &&
            input.ids.every(id => typeof id === 'string' && questionId.test(id))) {
            state.sentQueue = true;
            return { type: 'queue', ids: input.ids };
        }
        if (input?.type === 'rated' && !state.rated &&
            typeof input.id === 'string' && input.id === (state.role === 'host' ? state.question : partner.question)) {
            state.rated = input.id;
            return { type: 'rated', id: input.id };
        }
        if (state.role !== 'host' || state.finished || !partner.sentQueue ||
            (state.question && (state.rated !== state.question || partner.rated !== state.question))) return;
        if (input?.type === 'done') {
            state.finished = true;
            return { type: 'done' };
        }
        if (input?.type === 'question' && typeof input.id === 'string' && questionId.test(input.id) &&
            input.id !== state.question && Array.isArray(input.values) && input.values.length <= 20 &&
            input.values.every(value => typeof value === 'string' && value.length <= 500)) {
            state.question = input.id;
            state.rated = null;
            // Clear the guest's previous rating so the next question can be rated independently.
            partner.rated = null;
            return { type: 'question', id: input.id, values: input.values };
        }
    }

    webSocketClose() {
        this.closeRoom();
    }

    webSocketError() {
        this.closeRoom();
    }

    closeRoom() {
        for (const socket of this.ctx.getWebSockets()) socket.close(1000, 'Room closed');
    }

    async alarm() {
        this.closeRoom();
        await this.ctx.storage.deleteAll();
    }
}
