import {
  AdapterError, resolveCapabilities,
  type AgentAdapter, type AgentDescriptor, type ConnectionDescriptor, type ConnectionRecord,
  type ConnectionState, type SessionDescriptor, type SessionHistory, type SessionUpdate,
  type PromptInput, type ChatMessage,
} from '@clawket/agent-protocol';
import { generateId } from '../../services/gateway-auth';
import { DirectWsTransport } from '../transports/direct-ws';
import type { WebSocketFactory } from '../transports/types';

type Listeners = {
  update: (update: SessionUpdate) => void;
  state: (state: ConnectionState, reason?: string) => void;
  sessions: (sessions: SessionDescriptor[]) => void;
};

interface NanobotInbound {
  event?: string;
  type?: string;
  chat_id?: string;
  client_id?: string;
  text?: string;
  stream_id?: string;
  detail?: string;
}

const NO_SESSION_ACTIONS = { rename: false, reset: false, delete: false, pin: false };

/**
 * Direct-WebSocket adapter for the Nanobot gateway (ws://host:port/?token=...).
 * Maps Nanobot's event protocol (ready/attached/delta/stream_end/message) onto the
 * AgentAdapter surface; transcripts are buffered locally (Nanobot exposes no history API).
 */
export class NanobotAdapter implements AgentAdapter {
  readonly connection: ConnectionDescriptor;
  readonly capabilities = resolveCapabilities('nanobot', {});

  private transport: DirectWsTransport;
  private currentState: ConnectionState = 'idle';
  private defaultChatId: string | null = null;
  private readonly transcripts = new Map<string, ChatMessage[]>();
  private readonly runText = new Map<string, string>();
  private readonly activeRunByChat = new Map<string, string>();
  private pendingNewChat: { resolve: (chatId: string) => void; reject: (reason: Error) => void; timer: ReturnType<typeof setTimeout> } | null = null;
  private epoch = 0;
  private connectPromise: Promise<void> | null = null;
  private cancelConnect: (() => void) | null = null;
  private readonly listeners: { [K in keyof Listeners]: Set<Listeners[K]> } = { update: new Set(), state: new Set(), sessions: new Set() };

  constructor(private readonly record: ConnectionRecord, options: { isFreeSlot?: boolean; webSocketFactory?: WebSocketFactory } = {}) {
    if (record.backendKind !== 'nanobot') throw new TypeError('Expected nanobot connection');
    this.connection = {
      id: record.id, backendKind: record.backendKind, transportKind: record.transportKind,
      label: record.label, createdAt: record.createdAt, environment: record.environment,
      isFreeSlot: options.isFreeSlot ?? false,
    };
    const url = new URL(record.url);
    if (record.auth?.token && !url.searchParams.has('token')) url.searchParams.set('token', record.auth.token);
    url.searchParams.set('client_id', `clawket-${generateId().slice(0, 8)}`);
    this.transport = new DirectWsTransport({ url: url.toString(), webSocketFactory: options.webSocketFactory, autoReadyOnFirstFrame: false });
    this.transport.onOpen(() => { /* Nanobot sends `ready` right away; no client handshake */ });
    this.transport.onMessage(data => this.receive(data));
    this.transport.onStateChange(change => {
      if (change.state !== 'ready') this.setState(change.state === 'closed' ? 'offline' : change.state);
    });
    this.transport.onClose(() => { this.epoch++; this.pendingNewChat = null; });
  }

  get state(): ConnectionState { return this.currentState; }

  async connect(): Promise<void> {
    if (this.state === 'ready') return;
    if (this.connectPromise) return this.connectPromise;
    const epoch = ++this.epoch;
    const ready = new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => finish(new AdapterError('timeout', 'Nanobot did not become ready')), 25_000);
      let settled = false;
      const off = this.on('state', state => { if (state === 'ready') finish(); });
      const finish = (error?: Error) => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        off();
        this.cancelConnect = null;
        if (error) reject(error); else resolve();
      };
      this.cancelConnect = () => finish(new AdapterError('network', 'Nanobot connection cancelled'));
    });
    const attempt = ready.finally(() => { if (this.connectPromise === attempt) this.connectPromise = null; });
    this.connectPromise = attempt;
    if (epoch !== this.epoch) return attempt;
    this.setState('connecting');
    try {
      this.transport.connect();
    } catch (error) {
      this.connectPromise = null;
      this.setState('offline');
      throw error instanceof AdapterError ? error : new AdapterError('network', error instanceof Error ? error.message : String(error));
    }
    return attempt;
  }

  disconnect(): void {
    this.epoch++;
    this.cancelConnect?.();
    this.connectPromise = null;
    this.transport.disconnect();
    this.setState('offline');
  }

  async probe(_timeoutMs = 8_000): Promise<boolean> {
    const epoch = this.epoch;
    try {
      await this.connect();
      if (epoch !== this.epoch) return false;
      return this.state === 'ready' || this.transport.hasReceivedValidFrame;
    } catch { return false; }
  }

  async listAgents(): Promise<AgentDescriptor[]> {
    return [{
      connectionId: this.record.id,
      agentId: this.record.id,
      name: 'Nanobot',
      isMain: true,
      mainSessionKey: this.defaultChatId ?? '',
    }];
  }

  async listSessions(_agentId?: string): Promise<SessionDescriptor[]> {
    const sessions: SessionDescriptor[] = [];
    if (this.defaultChatId) sessions.push(this.describeSession(this.defaultChatId, 'Nanobot'));
    for (const key of this.transcripts.keys()) {
      if (key !== this.defaultChatId) sessions.push(this.describeSession(key, `Chat ${key.slice(0, 6)}`));
    }
    return sessions;
  }

  async loadSession(key: string, _options?: { limit?: number; cursor?: string }): Promise<SessionHistory> {
    return { key, messages: this.transcripts.get(key) ?? [], hasActiveRun: this.activeRunByChat.has(key) };
  }

  async prompt(key: string, input: PromptInput): Promise<{ runId: string }> {
    if (this.state !== 'ready' || !this.defaultChatId) throw new AdapterError('server', 'Nanobot is not ready');
    const runId = input.idempotencyKey || generateId();
    this.pushMessage(key, { id: generateId(), role: 'user', text: input.text, timestampMs: Date.now() });
    this.emitUpdate({ type: 'run_started', sessionKey: key, runId });
    this.activeRunByChat.set(key, runId);
    this.runText.set(key, '');
    const payload = key === this.defaultChatId
      ? { content: input.text }
      : { type: 'message', chat_id: key, content: input.text };
    try {
      this.transport.send(JSON.stringify(payload));
    } catch (error) {
      this.activeRunByChat.delete(key);
      this.runText.delete(key);
      throw error instanceof AdapterError ? error : new AdapterError('network', error instanceof Error ? error.message : String(error));
    }
    return { runId };
  }

  async cancel(_key: string, _runId?: string): Promise<void> {
    // Nanobot's WebSocket protocol has no abort message.
  }

  async createSession(_agentId: string, options?: { title?: string }): Promise<SessionDescriptor> {
    const chatId = await this.requestNewChat();
    return this.describeSession(chatId, options?.title ?? `Chat ${chatId.slice(0, 6)}`);
  }

  async patchSession(_key: string, _patch: { title?: string }): Promise<void> {
    // Nanobot has no rename API; titles are local-only metadata.
  }

  async resetSession(_key: string): Promise<void> { /* no reset API on Nanobot */ }
  async deleteSession(_key: string): Promise<void> { /* no delete API on Nanobot */ }

  on<K extends keyof Listeners>(event: K, listener: Listeners[K]): () => void {
    this.listeners[event].add(listener);
    return () => { this.listeners[event].delete(listener); };
  }

  private requestNewChat(): Promise<string> {
    if (this.pendingNewChat) return Promise.reject(new AdapterError('unsupported', 'A session is already being created'));
    return new Promise<string>((resolve, reject) => {
      const timer = setTimeout(() => {
        const pending = this.pendingNewChat;
        this.pendingNewChat = null;
        pending?.reject(new AdapterError('timeout', 'Nanobot new_chat timed out'));
      }, 15_000);
      this.pendingNewChat = { resolve, reject, timer };
      try {
        this.transport.send(JSON.stringify({ type: 'new_chat' }));
      } catch (error) {
        clearTimeout(timer);
        this.pendingNewChat = null;
        reject(error instanceof AdapterError ? error : new AdapterError('network', error instanceof Error ? error.message : String(error)));
      }
    });
  }

  private describeSession(key: string, title: string): SessionDescriptor {
    return {
      connectionId: this.record.id,
      agentId: this.record.id,
      key,
      kind: 'direct',
      title,
      updatedAt: Date.now(),
      hasActiveRun: this.activeRunByChat.has(key),
      allowedActions: { ...NO_SESSION_ACTIONS },
    };
  }

  private receive(data: unknown): void {
    let msg: NanobotInbound;
    try { msg = typeof data === 'string' ? JSON.parse(data) as NanobotInbound : (data ?? {}) as NanobotInbound; }
    catch { return; }
    const tag = msg.event ?? msg.type;
    const key = msg.chat_id ?? this.defaultChatId ?? undefined;
    switch (tag) {
      case 'ready': {
        if (msg.chat_id) this.defaultChatId = msg.chat_id;
        if (!this.defaultChatId) break;
        if (!this.transcripts.has(this.defaultChatId)) this.transcripts.set(this.defaultChatId, []);
        this.transport.markReady();
        this.setState('ready');
        this.announceSessions();
        break;
      }
      case 'attached': {
        if (msg.chat_id && this.pendingNewChat) {
          clearTimeout(this.pendingNewChat.timer);
          const pending = this.pendingNewChat;
          this.pendingNewChat = null;
          if (!this.transcripts.has(msg.chat_id)) this.transcripts.set(msg.chat_id, []);
          pending.resolve(msg.chat_id);
          this.announceSessions();
        }
        break;
      }
      case 'delta': {
        if (!key) break;
        const runId = this.ensureRun(key);
        this.runText.set(key, (this.runText.get(key) ?? '') + (msg.text ?? ''));
        this.emitUpdate({ type: 'agent_message_chunk', sessionKey: key, runId, text: msg.text ?? '', textMode: 'delta' });
        break;
      }
      case 'reasoning_delta': {
        if (!key) break;
        this.emitUpdate({ type: 'agent_thought_chunk', sessionKey: key, runId: this.ensureRun(key), text: msg.text ?? '' });
        break;
      }
      case 'stream_end': {
        if (key) this.finishRun(key);
        break;
      }
      case 'message': {
        if (!key) break;
        if (this.activeRunByChat.has(key)) {
          if (msg.text) this.runText.set(key, msg.text);
          this.finishRun(key);
        } else {
          this.pushMessage(key, { id: generateId(), role: 'assistant', text: msg.text ?? '', timestampMs: Date.now() });
          this.announceSessions();
        }
        break;
      }
      case 'error': {
        this.emitUpdate({ type: 'error', code: 'server', message: msg.detail ?? 'Nanobot error' });
        break;
      }
      default: break;
    }
  }

  private ensureRun(key: string): string {
    let runId = this.activeRunByChat.get(key);
    if (!runId) {
      runId = generateId();
      this.activeRunByChat.set(key, runId);
      this.runText.set(key, '');
      this.emitUpdate({ type: 'run_started', sessionKey: key, runId });
    }
    return runId;
  }

  private finishRun(key: string): void {
    const runId = this.activeRunByChat.get(key);
    if (!runId) return;
    const content = this.runText.get(key) ?? '';
    this.pushMessage(key, { id: generateId(), role: 'assistant', text: content, timestampMs: Date.now() });
    this.emitUpdate({ type: 'run_finished', sessionKey: key, runId, stopReason: 'end_turn', message: { role: 'assistant', content } });
    this.activeRunByChat.delete(key);
    this.runText.delete(key);
    this.announceSessions();
  }

  private pushMessage(key: string, message: ChatMessage): void {
    const list = this.transcripts.get(key);
    if (list) list.push(message);
    else this.transcripts.set(key, [message]);
  }

  private announceSessions(): void {
    void this.listSessions().then(sessions => {
      for (const listener of this.listeners.sessions) listener(sessions);
    });
  }

  private emitUpdate(update: SessionUpdate): void {
    for (const listener of this.listeners.update) listener(update);
  }

  private setState(state: ConnectionState): void {
    this.currentState = state;
    for (const listener of this.listeners.state) listener(state);
  }
}
