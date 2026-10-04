import {
  AdapterError, resolveCapabilities,
  type AgentAdapter, type AgentDescriptor, type ConnectionDescriptor, type ConnectionRecord,
  type ConnectionState, type SessionDescriptor, type SessionHistory, type SessionUpdate,
  type PromptInput, type ChatMessage, type ManagementOperations,
  type ModelInfo, type ModelSelectionState, type SkillStatusEntry, type SkillStatusReport,
  type CronJob, type CronListResult, type CronSchedule, type CostSummary,
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

interface ServerSession {
  key?: string;
  title?: string | null;
  preview?: string | null;
  created_at?: string;
  updated_at?: string;
}

interface ServerThreadMessage {
  id?: string;
  role?: string;
  content?: string;
  createdAt?: number;
}

const NO_SESSION_ACTIONS = { rename: false, reset: false, delete: false, pin: false };
const SESSION_CHANNEL_PREFIX = 'websocket:';
/** Fixed chat id backing the stable 'main' session, so reconnects resume one conversation. */
const MAIN_CHAT_ID = 'main';

/**
 * Nanobot adapter. Chat streams over the native gateway WebSocket
 * (ws://host:port/?token=...) while sessions, history and management data are
 * read from Nanobot's built-in WebUI HTTP API on the same host:port
 * (bootstrap -> api_token -> /api/*). Keeping history server-side is what makes
 * conversations survive reconnects and app restarts.
 */
export class NanobotAdapter implements AgentAdapter {
  readonly connection: ConnectionDescriptor;
  readonly capabilities = resolveCapabilities('nanobot', {});
  readonly management: ManagementOperations;

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
  private readonly httpBase: string;
  private readonly secret: string | null;
  private apiToken: string | null = null;
  private apiTokenExpiresAt = 0;
  private readonly listeners: { [K in keyof Listeners]: Set<Listeners[K]> } = { update: new Set(), state: new Set(), sessions: new Set() };

  constructor(private readonly record: ConnectionRecord, options: { isFreeSlot?: boolean; webSocketFactory?: WebSocketFactory } = {}) {
    if (record.backendKind !== 'nanobot') throw new TypeError('Expected nanobot connection');
    this.connection = {
      id: record.id, backendKind: record.backendKind, transportKind: record.transportKind,
      label: record.label, createdAt: record.createdAt, environment: record.environment,
      isFreeSlot: options.isFreeSlot ?? false,
    };
    const url = new URL(record.url);
    this.secret = record.auth?.token ?? url.searchParams.get('token') ?? null;
    this.httpBase = `${url.protocol === 'wss:' ? 'https:' : 'http:'}//${url.host}`;
    if (this.secret && !url.searchParams.has('token')) url.searchParams.set('token', this.secret);
    url.searchParams.set('client_id', `clawket-${generateId().slice(0, 8)}`);
    this.transport = new DirectWsTransport({ url: url.toString(), webSocketFactory: options.webSocketFactory, autoReadyOnFirstFrame: false });
    this.transport.onOpen(() => { /* Nanobot sends `ready` right away; no client handshake */ });
    this.transport.onMessage(data => this.receive(data));
    this.transport.onStateChange(change => {
      if (change.state !== 'ready') this.setState(change.state === 'closed' ? 'offline' : change.state);
    });
    this.transport.onClose(() => { this.epoch++; this.pendingNewChat = null; });

    this.management = {
      models: {
        list: () => this.listModels(),
        getSelection: () => this.getModelSelection(),
        listThinkingLevels: () => [],
      },
      skills: {
        status: () => this.listSkills(),
      },
      cron: {
        list: () => this.listCron(),
      },
      usage: {
        cost: params => this.costSummary(params),
      },
    };
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
    const out: SessionDescriptor[] = [];
    const seen = new Set<string>();
    try {
      const data = await this.api<{ sessions?: ServerSession[] }>('/api/sessions');
      for (const session of data.sessions ?? []) {
        const key = this.toChatId(session.key);
        if (!key || seen.has(key)) continue;
        seen.add(key);
        out.push(this.describeSession(
          key,
          (session.title && session.title.trim()) || (session.preview && session.preview.trim()) || `Chat ${key.slice(0, 6)}`,
          Date.parse(session.updated_at ?? '') || Date.now(),
        ));
      }
    } catch { /* fall back to locally-known chats below */ }
    for (const key of this.transcripts.keys()) {
      if (seen.has(key) || key === this.defaultChatId) continue;
      seen.add(key);
      out.push(this.describeSession(key, `Chat ${key.slice(0, 6)}`, Date.now()));
    }
    if (this.defaultChatId && !seen.has(this.defaultChatId)) {
      out.unshift(this.describeSession(this.defaultChatId, 'Nanobot', Date.now()));
    }
    return out;
  }

  async loadSession(key: string, _options?: { limit?: number; cursor?: string }): Promise<SessionHistory> {
    try {
      const data = await this.api<{ messages?: ServerThreadMessage[] }>(`/api/sessions/${encodeURIComponent(this.toServerKey(key))}/webui-thread`);
      const messages: ChatMessage[] = (data.messages ?? [])
        .filter(message => message.role === 'user' || message.role === 'assistant')
        .map(message => ({
          id: message.id ?? generateId(),
          role: message.role as ChatMessage['role'],
          text: message.content ?? '',
          timestampMs: message.createdAt,
        }));
      this.transcripts.set(key, messages);
      return { key, messages, hasActiveRun: this.activeRunByChat.has(key) };
    } catch {
      return { key, messages: this.transcripts.get(key) ?? [], hasActiveRun: this.activeRunByChat.has(key) };
    }
  }

  async prompt(key: string, input: PromptInput): Promise<{ runId: string }> {
    if (this.state !== 'ready' || !this.defaultChatId) throw new AdapterError('server', 'Nanobot is not ready');
    const runId = input.idempotencyKey || generateId();
    this.pushMessage(key, { id: generateId(), role: 'user', text: input.text, timestampMs: Date.now() });
    this.emitUpdate({ type: 'run_started', sessionKey: key, runId });
    this.activeRunByChat.set(key, runId);
    this.runText.set(key, '');
    const payload = { type: 'message', chat_id: key, content: input.text };
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
    return this.describeSession(chatId, options?.title ?? `Chat ${chatId.slice(0, 6)}`, Date.now());
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

  // ---------------------------------------------------------------- HTTP API

  private async ensureApiToken(): Promise<string> {
    if (this.apiToken && Date.now() < this.apiTokenExpiresAt - 30_000) return this.apiToken;
    if (!this.secret) throw new AdapterError('server', 'Nanobot token missing');
    const response = await fetch(`${this.httpBase}/webui/bootstrap`, { headers: { Authorization: `Bearer ${this.secret}` } });
    if (!response.ok) throw new AdapterError('server', `Nanobot bootstrap failed (${response.status})`);
    const data = await response.json() as { api_token?: string; expires_in?: number };
    if (!data.api_token) throw new AdapterError('server', 'Nanobot did not return an API token');
    this.apiToken = data.api_token;
    this.apiTokenExpiresAt = Date.now() + (data.expires_in ?? 300) * 1000;
    return this.apiToken;
  }

  private async api<T>(path: string): Promise<T> {
    let token = await this.ensureApiToken();
    let response = await fetch(`${this.httpBase}${path}`, { headers: { Authorization: `Bearer ${token}` } });
    if (response.status === 401) {
      this.apiToken = null;
      token = await this.ensureApiToken();
      response = await fetch(`${this.httpBase}${path}`, { headers: { Authorization: `Bearer ${token}` } });
    }
    if (!response.ok) throw new AdapterError('server', `Nanobot API ${path} failed (${response.status})`);
    return await response.json() as T;
  }

  private toChatId(key: string | undefined | null): string | null {
    if (!key) return null;
    return key.startsWith(SESSION_CHANNEL_PREFIX) ? key.slice(SESSION_CHANNEL_PREFIX.length) : key;
  }

  private toServerKey(chatId: string): string {
    return chatId.startsWith(SESSION_CHANNEL_PREFIX) ? chatId : `${SESSION_CHANNEL_PREFIX}${chatId}`;
  }

  private async listModels(): Promise<ModelInfo[]> {
    const settings = await this.api<{ agent?: { provider?: string }; providers?: Array<{ name?: string; configured?: boolean }> }>('/api/settings');
    const providers = (settings.providers ?? [])
      .filter(provider => provider.configured && typeof provider.name === 'string')
      .map(provider => provider.name as string);
    const fallbackProvider = settings.agent?.provider;
    if (!providers.length && fallbackProvider) providers.push(fallbackProvider);
    const models: ModelInfo[] = [];
    const seen = new Set<string>();
    for (const provider of providers) {
      try {
        const data = await this.api<{ models?: Array<{ id?: string; label?: string | null }> }>(`/api/settings/provider-models?provider=${encodeURIComponent(provider)}`);
        for (const model of data.models ?? []) {
          if (!model.id || seen.has(model.id)) continue;
          seen.add(model.id);
          models.push({ id: model.id, name: model.label || model.id, provider });
        }
      } catch { /* a provider without a usable catalog is skipped */ }
    }
    return models;
  }

  private async getModelSelection(): Promise<ModelSelectionState> {
    const settings = await this.api<{ agent?: { model?: string; provider?: string; resolved_provider?: string; model_preset?: string | null } }>('/api/settings');
    const models = await this.listModels().catch(() => [] as ModelInfo[]);
    return {
      currentModel: settings.agent?.model ?? '',
      currentProvider: settings.agent?.provider ?? '',
      currentBaseUrl: this.httpBase,
      models,
    };
  }

  private async listSkills(): Promise<SkillStatusReport> {
    const data = await this.api<{ skills?: Array<{ name?: string; description?: string; source?: string; enabled?: boolean; available?: boolean; unavailable_reason?: string }> }>('/api/webui/skills');
    const skills: SkillStatusEntry[] = (data.skills ?? []).map(skill => ({
      name: skill.name ?? '',
      description: skill.description ?? '',
      source: skill.source ?? 'workspace',
      bundled: false,
      filePath: '',
      baseDir: '',
      skillKey: skill.name ?? '',
      always: false,
      disabled: skill.enabled === false,
      blockedByAllowlist: false,
      eligible: skill.enabled !== false,
      requirements: {},
      missing: {},
      configChecks: [],
      install: [],
    }));
    return { workspaceDir: '', managedSkillsDir: '', skills };
  }

  private async listCron(): Promise<CronListResult> {
    const data = await this.api<{ jobs?: Array<Record<string, unknown>> }>('/api/webui/automations');
    const jobs: CronJob[] = (data.jobs ?? []).map(job => this.toCronJob(job));
    return { jobs, total: jobs.length, offset: 0, limit: jobs.length, hasMore: false, nextOffset: null };
  }

  private toCronJob(raw: Record<string, unknown>): CronJob {
    const scheduleRaw = (raw.schedule ?? {}) as Record<string, unknown>;
    let schedule: CronSchedule;
    if (typeof scheduleRaw.expr === 'string' && scheduleRaw.expr) {
      schedule = { kind: 'cron', expr: scheduleRaw.expr, tz: typeof scheduleRaw.tz === 'string' ? scheduleRaw.tz : undefined };
    } else if (typeof scheduleRaw.at_ms === 'number') {
      schedule = { kind: 'at', at: new Date(scheduleRaw.at_ms).toISOString() };
    } else {
      schedule = { kind: 'every', everyMs: typeof scheduleRaw.every_ms === 'number' ? scheduleRaw.every_ms : 0 };
    }
    const payloadRaw = (raw.payload ?? {}) as Record<string, unknown>;
    const message = typeof payloadRaw.message === 'string' ? payloadRaw.message : '';
    const payload = payloadRaw.kind === 'system_event'
      ? { kind: 'systemEvent' as const, text: message }
      : { kind: 'agentTurn' as const, message };
    const stateRaw = (raw.state ?? {}) as Record<string, unknown>;
    const lastStatus = stateRaw.last_status;
    return {
      id: typeof raw.id === 'string' ? raw.id : generateId(),
      name: typeof raw.name === 'string' ? raw.name : 'job',
      enabled: raw.enabled !== false,
      createdAtMs: 0,
      updatedAtMs: 0,
      schedule,
      sessionTarget: 'main',
      wakeMode: 'next-heartbeat',
      payload,
      state: {
        nextRunAtMs: typeof stateRaw.next_run_at_ms === 'number' ? stateRaw.next_run_at_ms : undefined,
        lastRunAtMs: typeof stateRaw.last_run_at_ms === 'number' ? stateRaw.last_run_at_ms : undefined,
        lastStatus: lastStatus === 'ok' || lastStatus === 'error' || lastStatus === 'skipped' ? lastStatus : undefined,
      },
    };
  }

  private async costSummary(params: { startDate?: string; endDate?: string }): Promise<CostSummary> {
    const data = await this.api<{ days?: Array<Record<string, unknown>> }>('/api/settings/usage');
    const start = params?.startDate ?? '';
    const end = params?.endDate ?? start;
    const inRange = (data.days ?? []).filter(day => {
      const date = typeof day.date === 'string' ? day.date : '';
      if (!date) return false;
      if (start && date < start) return false;
      if (end && date > end) return false;
      return true;
    });
    const sum = (key: string) => inRange.reduce((total, day) => total + (typeof day[key] === 'number' ? day[key] as number : 0), 0);
    const totals = {
      input: sum('input_tokens'), output: sum('output_tokens'),
      cacheRead: sum('cache_read_tokens'), cacheWrite: sum('cache_write_tokens'),
      totalTokens: sum('total_tokens'),
      totalCost: 0, inputCost: 0, outputCost: 0, cacheReadCost: 0, cacheWriteCost: 0, missingCostEntries: 0,
    };
    return {
      updatedAt: Date.now(),
      days: inRange.length,
      daily: inRange.map(day => ({
        date: typeof day.date === 'string' ? day.date : '',
        input: typeof day.input_tokens === 'number' ? day.input_tokens : 0,
        output: typeof day.output_tokens === 'number' ? day.output_tokens : 0,
        cacheRead: typeof day.cache_read_tokens === 'number' ? day.cache_read_tokens : 0,
        cacheWrite: typeof day.cache_write_tokens === 'number' ? day.cache_write_tokens : 0,
        totalTokens: typeof day.total_tokens === 'number' ? day.total_tokens : 0,
        totalCost: 0, inputCost: 0, outputCost: 0, cacheReadCost: 0, cacheWriteCost: 0, missingCostEntries: 0,
      })),
      totals,
      costPresentation: { mode: 'unknown' },
    };
  }

  // ---------------------------------------------------------------- Streaming

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

  private describeSession(key: string, title: string, updatedAt: number): SessionDescriptor {
    return {
      connectionId: this.record.id,
      agentId: this.record.id,
      key,
      kind: 'direct',
      title,
      updatedAt,
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
        this.defaultChatId = MAIN_CHAT_ID;
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
