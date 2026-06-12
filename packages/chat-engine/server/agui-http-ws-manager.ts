import { AGUIAdapter, type AGUIAdapterCallbacks } from '../adapters/agui';
import { AGUIEventType } from '../adapters/agui/types/events';
import { ChatEngineEventType } from '../event-bus';
import type { AIMessageContent, ChatRequestParams, SSEChunkData } from '../type';
import type { StreamContext } from '../stream-handlers/types';
import { LoggerManager } from '../utils/logger';
import { WebSocketClient, WebSocketConnectionState, type WSStateChangeEvent } from './websocket-client';

type RouteKey = `${string}:${string}`;

export interface AGUIHttpWSConnectionOptions {
  debugger?: boolean;
  heartbeatInterval?: number;
  maxRetries?: number;
  onStatusChange?: (event: Record<string, any>) => void;
  retryInterval?: number;
  timeout?: number;
}

interface ActiveRunRoute {
  adapter: AGUIAdapter;
  context: StreamContext;
  messageId: string;
  params: ChatRequestParams;
  routeId: string;
  runId: string;
  started: boolean;
  threadId: string;
}

export interface AGUIHttpWSRouteOptions {
  context: StreamContext;
  messageId: string;
  params: ChatRequestParams;
  runId: string;
  threadId: string;
}

const buildRouteKey = (threadId: string, runId: string): RouteKey => `${threadId}:${runId}`;
const DEFAULT_HEARTBEAT_INTERVAL = 5000;
const DEFAULT_MAX_RETRIES = 3;
const CONNECTION_EVENT_TYPES = new Set(['pong', 'ping', 'heartbeat']);

export class AGUIHttpWSManager {
  private static instance: AGUIHttpWSManager;

  private ackTimer: ReturnType<typeof setInterval> | null = null;

  private connectionId = '';

  private debuggerEnabled = false;

  private debugRouteGroups = new Set<RouteKey>();

  private heartbeatTimer: ReturnType<typeof setInterval> | null = null;

  private lastWSError: unknown = null;

  private logger = LoggerManager.getLogger();

  private onStatusChange: ((event: Record<string, any>) => void) | undefined;

  private reconnectResumePending = false;

  private routes = new Map<RouteKey, ActiveRunRoute>();

  private routeSeed = 0;

  private wsClient: WebSocketClient | null = null;

  private wsEndpoint = '';

  static getInstance() {
    if (!AGUIHttpWSManager.instance) {
      AGUIHttpWSManager.instance = new AGUIHttpWSManager();
    }
    return AGUIHttpWSManager.instance;
  }

  get currentConnectionId() {
    return this.connectionId;
  }

  async ensureConnected(endpoint: string, options: AGUIHttpWSConnectionOptions = {}): Promise<string> {
    if (!endpoint) return '';
    this.debuggerEnabled = Boolean(options.debugger);
    this.onStatusChange = options.onStatusChange;

    if (this.wsClient && this.wsEndpoint === endpoint && this.wsClient.isConnected()) {
      this.startHeartbeat(options.heartbeatInterval);
      this.emitStatus('connected', { connectionId: this.connectionId, reused: true });
      return this.connectionId;
    }

    await this.disconnect();
    this.emitStatus('connecting', { endpoint });
    this.wsEndpoint = endpoint;
    this.connectionId = '';
    this.lastWSError = null;
    this.wsClient = new WebSocketClient(endpoint);

    this.wsClient.on('stateChange', (event) => {
      this.handleWSStateChange(event as WSStateChangeEvent);
    });

    this.wsClient.on('message', (chunk: SSEChunkData) => {
      this.handleMessage(chunk);
    });

    this.wsClient.on('error', (error) => {
      this.lastWSError = error;
      this.emitStatus('error', {
        message: error instanceof Error ? error.message : String(error),
      });
    });

    this.wsClient.on('complete', (isAborted) => {
      this.emitStatus('complete', { isAborted });
      if (!isAborted) {
        if (this.lastWSError) {
          this.routes.forEach((route) => {
            route.context.handleError(route.messageId, this.lastWSError);
          });
        }
        return;
      }
      this.routes.forEach((route) => {
        route.context.handleComplete(route.messageId, true, route.params);
      });
      this.routes.clear();
    });

    await this.wsClient.connect({
      heartbeatInterval: 0,
      maxRetries: options.maxRetries ?? DEFAULT_MAX_RETRIES,
      retryInterval: options.retryInterval,
      timeout: options.timeout,
    });
    this.startHeartbeat(options.heartbeatInterval);

    return this.waitForAck();
  }

  registerRoute(options: AGUIHttpWSRouteOptions): string {
    const key = buildRouteKey(options.threadId, options.runId);
    const routeId = this.createRouteId();
    if (this.routes.has(key)) {
      this.logger.warn(`[AGUI HTTP+WS] duplicate routeKey registered: ${key}`);
    }
    this.routes.set(key, {
      adapter: new AGUIAdapter(),
      context: options.context,
      messageId: options.messageId,
      params: options.params,
      routeId,
      runId: options.runId,
      started: false,
      threadId: options.threadId,
    });
    this.debugRouteLifecycle('route subscribed', {
      messageId: options.messageId,
      routeId,
      routeKey: key,
      routeKeys: Array.from(this.routes.keys()),
      routeSize: this.routes.size,
    });
    return routeId;
  }

  unregisterRoute(threadId: string, runId: string, routeId?: string): void {
    const routeKey = buildRouteKey(threadId, runId);
    const route = this.routes.get(routeKey);
    if (!route) {
      this.debugRouteLifecycle('route unsubscribe skipped: route missing', {
        routeId,
        routeKey,
        routeKeys: Array.from(this.routes.keys()),
        routeSize: this.routes.size,
      });
      return;
    }

    if (routeId && route.routeId !== routeId) {
      this.debugRouteLifecycle('route unsubscribe skipped: routeId mismatch', {
        activeRouteId: route.routeId,
        routeId,
        routeKey,
        routeKeys: Array.from(this.routes.keys()),
        routeSize: this.routes.size,
      });
      return;
    }

    this.routes.delete(routeKey);
    this.debugRouteLifecycle('route unsubscribed', {
      routeId: route.routeId,
      routeKey,
      routeKeys: Array.from(this.routes.keys()),
      routeSize: this.routes.size,
    });
    this.closeDebugRouteGroup(routeKey);
  }

  async disconnect(): Promise<void> {
    if (this.ackTimer) {
      clearInterval(this.ackTimer);
      this.ackTimer = null;
    }
    this.stopHeartbeat();
    if (this.wsClient) {
      this.logger.info('[AGUI HTTP+WS] disconnect global websocket by client', {
        routeKeys: Array.from(this.routes.keys()),
        routeSize: this.routes.size,
        wsEndpoint: this.wsEndpoint,
      });
      this.wsClient.removeAllListeners();
      await this.wsClient.close().catch(() => {});
      this.wsClient = null;
    }
    this.closeDebugRouteGroups();
  }

  private startHeartbeat(interval = DEFAULT_HEARTBEAT_INTERVAL): void {
    this.stopHeartbeat();
    if (!interval || interval <= 0) return;

    this.heartbeatTimer = setInterval(() => {
      if (!this.wsClient?.isConnected()) return;
      const pingMessage = {
        type: 'ping',
        ts: Date.now(),
      };
      this.wsClient.send(pingMessage);
      this.emitStatus('ping', pingMessage);
    }, interval);
  }

  private stopHeartbeat(): void {
    if (!this.heartbeatTimer) return;
    clearInterval(this.heartbeatTimer);
    this.heartbeatTimer = null;
  }

  private handleWSStateChange(event: WSStateChangeEvent): void {
    this.emitStatus('stateChange', event as unknown as Record<string, any>);

    if (
      event.to === WebSocketConnectionState.DISCONNECTED
      || event.to === WebSocketConnectionState.ERROR
    ) {
      this.prepareRoutesForReconnectResume();
      return;
    }

    if (event.to === WebSocketConnectionState.CONNECTED && this.reconnectResumePending) {
      void this.resumeRoutesAfterReconnect();
    }
  }

  private prepareRoutesForReconnectResume(): void {
    if (this.routes.size === 0) return;

    this.reconnectResumePending = true;
    this.routes.forEach((route) => {
      route.started = false;
    });
    this.debugRouteLifecycle('routes paused before reconnect resume', {
      routeKeys: Array.from(this.routes.keys()),
      routeSize: this.routes.size,
    });
  }

  private async resumeRoutesAfterReconnect(): Promise<void> {
    this.reconnectResumePending = false;
    const routes = Array.from(this.routes.entries());
    this.debugRouteLifecycle('resume routes after websocket reconnect start', {
      routeKeys: routes.map(([routeKey]) => routeKey),
      routeSize: routes.length,
    });

    await Promise.all(routes.map(([routeKey, route]) => this.resumeRouteAfterReconnect(routeKey, route)));
  }

  private async resumeRouteAfterReconnect(routeKey: RouteKey, route: ActiveRunRoute): Promise<void> {
    const request = (await route.context.config.onRequest?.(route.params)) || route.params;
    const requestBody = this.rewriteRunRequestBody(request.body, route);

    if (!requestBody) {
      this.logger.warn(`[AGUI HTTP+WS] reconnect resume skipped: invalid request body ${routeKey}`);
      return;
    }

    try {
      const response = await fetch(route.context.config.endpoint!, {
        method: request.method || 'POST',
        headers: {
          'Content-Type': 'application/json',
          ...request.headers,
        },
        body: requestBody,
      });

      if (!response.ok) {
        throw response;
      }

      this.debugRouteLifecycle('resume route request accepted after reconnect', {
        routeId: route.routeId,
        routeKey,
        status: response.status,
      });
    } catch (error) {
      route.context.handleError(route.messageId, error);
      this.logger.error(`[AGUI HTTP+WS] reconnect resume failed: ${routeKey}`, error);
    }
  }

  private rewriteRunRequestBody(body: BodyInit | null | undefined, route: ActiveRunRoute): BodyInit | null {
    if (!body || typeof body !== 'string') return null;

    try {
      const parsedBody = JSON.parse(body);
      parsedBody.threadId = route.threadId;
      parsedBody.runId = route.runId;
      return JSON.stringify(parsedBody);
    } catch {
      return null;
    }
  }

  private handleMessage(chunk: SSEChunkData): void {
    const event = this.normalizeEvent(chunk);
    if (!event) return;

    if (event.type === 'ack') {
      this.connectionId = event.connectionId || '';
      this.emitStatus('ack', event);
      return;
    }

    const threadId = event.threadId ? String(event.threadId) : '';
    const runId = event.runId ? String(event.runId) : '';
    if (!threadId || !runId) {
      if (CONNECTION_EVENT_TYPES.has(event.type)) return;
      this.warnMissingRouteFields(event, chunk);
      return;
    }

    const routeKey = buildRouteKey(threadId, runId);
    const route = this.routes.get(routeKey);
    if (!route) return;

    this.debugRouteChunk(routeKey, chunk);

    if (!route.started && event.type !== AGUIEventType.RUN_STARTED) return;
    if (event.type === AGUIEventType.RUN_STARTED) {
      route.started = true;
    }

    const normalizedChunk: SSEChunkData = {
      event: event.type,
      data: event,
    };
    const routedChunk = route.context.config.onChunk?.(normalizedChunk) ?? normalizedChunk;
    if (!routedChunk) return;

    let result = route.adapter.handleAGUIEvent(routedChunk, this.createCallbacks(route));
    if (route.context.config.onMessage) {
      const userResult = route.context.config.onMessage(
        routedChunk,
        route.context.messageStore.getMessageByID(route.messageId),
        result,
      );
      if (userResult) {
        result = userResult;
      }
    }

    route.context.eventBus.emit(ChatEngineEventType.REQUEST_STREAM, {
      messageId: route.messageId,
      chunk: routedChunk,
      content: result,
    });

    route.context.processMessageResult(route.messageId, result);
  }

  private createCallbacks(route: ActiveRunRoute): AGUIAdapterCallbacks {
    return {
      onRunStart: (event) => {
        route.adapter.reset();
        route.context.config.onStart?.(JSON.stringify(event));
        route.context.eventBus.emit(ChatEngineEventType.AGUI_RUN_START, {
          runId: event.runId || route.runId,
          threadId: event.threadId || route.threadId,
          timestamp: Date.now(),
        });
      },
      onRunComplete: (isAborted, _params, event) => {
        route.context.handleComplete(route.messageId, isAborted, route.params, event);
        if (!isAborted) {
          route.context.eventBus.emit(ChatEngineEventType.AGUI_RUN_COMPLETE, {
            runId: event?.runId || route.runId,
            threadId: event?.threadId || route.threadId,
            timestamp: Date.now(),
          });
        }
        this.unregisterRoute(route.threadId, route.runId, route.routeId);
      },
      onRunError: (error) => {
        route.context.handleError(route.messageId, error);
        route.context.eventBus.emit(ChatEngineEventType.AGUI_RUN_ERROR, {
          error,
        });
      },
    };
  }

  private normalizeEvent(chunk: SSEChunkData): any {
    const raw = typeof chunk.data === 'string' ? this.safeParse(chunk.data) : chunk.data;
    if (!raw) return null;
    if (raw.type) return raw;
    if (raw.event && raw.data) {
      return typeof raw.data === 'string' ? this.safeParse(raw.data) : raw.data;
    }
    return null;
  }

  private safeParse(value: string): any {
    try {
      return JSON.parse(value);
    } catch {
      return null;
    }
  }

  private createRouteId(): string {
    this.routeSeed += 1;
    return `${Date.now()}_${this.routeSeed}`;
  }

  private debugRouteChunk(routeKey: RouteKey, chunk: SSEChunkData): void {
    if (!this.debuggerEnabled) return;

    if (!this.debugRouteGroups.has(routeKey)) {
      // 暂时不启用 console group，避免高频事件下控制台分组过多。
      // this.logger.group?.(routeKey);
      this.debugRouteGroups.add(routeKey);
    }
    // 高频事件先不打印原始 chunk，避免控制台和 WS 监控压力过大。
    // this.logger.info('AGUI HTTP+WS route chunk', chunk);
  }

  private warnMissingRouteFields(event: Record<string, any>, chunk: SSEChunkData): void {
    if (!this.debuggerEnabled) return;

    this.logger.warn('[AGUI HTTP+WS] event missing threadId or runId', {
      chunk,
      event,
    });
  }

  private debugRouteLifecycle(message: string, payload: Record<string, any>): void {
    if (!this.debuggerEnabled) return;

    this.logger.info(`[AGUI HTTP+WS] ${message}`, payload);
  }

  private closeDebugRouteGroup(routeKey: RouteKey): void {
    if (!this.debugRouteGroups.delete(routeKey)) return;

    // this.logger.groupEnd?.();
  }

  private closeDebugRouteGroups(): void {
    this.debugRouteGroups.forEach(() => {
      // this.logger.groupEnd?.();
    });
    this.debugRouteGroups.clear();
  }

  private waitForAck(): Promise<string> {
    if (this.connectionId) return Promise.resolve(this.connectionId);

    return new Promise((resolve) => {
      const startedAt = Date.now();
      this.ackTimer = setInterval(() => {
        if (this.connectionId || Date.now() - startedAt > 3000) {
          if (this.ackTimer) {
            clearInterval(this.ackTimer);
            this.ackTimer = null;
          }
          resolve(this.connectionId);
        }
      }, 50);
    });
  }

  private emitStatus(type: string, payload: Record<string, any> = {}): void {
    this.onStatusChange?.({
      type,
      ...payload,
      timestamp: Date.now(),
    });
  }
}

export const aguiHttpWSManager = AGUIHttpWSManager.getInstance();

export const disconnectAGUIHttpWS = () => aguiHttpWSManager.disconnect();
