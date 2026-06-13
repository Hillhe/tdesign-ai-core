import { aguiHttpWSManager } from '../server/agui-http-ws-manager';
import { ChatEngineEventType } from '../event-bus';
import { getHTTPStatusCode } from '../server/errors';
import type { ChatRequestParams, ChatServiceConfig } from '../type';
import { LoggerManager } from '../utils/logger';
import type { IStreamHandler, StreamContext, StreamLifecycleContext, StreamProtocol } from './types';

const toWebSocketEndpoint = (endpoint?: string): string => {
  if (!endpoint) return '';
  if (endpoint.startsWith('ws://') || endpoint.startsWith('wss://')) return endpoint;

  try {
    const url = new URL(endpoint);
    url.protocol = url.protocol === 'https:' ? 'wss:' : 'ws:';
    url.pathname = url.pathname.replace(/\/agui\/run\/[^/]+$/, '/agui/ws');
    url.search = '';
    return url.toString();
  } catch {
    return '';
  }
};

const getConnectionOptions = (config: ChatServiceConfig) => ({
  debugger: config.aguiHttpWs?.debugger,
  heartbeatInterval: config.aguiHttpWs?.heartbeatInterval,
  maxRetries: config.aguiHttpWs?.maxRetries,
  onStatusChange: config.aguiHttpWs?.onStatusChange,
  retryInterval: config.aguiHttpWs?.retryInterval,
  timeout: config.aguiHttpWs?.timeout,
});

const extractBodyField = (body: unknown, field: 'threadId' | 'runId'): string => {
  if (!body || typeof body !== 'object') return '';
  const value = (body as Record<string, unknown>)[field];
  return value === undefined || value === null ? '' : String(value);
};

const injectConnectionId = (
  request: ChatRequestParams & RequestInit,
  connectionId: string,
): { body: BodyInit | null | undefined; runId: string; threadId: string } => {
  if (!request.body || typeof request.body !== 'string') {
    return {
      body: request.body,
      runId: '',
      threadId: '',
    };
  }

  try {
    const body = JSON.parse(request.body);
    // 暂不透传 connectionId，后续确认后端消费方式后再恢复。
    // if (connectionId) {
    //   body.forwardedProps = {
    //     ...(body.forwardedProps || {}),
    //     connectionId,
    //   };
    // }
    return {
      body: JSON.stringify(body),
      runId: extractBodyField(body, 'runId'),
      threadId: extractBodyField(body, 'threadId'),
    };
  } catch {
    return {
      body: request.body,
      runId: '',
      threadId: '',
    };
  }
};

export class AGUIHttpWSStreamHandler implements IStreamHandler {
  readonly protocol: StreamProtocol = 'agui-http-ws';

  private activeRouteKeys = new Set<{ routeId: string; threadId: string; runId: string }>();

  private debuggerEnabled = false;

  private logger = LoggerManager.getLogger();

  private wsEndpoint = '';

  async initialize(config: ChatServiceConfig): Promise<void> {
    this.debuggerEnabled = Boolean(config.aguiHttpWs?.debugger);
    this.wsEndpoint = config.aguiHttpWs?.wsEndpoint || toWebSocketEndpoint(config.endpoint);
  }

  async startGlobalWS(config: ChatServiceConfig): Promise<string> {
    this.debuggerEnabled = Boolean(config.aguiHttpWs?.debugger);
    const wsEndpoint = config.aguiHttpWs?.wsEndpoint || this.wsEndpoint || toWebSocketEndpoint(config.endpoint);
    return aguiHttpWSManager.ensureConnected(wsEndpoint, getConnectionOptions(config));
  }

  async stopGlobalWS(): Promise<void> {
    await aguiHttpWSManager.disconnect();
  }

  async handleStream(params: ChatRequestParams, context: StreamContext): Promise<void> {
    const { config, messageId } = context;
    if (!messageId || !config.endpoint) return;
    this.debuggerEnabled = Boolean(config.aguiHttpWs?.debugger);

    const request = (await config.onRequest?.(params)) || params;
    const wsEndpoint = config.aguiHttpWs?.wsEndpoint || this.wsEndpoint || toWebSocketEndpoint(config.endpoint);
    const connectionId = await aguiHttpWSManager.ensureConnected(wsEndpoint, getConnectionOptions(config));
    const bodyMeta = injectConnectionId(request, connectionId);
    if (!bodyMeta.threadId || !bodyMeta.runId) {
      throw new Error('[ChatEngine] agui-http-ws requires request body threadId and runId');
    }

    const routeId = aguiHttpWSManager.registerRoute({
      context,
      messageId,
      params,
      runId: bodyMeta.runId,
      threadId: bodyMeta.threadId,
    });
    this.activeRouteKeys.add({
      routeId,
      runId: bodyMeta.runId,
      threadId: bodyMeta.threadId,
    });
    this.debugRouteLifecycle('handler active route added', {
      activeRouteKeys: this.getActiveRouteKeySnapshot(),
      activeRouteSize: this.activeRouteKeys.size,
      routeId,
      routeKey: `${bodyMeta.threadId}:${bodyMeta.runId}`,
    });

    try {
      const response = await fetch(config.endpoint, {
        method: request.method || 'POST',
        headers: {
          'Content-Type': 'application/json',
          ...request.headers,
        },
        body: bodyMeta.body,
        signal: request.signal,
      });

      if (!response.ok) {
        throw response;
      }
    } catch (error) {
      aguiHttpWSManager.unregisterRoute(bodyMeta.threadId, bodyMeta.runId, routeId);
      this.removeActiveRoute(routeId);
      if (getHTTPStatusCode(error) === 409) {
        context.handleError(messageId, error);
        return;
      }
      throw error;
    }
  }

  afterMessageUpdate(messageId: string, result: any, context: StreamLifecycleContext): void {
    const contents = Array.isArray(result) ? result : [result];
    for (const content of contents) {
      if ((content as any).data?.activityType) {
        context.eventBus.emit(ChatEngineEventType.AGUI_ACTIVITY, {
          activityType: (content as any).data.activityType,
          messageId,
          content: (content as any)?.data?.content,
        });
      }
      if ((content as any)?.data?.eventType?.startsWith('TOOL_CALL')) {
        context.eventBus.emit(ChatEngineEventType.AGUI_TOOLCALL, {
          toolCall: (content as any).data,
          eventType: (content as any).data.eventType,
        });
      }
    }
  }

  abort(): void {
    this.unregisterActiveRoutes();
  }

  async destroy(): Promise<void> {
    this.unregisterActiveRoutes();
  }

  private unregisterActiveRoutes(): void {
    this.debugRouteLifecycle('handler active routes unsubscribe start', {
      activeRouteKeys: this.getActiveRouteKeySnapshot(),
      activeRouteSize: this.activeRouteKeys.size,
    });
    this.activeRouteKeys.forEach(({ threadId, runId, routeId }) => {
      aguiHttpWSManager.unregisterRoute(threadId, runId, routeId);
    });
    this.activeRouteKeys.clear();
    this.debugRouteLifecycle('handler active routes unsubscribe end', {
      activeRouteKeys: this.getActiveRouteKeySnapshot(),
      activeRouteSize: this.activeRouteKeys.size,
    });
  }

  private removeActiveRoute(routeId: string): void {
    this.activeRouteKeys.forEach((route) => {
      if (route.routeId === routeId) {
        this.activeRouteKeys.delete(route);
      }
    });
    this.debugRouteLifecycle('handler active route removed', {
      activeRouteKeys: this.getActiveRouteKeySnapshot(),
      activeRouteSize: this.activeRouteKeys.size,
      routeId,
    });
  }

  private debugRouteLifecycle(message: string, payload: Record<string, any>): void {
    if (!this.debuggerEnabled) return;

    this.logger.info(`[AGUI HTTP+WS] ${message}`, payload);
  }

  private getActiveRouteKeySnapshot() {
    return Array.from(this.activeRouteKeys).map(({ routeId, threadId, runId }) => ({
      routeId,
      routeKey: `${threadId}:${runId}`,
    }));
  }
}
