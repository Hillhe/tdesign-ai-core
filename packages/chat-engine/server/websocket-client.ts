/* eslint-disable max-classes-per-file */
import EventEmitter from '../utils/eventEmitter';
import { LoggerManager } from '../utils/logger';
import { ConnectionError, TimeoutError } from './errors';
import type { ConnectionInfo } from './types';

/**
 * WebSocket 连接状态
 */
export enum WebSocketConnectionState {
  DISCONNECTED = 'disconnected',
  CONNECTING = 'connecting',
  CONNECTED = 'connected',
  CLOSING = 'closing',
  CLOSED = 'closed',
  ERROR = 'error',
}

/**
 * WebSocket 客户端配置
 */
export interface WebSocketClientConfig {
  /** 连接超时时间（毫秒），默认 30000 */
  timeout?: number;
  /** 重连间隔（毫秒），默认 1000 */
  retryInterval?: number;
  /** 最大重连次数，默认 0（不重连） */
  maxRetries?: number;
  /** 心跳间隔（毫秒），默认 0（禁用） */
  heartbeatInterval?: number;
}

/**
 * 默认 WebSocket 配置
 *
 * 默认禁用心跳超时检测和自动重连，连接保活依赖 WebSocket 协议层的 ping/pong 或服务端心跳。
 * 如需启用心跳/重连（如 OpenClaw 适配器），请在 connect() 时显式传入配置。
 */
export const DEFAULT_WS_CONFIG: Required<WebSocketClientConfig> = {
  timeout: 30000,
  retryInterval: 1000,
  maxRetries: 0,
  heartbeatInterval: 0,
};

/**
 * WebSocket 状态变更事件
 */
export interface WSStateChangeEvent {
  connectionId: string;
  from: WebSocketConnectionState;
  to: WebSocketConnectionState;
  timestamp: number;
}

/**
 * WebSocket 客户端
 *
 * 设计原则：
 * - 与 SSEClient 保持相同的事件接口（message、error、complete、start、stateChange）
 * - 默认禁用心跳保活和自动重连（heartbeatInterval=0, maxRetries=0）
 * - 如需启用（如 OpenClaw 适配器），在 connect() 时显式传入非零配置
 * - 支持消息发送和接收
 */
export class WebSocketClient extends EventEmitter {
  public readonly connectionId: string;

  private state = WebSocketConnectionState.DISCONNECTED;

  private ws: WebSocket | null = null;

  private config: Required<WebSocketClientConfig>;

  private logger = LoggerManager.getLogger();

  private url: string;

  private connectionInfo: ConnectionInfo;

  private retryCount = 0;

  private reconnectTimer?: ReturnType<typeof setTimeout>;

  private heartbeatTimer?: ReturnType<typeof setInterval>;

  private timeoutTimer?: ReturnType<typeof setTimeout>;

  private manualClose = false;

  private firstMessageReceived = false;

  /** 当前正在等待的 connect promise */
  private connectPromise: Promise<void> | null = null;

  /** 用于在连接成功时 resolve 正在等待的 connect promise */
  private connectResolve: (() => void) | null = null;

  /** 用于在 close/重试耗尽时 reject 正在等待的 connect promise */
  private connectReject: ((reason: Error) => void) | null = null;

  constructor(url: string) {
    super();
    this.url = url;
    this.connectionId = this.generateConnectionId();
    this.config = { ...DEFAULT_WS_CONFIG };

    this.connectionInfo = {
      id: this.connectionId,
      url,
      state: this.state,
      createdAt: Date.now(),
      retryCount: 0,
      lastActivity: Date.now(),
      stats: {},
    };

    this.setupInternalEventHandlers();
  }

  /**
   * 连接 WebSocket 服务
   */
  async connect(config?: WebSocketClientConfig): Promise<void> {
    if (this.state === WebSocketConnectionState.CONNECTED) {
      return;
    }
    if (this.state === WebSocketConnectionState.CONNECTING && this.connectPromise) {
      return this.connectPromise;
    }

    this.config = {
      timeout: config?.timeout ?? DEFAULT_WS_CONFIG.timeout,
      retryInterval: config?.retryInterval ?? DEFAULT_WS_CONFIG.retryInterval,
      maxRetries: config?.maxRetries ?? DEFAULT_WS_CONFIG.maxRetries,
      heartbeatInterval: config?.heartbeatInterval ?? DEFAULT_WS_CONFIG.heartbeatInterval,
    };

    this.manualClose = false;
    this.retryCount = 0;

    this.connectPromise = new Promise((resolve, reject) => {
      this.connectResolve = resolve;
      this.connectReject = reject;
      this.openSocket();
    });

    return this.connectPromise;
  }

  /**
   * 发送消息
   */
  send(data: string | object): void {
    if (this.state !== WebSocketConnectionState.CONNECTED || !this.ws) {
      this.logger.warn(`Cannot send message: WebSocket not connected (state: ${this.state})`);
      return;
    }

    const message = typeof data === 'string' ? data : JSON.stringify(data);
    this.ws.send(message);
    this.connectionInfo.lastActivity = Date.now();
  }

  /**
   * 关闭连接
   */
  async close(): Promise<void> {
    if (this.state === WebSocketConnectionState.DISCONNECTED || this.state === WebSocketConnectionState.CLOSING || this.state === WebSocketConnectionState.CLOSED) {
      return;
    }

    this.manualClose = true;
    this.setState(WebSocketConnectionState.CLOSING);
    this.logger.info(`WebSocket ${this.connectionId} close() called by client`, {
      state: this.state,
      url: this.url,
    });

    try {
      this.clearTimers();

      // 如果 connect 还在 pending（CONNECTING 阶段被 close），reject 它
      if (this.connectReject) {
        this.rejectConnect(new ConnectionError('Connection closed before established'));
      }

      if (this.ws) {
        // 先清除原生 handlers，防止 close 握手期间回调引发竞态
        this.ws.onopen = null;
        this.ws.onmessage = null;
        this.ws.onerror = null;
        this.ws.onclose = null;
        this.ws.close(1000, 'Client initiated close');
        this.ws = null;
      }

      this.emit('complete', true);
    } catch (error) {
      this.logger.error('WebSocket close failed:', error);
      this.emit('error', error);
    } finally {
      this.setState(WebSocketConnectionState.CLOSED);
    }
  }

  /**
   * 中止连接（与 SSEClient 接口保持一致）
   */
  async abort(): Promise<void> {
    return this.close();
  }

  /**
   * 获取连接状态
   */
  getStatus(): WebSocketConnectionState {
    return this.state;
  }

  /**
   * 获取连接信息
   */
  getInfo(): ConnectionInfo {
    return {
      ...this.connectionInfo,
      state: this.state,
      retryCount: this.retryCount,
    };
  }

  /**
   * 检查是否已连接
   */
  isConnected(): boolean {
    return this.state === WebSocketConnectionState.CONNECTED && this.ws?.readyState === WebSocket.OPEN;
  }

  /**
   * 建立 WebSocket 连接
   */
  private establishConnection(): Promise<void> {
    return this.connect(this.config);
  }

  /**
   * 建立一次 WebSocket 连接尝试。重连时会复用同一个 connectPromise，
   * 直到成功 resolve 或重试耗尽 reject。
   */
  private openSocket(): void {
    this.clearTimeout();
    this.setState(WebSocketConnectionState.CONNECTING);

    try {
      const socket = new WebSocket(this.url);
      this.ws = socket;

      // 连接超时处理
      if (this.config.timeout > 0) {
        this.timeoutTimer = setTimeout(() => {
          if (this.state === WebSocketConnectionState.CONNECTING && this.ws === socket) {
            const error = new TimeoutError(`WebSocket connection timeout after ${this.config.timeout}ms`);
            this.closeSocketSilently(socket, 'Connection timeout');
            this.ws = null;
            this.handleConnectionError(error);
          }
        }, this.config.timeout);
      }

      socket.onopen = () => {
        if (this.ws !== socket) return;

        this.clearTimeout();
        this.setState(WebSocketConnectionState.CONNECTED);
        this.connectionInfo.lastActivity = Date.now();

        // 启动心跳
        this.startHeartbeat();

        this.logger.info(`WebSocket ${this.connectionId} connected to ${this.url}`);
        this.resolveConnect();
      };

      socket.onmessage = (event) => {
        if (this.ws !== socket) return;
        this.handleMessage(event.data);
      };

      socket.onerror = (event) => {
        if (this.ws !== socket) return;
        this.logger.error(`WebSocket ${this.connectionId} error:`, event);
        const error = new ConnectionError('WebSocket connection error');
        this.emit('error', error);
      };

      socket.onclose = (event) => {
        if (this.ws !== socket) return;
        this.handleClose(event.code, event.reason);
      };
    } catch (error) {
      this.handleConnectionError(error as Error);
    }
  }

  /**
   * 处理接收到的消息
   */
  private handleMessage(data: string): void {
    this.connectionInfo.lastActivity = Date.now();

    // 首次消息，发出 start 事件
    if (!this.firstMessageReceived) {
      this.firstMessageReceived = true;
      this.emit('start', data);
    }

    // 尝试解析 JSON
    try {
      const parsed = JSON.parse(data);
      this.emit('message', {
        event: parsed.event || parsed.type || 'message',
        data: parsed,
      });
    } catch {
      // 非 JSON 消息，原样发送
      this.emit('message', {
        event: 'message',
        data,
      });
    }
  }

  /**
   * 处理连接关闭
   */
  private handleClose(code: number, reason: string): void {
    this.clearTimers();
    this.ws = null;

    this.logger.debug(`[WS handleClose] code=${code}, reason="${reason}", manualClose=${this.manualClose}`);

    if (this.manualClose || code === 1000) {
      // 正常关闭
      this.setState(WebSocketConnectionState.CLOSED);
      this.emit('complete', this.manualClose);
      this.logger.info(`WebSocket ${this.connectionId} closed normally`);
    } else {
      // 异常关闭，尝试重连
      this.setState(WebSocketConnectionState.DISCONNECTED);
      this.logger.warn(`WebSocket ${this.connectionId} closed unexpectedly: ${code} - ${reason}`);

      if (this.shouldReconnect()) {
        this.scheduleReconnect();
      } else {
        const error = new ConnectionError(`WebSocket closed: ${code} - ${reason}`);
        this.emit('complete', false);
        this.emit('error', error);
        this.rejectConnect(error);
      }
    }
  }

  /**
   * 处理连接错误
   */
  private handleConnectionError(error: Error): void {
    this.clearTimeout();
    this.connectionInfo.error = error;
    this.setState(WebSocketConnectionState.ERROR);
    this.emit('error', error);

    if (this.shouldReconnect()) {
      this.scheduleReconnect();
    } else {
      this.rejectConnect(error);
    }
  }

  /**
   * 判断是否应该重连
   */
  private shouldReconnect(): boolean {
    return !this.manualClose && this.retryCount < this.config.maxRetries;
  }

  /**
   * 调度重连
   */
  private scheduleReconnect(): void {
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = undefined;
    }

    this.retryCount++;
    const delay = this.config.retryInterval * Math.pow(1.5, this.retryCount - 1);

    this.logger.info(`WebSocket ${this.connectionId} reconnecting in ${delay}ms (attempt ${this.retryCount}/${this.config.maxRetries})`);

    this.reconnectTimer = setTimeout(() => {
      if (!this.manualClose && this.state !== WebSocketConnectionState.CONNECTED) {
        this.firstMessageReceived = false;
        this.openSocket();
      }
    }, delay);
  }

  private resolveConnect(): void {
    this.connectResolve?.();
    this.connectPromise = null;
    this.connectResolve = null;
    this.connectReject = null;
  }

  private rejectConnect(error: Error): void {
    this.connectReject?.(error);
    this.connectPromise = null;
    this.connectResolve = null;
    this.connectReject = null;
  }

  /**
   * 启动心跳
   *
   * 注意：OpenClaw Gateway 不支持 RPC 方式的 heartbeat 方法，
   * 这里仅通过检测连接活跃度来判断是否需要重连，
   * 不再主动发送 heartbeat 请求帧。
   */
  private startHeartbeat(): void {
    if (this.config.heartbeatInterval <= 0) return;

    this.heartbeatTimer = setInterval(() => {
      if (!this.isConnected()) {
        return;
      }

      // 检测连接是否长时间无活动（2倍心跳间隔），如果是则主动关闭触发重连
      const idleTime = Date.now() - this.connectionInfo.lastActivity;
      if (idleTime > this.config.heartbeatInterval * 2) {
        this.logger.warn(`WebSocket ${this.connectionId} idle for ${idleTime}ms, reconnecting...`);
        this.ws?.close(4000, 'Heartbeat timeout');
      }
    }, this.config.heartbeatInterval);
  }

  /**
   * 清理超时定时器
   */
  private clearTimeout(): void {
    if (this.timeoutTimer) {
      clearTimeout(this.timeoutTimer);
      this.timeoutTimer = undefined;
    }
  }

  /**
   * 清理所有定时器
   */
  private clearTimers(): void {
    this.clearTimeout();

    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = undefined;
    }

    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = undefined;
    }
  }

  private closeSocketSilently(socket: WebSocket, reason: string): void {
    socket.onopen = null;
    socket.onmessage = null;
    socket.onerror = null;
    socket.onclose = null;

    if (socket.readyState === WebSocket.CLOSING || socket.readyState === WebSocket.CLOSED) return;

    try {
      socket.close(4000, reason);
    } catch {
      // Ignore best-effort cleanup errors from stale sockets.
    }
  }

  /**
   * 设置连接状态
   */
  private setState(newState: WebSocketConnectionState): void {
    const oldState = this.state;
    this.state = newState;
    this.connectionInfo.state = newState;

    const stateChangeEvent: WSStateChangeEvent = {
      connectionId: this.connectionId,
      from: oldState,
      to: newState,
      timestamp: Date.now(),
    };

    this.emit('stateChange', stateChangeEvent);
    this.logger.debug(`WebSocket ${this.connectionId} state: ${oldState} -> ${newState}`);
  }

  /**
   * 设置内部事件处理器
   */
  private setupInternalEventHandlers(): void {
    this.on('error', (error) => {
      this.logger.error(`WebSocket Client ${this.connectionId} error:`, error);
    });

    this.on('complete', (isAborted) => {
      this.logger.info(`WebSocket Client ${this.connectionId} completed, aborted: ${isAborted}`);
    });
  }

  /**
   * 生成连接 ID
   */
  private generateConnectionId(): string {
    const timestamp = Date.now();
    return `ws_${timestamp}_${Math.random().toString(36).substr(2, 9)}`;
  }
}
