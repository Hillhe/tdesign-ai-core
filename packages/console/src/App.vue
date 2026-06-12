<script setup lang="ts">
import { computed, reactive, ref, watch } from 'vue';
import type {
  AIMessageContent,
  ChatMessagesData,
  ChatRequestParams,
  ChatServiceConfig,
  ChatTransport,
  SSEChunkData,
} from '@tdesign/ai-chat-engine';
import { useChat } from './composables/useChat';

type Protocol = NonNullable<ChatServiceConfig['protocol']>;
type ActiveTab = 'chat' | 'rawMessages' | 'rawEvents';
const TOKEN_COOKIE_NAME = 'na-token';

const getSameOriginWSEndpoint = () => {
  const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
  return `${protocol}//${window.location.host}/api/agent-runtime/agui/ws`;
};

const normalizeWSEndpointForBrowser = (endpoint: string) => {
  if (!endpoint) return endpoint;

  try {
    const url = new URL(endpoint, window.location.href);
    if (url.host === window.location.host) return url.toString();
    if (!url.pathname.startsWith('/api/')) return endpoint;

    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    return `${protocol}//${window.location.host}${url.pathname}${url.search}`;
  } catch {
    return endpoint;
  }
};

const seedMessages: ChatMessagesData[] = [
  {
    id: 'welcome-user',
    role: 'user',
    status: 'complete',
    content: [{ type: 'text', data: '测试 ChatEngine 初始化消息' }],
  },
  {
    id: 'welcome-assistant',
    role: 'assistant',
    status: 'complete',
    content: [{ type: 'markdown', data: '已准备好。填写接口配置后发送一条消息即可开始调试。' }],
  },
];

const defaultRequestParams = {
  context: [],
  runId: '1781163843016',
  state: {},
  threadId: '2064976035696799746',
  tools: [],
  messages: [
    {
      role: 'user',
      id: '7ed94f5c-87e6-43d7-8bfa-3b5fcd512542',
      content: '你有什么能力',
      toolCallId: '',
    },
  ],
  forwardedProps: {
    agentCode: 'super-001',
    agentId: '2039282579535003649',
    fileIds: [],
    planActive: false,
    memoryActive: true,
    serviceParam: {},
    thinking: false,
    userInfo: {},
    hitl: null,
  },
};

const form = reactive({
  endpoint: '/api/agent-runtime/agui/run/super-001',
  wsEndpoint: getSameOriginWSEndpoint(),
  wsHeartbeatInterval: 5000,
  wsMaxRetries: 3,
  wsDebugger: false,
  token: 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJuYS11c2VyLWNlbnRlciIsInN1YiI6IjY0ODJjNDI2NTQ0OGNiNDhjNWUyOTRlYyIsInVzZXJJZCI6IjY0ODJjNDI2NTQ0OGNiNDhjNWUyOTRlYyIsInNlc3Npb25JZCI6ImYyODU2MWU1NjM5MDQ1ZGRhZjdkZWQzNmFlMTVjN2YwIiwicHJlZmVycmVkX3VzZXJuYW1lIjoicm9vdCIsInJlYWxfbmFtZSI6InJvb3QiLCJkZXBhcnRtZW50X2lkIjpudWxsLCJpYXQiOjE3ODEyMzA4MTMsImp0aSI6IjYxMTg4YzdkMDE0OTQ5MTJhZmJkMTlkMDM5YjZiODVjIn0.6-26m7TgaMQJdYJU5qmvx7G0uvN9pBxQLkWbnNnZtA0',
  transport: 'sse' as ChatTransport,
  protocol: 'agui-http-ws' as Protocol,
  prompt: '你有什么能力',
  requestParams: JSON.stringify(defaultRequestParams, null, 2),
});

const logs = ref<string[]>([]);
const errorMessage = ref('');
const requestCount = ref(0);
const activeTab = ref<ActiveTab>('chat');
const rawEvents = ref<string[]>([]);

const appendLog = (message: string) => {
  const time = new Date().toLocaleTimeString();
  logs.value = [`[${time}] ${message}`, ...logs.value].slice(0, 60);
};

const getChunkText = (chunk: SSEChunkData): string => {
  const data = typeof chunk.data === 'string' ? safeJsonParse(chunk.data) : chunk.data;
  if (typeof data === 'string') return data;
  if (typeof data?.data === 'string') return data.data;

  return (
    data?.choices?.[0]?.delta?.content ||
    data?.choices?.[0]?.message?.content ||
    data?.delta?.content ||
    data?.content ||
    data?.text ||
    ''
  );
};

const safeJsonParse = (value: string) => {
  try {
    return JSON.parse(value);
  } catch {
    return value;
  }
};

const formatJson = (value: unknown) => JSON.stringify(value, null, 2);

const recordRawEvent = (label: string, payload: unknown) => {
  const time = new Date().toLocaleTimeString();
  const normalizedPayload = typeof payload === 'string' ? safeJsonParse(payload) : payload;
  rawEvents.value = [`[${time}] ${label}\n${formatJson(normalizedPayload)}`, ...rawEvents.value].slice(0, 120);
};

const getTokenValue = () => form.token.trim().replace(/^Bearer\s+/i, '');

const syncTokenCookie = () => {
  const token = getTokenValue();
  if (!token) {
    document.cookie = `${TOKEN_COOKIE_NAME}=; Max-Age=0; Path=/; SameSite=Lax`;
    return;
  }

  document.cookie = `${TOKEN_COOKIE_NAME}=${encodeURIComponent(token)}; Max-Age=86400; Path=/; SameSite=Lax`;
};

watch(
  () => form.token,
  () => {
    syncTokenCookie();
  },
  { immediate: true },
);

const parseRequestParams = () => {
  try {
    errorMessage.value = '';
    return JSON.parse(form.requestParams) as Record<string, any>;
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : 'POST 参数 JSON 解析失败';
    return null;
  }
};

const buildRequestBody = (params: ChatRequestParams) => {
  const body = parseRequestParams();
  if (!body) return null;

  const prompt = params.prompt || '';
  const userMessageId = crypto.randomUUID?.() || `${Date.now()}`;
  const messages = Array.isArray(body.messages) ? [...body.messages] : [];
  const firstUserIndex = messages.findIndex((message) => message?.role === 'user');
  const targetIndex = firstUserIndex >= 0 ? firstUserIndex : messages.length;

  messages[targetIndex] = {
    ...messages[targetIndex],
    role: 'user',
    id: messages[targetIndex]?.id || userMessageId,
    content: prompt,
    toolCallId: messages[targetIndex]?.toolCallId || '',
  };

  return {
    ...body,
    runId: body.runId || `${Date.now()}`,
    forwardedProps: {
      ...(body.forwardedProps || {}),
      ...(form.protocol === 'agui-http-ws' ? { transport: 'ws' } : {}),
    },
    messages,
  };
};

const chatServiceConfig: ChatServiceConfig = {
  get endpoint() {
    return form.endpoint;
  },
  get transport() {
    return form.transport;
  },
  get protocol() {
    return form.protocol;
  },
  get aguiHttpWs() {
    return {
      debugger: form.wsDebugger,
      heartbeatInterval: form.wsHeartbeatInterval,
      maxRetries: form.wsMaxRetries,
      onStatusChange: (event: Record<string, any>) => {
        recordRawEvent(`ws:${event.type}`, event);
      },
      wsEndpoint: normalizeWSEndpointForBrowser(form.wsEndpoint),
    };
  },
  timeout: 60000,
  onRequest: (params: ChatRequestParams) => {
    const body = buildRequestBody(params);
    if (!body) return params;
    recordRawEvent('request body', body);

    return {
      ...params,
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        ...(form.token ? { Authorization: form.token } : {}),
      },
      body: JSON.stringify(body),
    };
  },
  onStart: (chunk) => {
    recordRawEvent('start', chunk);
  },
  onChunk: (chunk) => {
    recordRawEvent(chunk.event || 'chunk', chunk);
    return chunk;
  },
  onMessage: (chunk) => {
    const text = getChunkText(chunk);
    if (!text) return null;

    return {
      type: 'markdown',
      data: text,
      strategy: 'merge',
    } satisfies AIMessageContent;
  },
  onComplete: (isAborted, _params, result) => {
    appendLog(isAborted ? '请求已停止' : '请求完成');
    recordRawEvent(isAborted ? 'abort' : 'complete', result);
    const content = getChunkText({ data: result });
    if (!content) return undefined;

    return {
      type: 'markdown',
      data: content,
      strategy: 'merge',
    } satisfies AIMessageContent;
  },
  onError: (error) => {
    const message = error instanceof Error ? error.message : String(error);
    errorMessage.value = message;
    appendLog(`请求错误：${message}`);
  },
};

const { chatEngine, messages, status } = useChat({
  defaultMessages: seedMessages,
  chatServiceConfig,
});

const canSend = computed(() => Boolean(form.prompt.trim()) && status.value !== 'pending' && status.value !== 'streaming');
const messageTotal = computed(() => messages.value.length);

const sendMessage = async () => {
  if (!chatEngine.value || !canSend.value) return;
  if (!form.endpoint.trim()) {
    errorMessage.value = '请先填写 endpoint';
    return;
  }
  if (!parseRequestParams()) return;

  syncTokenCookie();
  requestCount.value += 1;
  rawEvents.value = [];
  appendLog(`发送第 ${requestCount.value} 次请求`);
  await chatEngine.value.sendUserMessage({
    prompt: form.prompt.trim(),
  });
};

const stopChat = async () => {
  await chatEngine.value?.abortChat();
};

const clearMessages = () => {
  chatEngine.value?.clearMessages();
  rawEvents.value = [];
  appendLog('已清空消息');
};

const reloadSeedMessages = () => {
  chatEngine.value?.setMessages(seedMessages, 'replace');
  appendLog('已恢复默认消息');
};

const renderContent = (message: ChatMessagesData) => {
  return (message.content || [])
    .map((item) => {
      if (typeof item.data === 'string') return item.data;
      return JSON.stringify(item.data, null, 2);
    })
    .join('\n\n');
};
</script>

<template>
  <main class="console-shell">
    <section class="panel config-panel">
      <div class="section-title">
        <p>ChatEngine Console</p>
        <span :class="['status-pill', `status-${status}`]">{{ status }}</span>
      </div>

      <label>
        <span>Endpoint</span>
        <input v-model="form.endpoint" placeholder="https://api.example.com/chat/completions" />
      </label>

      <label>
        <span>WebSocket Endpoint</span>
        <input v-model="form.wsEndpoint" placeholder="ws://host/api/agent-runtime/agui/ws" />
      </label>

      <div class="field-grid">
        <label>
          <span>WS 重连次数</span>
          <input v-model.number="form.wsMaxRetries" min="0" type="number" />
        </label>

        <label>
          <span>心跳间隔 ms</span>
          <input v-model.number="form.wsHeartbeatInterval" min="0" step="1000" type="number" />
        </label>
      </div>

      <label class="checkbox-field">
        <input v-model="form.wsDebugger" type="checkbox" />
        <span>WS Debugger</span>
      </label>

      <label>
        <span>POST 参数 JSON</span>
        <textarea v-model="form.requestParams" rows="16" spellcheck="false" />
      </label>

      <label>
        <span>Token</span>
        <input v-model="form.token" placeholder="可选，Bearer Token" />
      </label>

      <div class="field-grid">
        <label>
          <span>Transport</span>
          <select v-model="form.transport">
            <option value="sse">SSE</option>
            <option value="fetch">Fetch</option>
            <option value="ws">WebSocket</option>
          </select>
        </label>

        <label>
          <span>Protocol</span>
          <select v-model="form.protocol">
            <option value="default">Default</option>
            <option value="agui">AG-UI</option>
            <option value="agui-http-ws">AG-UI HTTP+WS</option>
            <option value="openclaw">OpenClaw</option>
          </select>
        </label>
      </div>

      <div class="stats">
        <span>消息 {{ messageTotal }}</span>
        <span>请求 {{ requestCount }}</span>
      </div>
    </section>

    <section class="panel chat-panel">
      <div class="toolbar">
        <div>
          <h1>ChatEngine Console</h1>
          <p>直接调试 `@tdesign/ai-chat-engine` 的消息状态、流式内容和请求参数。</p>
        </div>
        <div class="toolbar-actions">
          <button type="button" class="ghost" @click="reloadSeedMessages">恢复默认</button>
          <button type="button" class="ghost" @click="clearMessages">清空</button>
          <button type="button" class="danger" :disabled="status !== 'streaming' && status !== 'pending'" @click="stopChat">
            停止
          </button>
        </div>
      </div>

      <div v-if="errorMessage" class="error-box">{{ errorMessage }}</div>

      <div class="tabs" role="tablist" aria-label="ChatEngine 调试视图">
        <button type="button" :class="{ active: activeTab === 'chat' }" @click="activeTab = 'chat'">
          ChatEngine 测试界面
        </button>
        <button type="button" :class="{ active: activeTab === 'rawMessages' }" @click="activeTab = 'rawMessages'">
          原始消息 JSON
        </button>
        <button type="button" :class="{ active: activeTab === 'rawEvents' }" @click="activeTab = 'rawEvents'">
          原始事件流
        </button>
      </div>

      <template v-if="activeTab === 'chat'">
        <div class="messages">
          <article v-for="message in messages" :key="message.id" :class="['message', message.role]">
            <header>
              <strong>{{ message.role }}</strong>
              <span>{{ message.status || 'idle' }}</span>
            </header>
            <pre>{{ renderContent(message) }}</pre>
          </article>
        </div>

        <form class="composer" @submit.prevent="sendMessage">
          <textarea v-model="form.prompt" rows="4" placeholder="输入一条测试消息" />
          <button type="submit" :disabled="!canSend">发送</button>
        </form>
      </template>

      <div v-else-if="activeTab === 'rawMessages'" class="raw-view">
        <pre>{{ JSON.stringify(messages, null, 2) }}</pre>
      </div>

      <div v-else class="raw-view raw-events">
        <pre v-if="rawEvents.length">{{ rawEvents.join('\n\n') }}</pre>
        <p v-else class="empty">暂无原始事件流</p>
      </div>
    </section>

    <aside class="panel log-panel">
      <div class="section-title">
        <p>事件日志</p>
        <button type="button" class="ghost compact" @click="logs = []">清除</button>
      </div>
      <div class="logs">
        <p v-for="item in logs" :key="item">{{ item }}</p>
        <p v-if="logs.length === 0" class="empty">暂无事件</p>
      </div>
    </aside>
  </main>
</template>
