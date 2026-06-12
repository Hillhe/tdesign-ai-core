import { ref, onMounted, onUnmounted, watch, type Ref } from 'vue';
import ChatEngine, { type ChatMessagesData, type ChatServiceConfig, type ChatStatus } from '@tdesign/ai-chat-engine';

export const useChat = (options: {
  defaultMessages?: ChatMessagesData[];
  chatServiceConfig: ChatServiceConfig;
}) => {
  const messages: Ref<ChatMessagesData[]> = ref([]);
  const status: Ref<ChatStatus> = ref('idle');
  const chatEngineRef = ref<ChatEngine | null>(null);
  const msgSubscribeRef = ref<(() => void) | null>(null);
  const prevInitialMessages = ref<ChatMessagesData[]>([]);

  const syncState = (state: ChatMessagesData[]) => {
    messages.value = state;
    status.value = state[state.length - 1]?.status || 'idle';
  };

  const subscribeToChat = () => {
    if (!chatEngineRef.value) return;

    msgSubscribeRef.value = chatEngineRef.value.messageStore.subscribe((state) => {
      syncState(state.messages);
    });
  };

  const initChat = () => {
    chatEngineRef.value = new ChatEngine();
    chatEngineRef.value.init(options.chatServiceConfig, options.defaultMessages);
    syncState(options.defaultMessages || []);
    subscribeToChat();
  };

  onMounted(() => {
    initChat();
  });

  onUnmounted(() => {
    if (msgSubscribeRef.value) {
      msgSubscribeRef.value();
    }
    chatEngineRef.value?.destroy();
  });

  watch(
    () => options.defaultMessages,
    (newMessages) => {
      const hasChanged = JSON.stringify(prevInitialMessages.value) !== JSON.stringify(newMessages);

      if (hasChanged && newMessages && newMessages.length > 0) {
        prevInitialMessages.value = newMessages;

        if (chatEngineRef.value) {
          chatEngineRef.value.setMessages(newMessages, 'replace');
          syncState(newMessages);
        }
      }
    },
    { deep: true },
  );

  return {
    chatEngine: chatEngineRef,
    messages,
    status,
  };
};
