import { create } from 'zustand';
import { persist, createJSONStorage } from 'zustand/middleware';
import type { TopicScope } from './types';
import { topicContract, parseTopicPayload } from './contract';

const STORAGE_KEY = 'handsoff.realtime.subscriptions';
const SCHEMA_VERSION = 1;
const MAX_CURSOR_LENGTH = 512;

const MAX_CURSOR = MAX_CURSOR_LENGTH;

export interface ActiveSubscription {
  topic: string;
  scope: TopicScope;
  cursor: string | null;
}

interface PersistedState {
  version: number;
  subscriptions: Record<string, ActiveSubscription>;
}

function subscriptionKey(topic: string, scope: TopicScope): string {
  return `${scope}:${topic}`;
}

function boundCursor(cursor: string | null): string | null {
  if (cursor == null) return null;
  if (cursor.length <= MAX_CURSOR) return cursor;
  return cursor.slice(0, MAX_CURSOR);
}

function isKnownTopic(topic: string, scope: TopicScope): boolean {
  const entry = topicContract[topic];
  if (!entry) return false;
  return entry.scopes.includes(scope);
}

function reconcile(
  subscriptions: Record<string, ActiveSubscription>,
): Record<string, ActiveSubscription> {
  const next: Record<string, ActiveSubscription> = {};
  for (const [key, sub] of Object.entries(subscriptions)) {
    if (!sub || typeof sub.topic !== 'string' || typeof sub.scope !== 'string') {
      continue;
    }
    if (!isKnownTopic(sub.topic, sub.scope)) continue;
    const cursor = boundCursor(sub.cursor ?? null);
    if (cursor != null && !parseTopicPayload(sub.topic, cursor)) continue;
    next[key] = { topic: sub.topic, scope: sub.scope, cursor };
  }
  return next;
}

interface RealtimeRegistryState {
  subscriptions: Record<string, ActiveSubscription>;
  setSubscription: (sub: ActiveSubscription) => void;
  removeSubscription: (topic: string, scope: TopicScope) => void;
  clearSubscriptions: () => void;
}

export const useRealtimeRegistry = create<RealtimeRegistryState>()(
  persist(
    (set) => ({
      subscriptions: {},
      setSubscription: (sub) =>
        set((state) => ({
          subscriptions: {
            ...state.subscriptions,
            [subscriptionKey(sub.topic, sub.scope)]: {
              topic: sub.topic,
              scope: sub.scope,
              cursor: boundCursor(sub.cursor),
            },
          },
        })),
      removeSubscription: (topic, scope) =>
        set((state) => {
          const next = { ...state.subscriptions };
          delete next[subscriptionKey(topic, scope)];
          return { subscriptions: next };
        }),
      clearSubscriptions: () => set({ subscriptions: {} }),
    }),
    {
      name: STORAGE_KEY,
      version: SCHEMA_VERSION,
      storage: createJSONStorage(() => sessionStorage),
      partialize: (state) => ({
        version: SCHEMA_VERSION,
        subscriptions: state.subscriptions,
      }),
      merge: (persisted, current) => {
        const data = persisted as Partial<PersistedState> | undefined;
        if (!data || data.version !== SCHEMA_VERSION) {
          return current;
        }
        const subscriptions = reconcile(data.subscriptions ?? {});
        return { ...current, subscriptions };
      },
    },
  ),
);
