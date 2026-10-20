import { useCallback, useEffect, useRef, useState } from 'react';
import {
  clearPersistedSubscriptions,
  persistSubscriptions,
  readPersistedSubscriptions,
  type PersistedSubscription,
} from '../lib/realtime/persistence';

export interface UseRealtimeTopicOptions {
  topic: string;
  scope: string;
  wallet: string;
  knownTopics: ReadonlySet<string>;
}

export interface UseRealtimeTopicResult {
  cursor: string | null;
  setCursor: (cursor: string | null) => void;
  clear: () => void;
}

/**
 * Primitive hook for a single realtime topic subscription. Persists the last
 * cursor to session storage and rehydrates it on mount so reloads and route
 * navigations resume from the snapshot instead of refetching from scratch.
 */
export function useRealtimeTopic({
  topic,
  scope,
  wallet,
  knownTopics,
}: UseRealtimeTopicOptions): UseRealtimeTopicResult {
  const [cursor, setCursorState] = useState<string | null>(null);
  const hydratedRef = useRef(false);

  useEffect(() => {
    if (hydratedRef.current) return;
    hydratedRef.current = true;
    const persisted = readPersistedSubscriptions(wallet, scope, knownTopics);
    const match = persisted.find((entry) => entry.topic === topic);
    if (match) setCursorState(match.cursor);
  }, [wallet, scope, topic, knownTopics]);

  const persist = useCallback(
    (nextCursor: string | null) => {
      const existing = readPersistedSubscriptions(wallet, scope, knownTopics);
      const others = existing.filter((entry) => entry.topic !== topic);
      const next: PersistedSubscription[] = [
        ...others,
        { topic, scope, cursor: nextCursor },
      ];
      persistSubscriptions(wallet, scope, next);
    },
    [wallet, scope, topic, knownTopics],
  );

  const setCursor = useCallback(
    (nextCursor: string | null) => {
      setCursorState(nextCursor);
      persist(nextCursor);
    },
    [persist],
  );

  const clear = useCallback(() => {
    setCursorState(null);
    clearPersistedSubscriptions(wallet, scope);
  }, [wallet, scope]);

  return { cursor, setCursor, clear };
}
