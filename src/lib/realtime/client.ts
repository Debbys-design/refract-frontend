import { API_BASE_URL } from "@/lib/api/client";

type Handler = (payload: unknown) => void;

/**
 * Backoff policy for the reconnecting WebSocket. Kept as a plain config
 * object so the pure `computeBackoff` helper can be unit-tested without a
 * socket, and so callers/tests can override the clock and jitter source.
 */
export interface BackoffConfig {
  /** Delay for the first retry, in milliseconds. */
  baseMs: number;
  /** Upper bound on any single delay, in milliseconds. */
  maxMs: number;
  /** How long a connection must stay open before the budget resets. */
  stableThresholdMs: number;
  /** Injectable RNG in [0, 1) for deterministic tests. */
  random: () => number;
}

export const DEFAULT_BACKOFF_CONFIG: BackoffConfig = {
  baseMs: 500,
  maxMs: 30_000,
  stableThresholdMs: 10_000,
  random: Math.random,
};

/**
 * Pure, lifecycle-independent backoff calculation. Uses "decorrelated"
 * jitter: the delay is drawn uniformly from [base, min(max, prev * 3)] so
 * clients that failed together do not retry in lockstep. The result is
 * always a positive, capped integer.
 */
export function computeBackoff(
  attempt: number,
  config: BackoffConfig = DEFAULT_BACKOFF_CONFIG,
): number {
  const { baseMs, maxMs, random } = config;
  const safeAttempt = Math.max(0, Math.floor(attempt));
  // Exponential ceiling for this attempt, capped at maxMs.
  const ceiling = Math.min(maxMs, baseMs * 2 ** safeAttempt);
  const floor = Math.min(baseMs, ceiling);
  const span = ceiling - floor;
  const jittered = floor + span * random();
  // Guard against zero/negative delays from a misbehaving RNG.
  return Math.max(1, Math.min(maxMs, Math.round(jittered)));
}

/**
 * A small reconnecting WebSocket wrapper exposing a subscribe-by-topic API.
 * The backend contract this assumes (documented, not yet implemented
 * upstream): connect to `${wsUrl}`, receive JSON messages shaped
 * `{ topic: string, payload: unknown }`, one topic per push-based update
 * type (e.g. "oracle_update"). Callers must treat this connection as
 * best-effort — always keep an independent REST fetch as the source of
 * truth for initial state.
 *
 * Reconnects use capped exponential backoff with jitter (see
 * `computeBackoff`). The backoff budget only resets once a connection has
 * been stable for `stableThresholdMs`, so rapid open/close cycles keep
 * escalating instead of producing a reconnect storm.
 */
class RealtimeClient {
  private socket: WebSocket | null = null;
  private handlers = new Map<string, Set<Handler>>();
  private attempt = 0;
  private closedByUser = false;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private stabilityTimer: ReturnType<typeof setTimeout> | null = null;
  private config: BackoffConfig = DEFAULT_BACKOFF_CONFIG;

  connect(): void {
    if (typeof WebSocket === "undefined" || this.socket) return;
    this.closedByUser = false;
    this.open();
  }

  private open(): void {
    const wsUrl = API_BASE_URL.replace(/^http/, "ws").replace(/\/api\/v1$/, "/ws");
    try {
      this.socket = new WebSocket(wsUrl);
    } catch {
      this.scheduleReconnect();
      return;
    }

    this.socket.addEventListener("open", () => {
      // Do NOT reset the budget here — only after the connection proves
      // stable. Otherwise a flapping backend resets backoff every cycle.
      this.clearStabilityTimer();
      this.stabilityTimer = setTimeout(() => {
        this.stabilityTimer = null;
        this.attempt = 0;
      }, this.config.stableThresholdMs);
    });
    this.socket.addEventListener("message", (event) => {
      try {
        const { topic, payload } = JSON.parse(event.data as string) as { topic: string; payload: unknown };
        this.handlers.get(topic)?.forEach((handler) => handler(payload));
      } catch {
        // Malformed message — ignore, don't crash the connection.
      }
    });
    this.socket.addEventListener("close", (event) => {
      this.socket = null;
      this.clearStabilityTimer();
      if (!this.closedByUser) this.scheduleReconnect(event);
    });
    this.socket.addEventListener("error", () => {
      this.socket?.close();
    });
  }

  private scheduleReconnect(event?: CloseEvent): void {
    if (this.reconnectTimer) return;
    const delay = this.resolveDelay(event);
    this.attempt += 1;
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      if (!this.closedByUser) this.open();
    }, delay);
  }

  /**
   * Compute the next delay, honouring a server-provided retry hint when
   * present. The hint may arrive as a `retry-after` header on the close
   * event (seconds or ms) or be encoded in the close reason.
   */
  private resolveDelay(event?: CloseEvent): number {
    const hint = this.parseRetryAfter(event);
    if (hint !== null) {
      return Math.max(1, Math.min(this.config.maxMs, hint));
    }
    return computeBackoff(this.attempt, this.config);
  }

  private parseRetryAfter(event?: CloseEvent): number | null {
    if (!event) return null;
    const header = event.reason?.match(/retry-after[:=]\s*(\d+(?:\.\d+)?)/i);
    if (header) {
      const value = Number(header[1]);
      if (Number.isFinite(value) && value > 0) {
        // Values under 1000 are treated as seconds, larger as milliseconds.
        return value < 1000 ? value * 1000 : value;
      }
    }
    return null;
  }

  private clearStabilityTimer(): void {
    if (this.stabilityTimer) {
      clearTimeout(this.stabilityTimer);
      this.stabilityTimer = null;
    }
  }

  subscribe(topic: string, handler: Handler): () => void {
    if (!this.handlers.has(topic)) this.handlers.set(topic, new Set());
    this.handlers.get(topic)!.add(handler);
    this.connect();

    return () => {
      this.handlers.get(topic)?.delete(handler);
    };
  }

  disconnect(): void {
    this.closedByUser = true;
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    this.clearStabilityTimer();
    this.socket?.close();
    this.socket = null;
  }
}

export const realtimeClient = new RealtimeClient();
