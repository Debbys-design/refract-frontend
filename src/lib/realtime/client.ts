import { API_BASE_URL } from "@/lib/api/client";
import { getRealtimeCapability } from "@/lib/realtime/capabilities";

type Handler = (payload: unknown) => void;

const MAX_BACKOFF_MS = 30_000;
const BASE_BACKOFF_MS = 1_000;

/**
 * Realtime WebSocket client.
 *
 * Handles connection lifecycle, subscription replay, and auth token expiry
 * for wallet-scoped subscriptions. Token acquisition is isolated behind an
 * injected AuthTokenProvider so the transport stays wallet-agnostic.
 *
 * A small reconnecting WebSocket wrapper exposing a subscribe-by-topic API.
 * The backend contract this assumes (documented, not yet implemented
 * upstream): connect to `${wsUrl}`, receive JSON messages shaped
 * `{ topic: string, payload: unknown }`, one topic per push-based update
 * type (e.g. "oracle_update"). Callers must treat this connection as
 * best-effort — always keep an independent REST fetch as the source of
 * truth for initial state.
 *
 * Transport is negotiated at startup via `getRealtimeCapability()`. Until
 * capability is confirmed the client stays on REST; it only opens a socket
 * when the backend advertises WebSocket support, avoiding reconnect loops
 * against backends that never answer.
 */

import type {
  AuthTokenProvider,
  AuthenticatedSubscription,
  RealtimeAuthOptions,
  RealtimeCloseCode,
  RealtimeConnectionState,
  RealtimeDegradedState,
} from './types';

const AUTH_CLOSE_CODES: ReadonlySet<number> = new Set([4001, 4003, 4401, 4403]);
const DEFAULT_MAX_REFRESH_ATTEMPTS = 2;

export interface RealtimeClientOptions extends RealtimeAuthOptions {
  url: string;
  onStateChange?: (state: RealtimeConnectionState) => void;
  onDegraded?: (state: RealtimeDegradedState) => void;
}
 */

import type {
  AuthTokenProvider,
  AuthenticatedSubscription,
  RealtimeAuthOptions,
  RealtimeCloseCode,
  RealtimeConnectionState,
  RealtimeDegradedState,
} from './types';

const AUTH_CLOSE_CODES: ReadonlySet<number> = new Set([4001, 4003, 4401, 4403]);
const DEFAULT_MAX_REFRESH_ATTEMPTS = 2;

export interface RealtimeClientOptions extends RealtimeAuthOptions {
  url: string;
  onStateChange?: (state: RealtimeConnectionState) => void;
  onDegraded?: (state: RealtimeDegradedState) => void;
}

export class RealtimeClient {
  private readonly url: string;
  private readonly auth?: AuthTokenProvider;
  private readonly maxRefreshAttempts: number;
  private readonly onStateChange?: (state: RealtimeConnectionState) => void;
  private readonly onDegraded?: (state: RealtimeDegradedState) => void;

  private socket: WebSocket | null = null;
  private state: RealtimeConnectionState = 'idle';
  private refreshAttempts = 0;
  private refreshInFlight: Promise<string | null> | null = null;
  private readonly authenticatedSubscriptions = new Map<string, AuthenticatedSubscription>();
  private handlers = new Map<string, Set<Handler>>();
  private attempt = 0;
  private closedByUser = false;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private negotiated = false;

  constructor(options: RealtimeClientOptions) {
    this.url = options.url;
    this.auth = options.auth;
    this.maxRefreshAttempts = options.maxRefreshAttempts ?? DEFAULT_MAX_REFRESH_ATTEMPTS;
    this.onStateChange = options.onStateChange;
    this.onDegraded = options.onDegraded;
  }

  connect(): void {
    if (typeof WebSocket === "undefined" || this.socket) return;
    this.closedByUser = false;
    if (this.negotiated) {
      this.open();
      return;
    }
    void this.negotiate();
  }

  private async negotiate(): Promise<void> {
    const capability = await getRealtimeCapability();
    this.negotiated = true;
    if (this.closedByUser) return;
    if (capability.transport === "websocket") {
      this.open();
    }
    // SSE and REST-only transports are handled elsewhere; this client only
    // manages the WebSocket transport.
  }

  getState(): RealtimeConnectionState {
    return this.state;
  }

  /**
   * Registers a wallet-scoped subscription that will be replayed after a
   * successful re-authentication.
   */
  registerAuthenticatedSubscription(sub: AuthenticatedSubscription): void {
    this.authenticatedSubscriptions.set(sub.topic, sub);
  }

  unregisterAuthenticatedSubscription(topic: string): void {
    this.authenticatedSubscriptions.delete(topic);
  }

  async connect(): Promise<void> {
    this.setState('connecting');
    const token = await this.resolveToken();
    this.openSocket(token);
  }

  disconnect(): void {
    this.socket?.close(1000, 'client disconnect');
    this.socket = null;
    this.setState('closed');
  }

  private async resolveToken(): Promise<string | null> {
    if (!this.auth) return null;
    try {
      return await this.auth.getAuthToken();
    } catch {
      return null;
    }
  }

  private openSocket(token: string | null): void {
    const url = token ? this.withToken(this.url, token) : this.url;
    const socket = new WebSocket(url);
    this.socket = socket;

    socket.onopen = () => {
      this.refreshAttempts = 0;
      this.setState('connected');
      this.replaySubscriptions();
    };

    socket.onmessage = (event) => {
      if (this.isAuthErrorFrame(event.data)) {
        void this.handleAuthFailure();
      }
    };

    socket.onclose = (event) => {
      if (this.isAuthCloseCode(event.code)) {
        void this.handleAuthFailure();
        return;
      }
      this.setState('closed');
    };

    socket.onerror = () => {
      // Errors surface via onclose; nothing to do here.
    };
  }

  private isAuthCloseCode(code: RealtimeCloseCode): boolean {
    return AUTH_CLOSE_CODES.has(code);
  }

  private isAuthErrorFrame(data: unknown): boolean {
    if (typeof data !== 'string') return false;
    try {
      const parsed = JSON.parse(data) as { type?: string; code?: number };
      if (parsed?.type === 'error' && typeof parsed.code === 'number') {
        return AUTH_CLOSE_CODES.has(parsed.code);
      }
      return parsed?.type === 'auth_error';
    } catch {
      return false;
    }
  }

  /**
   * Handles an auth failure by refreshing the token and replaying
   * wallet-scoped subscriptions. Guards against refresh loops and dedupes
   * concurrent refreshes.
   */
  private async handleAuthFailure(): Promise<void> {
    if (!this.auth) {
      this.enterDegraded('auth-required', 'Authentication required for this subscription.');
      return;
    }

    if (this.refreshAttempts >= this.maxRefreshAttempts) {
      this.enterDegraded('auth-refresh-loop', 'Unable to re-authenticate. Showing cached data.');
      return;
    }

    this.refreshAttempts += 1;
    this.setState('reconnecting');

    const token = await this.refreshTokenDeduped();
    if (!token) {
      this.enterDegraded('auth-refresh-failed', 'Session expired. Falling back to REST.');
      return;
    }

    this.socket?.close(1000, 'reauth');
    this.openSocket(token);
  }

  /**
   * Dedupes concurrent refresh requests so multiple failing subscriptions
   * share a single refresh call.
   */
  private refreshTokenDeduped(): Promise<string | null> {
    if (this.refreshInFlight) return this.refreshInFlight;
    if (!this.auth) return Promise.resolve(null);

    this.refreshInFlight = this.auth
      .refreshAuthToken()
      .catch(() => null)
      .finally(() => {
        this.refreshInFlight = null;
      });

    return this.refreshInFlight;
  }

  private replaySubscriptions(): void {
    if (!this.socket || this.socket.readyState !== WebSocket.OPEN) return;
    for (const sub of this.authenticatedSubscriptions.values()) {
      this.socket.send(
        JSON.stringify({ type: 'subscribe', topic: sub.topic, wallet: sub.wallet, params: sub.params }),
      );
    }
  }

  private enterDegraded(
    reason: RealtimeDegradedState['reason'],
    message: string,
  ): void {
    this.setState('degraded');
    this.onDegraded?.({ reason, fallbackToRest: true, message });
  }

  private withToken(url: string, token: string): string {
    try {
      const parsed = new URL(url);
      parsed.searchParams.set('token', token);
      return parsed.toString();
    } catch {
      return url;
    }
  }

  private setState(state: RealtimeConnectionState): void {
    if (this.state === state) return;
    this.state = state;
    this.onStateChange?.(state);
  }
}
