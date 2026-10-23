import { ShadowMode, type ShadowTopic } from './shadow';

/**
 * Unified realtime merge layer.
 *
 * Realtime frames are validated upstream and passed here. The merge layer
 * always prefers polled REST values as the source of truth; realtime values
 * are only used when no REST value is available yet. When shadow mode is
 * enabled, realtime frames are additionally compared against the polled REST
 * values for diagnostics, without affecting the merged output.
 */

export interface MergeState<T> {
  polled: T | null;
  realtime: T | null;
}

export interface MergeOptions {
  /** Shadow mode recorder; disabled by default. */
  shadow?: ShadowMode | null;
  /** Topic used for shadow comparisons. */
  topic?: ShadowTopic;
  /** Stable key for discrepancy reports. */
  key?: string;
}

/**
 * Merge polled REST and realtime values, preferring REST. Shadow comparisons
 * are recorded as a side effect but never influence the returned value.
 */
export function mergeValues<T extends Record<string, unknown>>(
  state: MergeState<T>,
  options: MergeOptions = {},
): T | null {
  const { shadow, topic, key } = options;

  if (shadow && topic && state.realtime) {
    shadow.record(
      topic,
      key ?? topic,
      state.realtime,
      state.polled ?? null,
    );
  }

  // REST is the known-good source of truth; realtime is only a fallback.
  return state.polled ?? state.realtime ?? null;
}
