import type { Context } from "@deepseek-ai/cordis";
import type {
  Agent,
  AgentHandle,
  ModelSelection,
  ModelSelectionRef,
} from "@deepseek-ai/dsh-agent";
import { installModelSelection } from "@deepseek-ai/dsh-agent";
import type {} from "@deepseek-ai/dsh-agent-default-model";
import type {} from "@deepseek-ai/dsh-agent-presets";
import type {} from "@deepseek-ai/dsh-agent/types";
import type { ConnectionRpcHandler } from "@deepseek-ai/dsh-client-connection";
import {
  RpcId,
  type ConnectionRpcResult,
} from "@deepseek-ai/dsh-client-connection";
import type {} from "@deepseek-ai/dsh-session-persistence";
import type { MessageSource } from "@deepseek-ai/dsh-llm";
import type { SubagentListEntry } from "@deepseek-ai/dsh-subagent";
import { queueHostSubagentPrompt } from "@deepseek-ai/dsh-subagent/internal";
import {
  foldRequestHeader,
  SessionId,
  type SessionEvent,
  type SessionHeader,
  type UserMessage,
} from "@deepseek-ai/dsh-session";
import { randomUUID } from "node:crypto";

export const name = "continue";
export const inject = [
  "agents",
  "agentDefaultModel",
  "agentPresets",
  "connection",
  "sessionPersistence",
  "subagents",
];
export const RPC_CHANNEL = "/dsh-continue";

const ENDPOINT_STATUS = "status";
const ENDPOINT_RESUME = "resume";
const MAX_SESSION_ID_LENGTH = 512;
const CONTINUE_WAKE_PREFIX = "dsh-continue:";
const SUBAGENT_WAKE_PREFIX = "dsh-continue:subagent:";
const PATCH_STATE = Symbol.for("dsh-continue.agent-wake-patch.v2");

type ContinueReason = "request-error" | "interrupted" | "max-tokens" | "disposed" | "none";

type InternalDecision = {
  readonly kind: "reject";
} | {
  readonly kind: "enter";
  readonly messages: UserMessage[];
  readonly [key: string]: unknown;
};

type InternalAgentPhase = {
  readonly kind: string;
};

type InternalAgent = Agent & {
  phase?: InternalAgentPhase;
  preStep?: (
    target: string,
    position: { readonly turn: number; readonly step: number },
  ) => Promise<InternalDecision>;
  wakeDriver?: (wakeAfterAbort?: boolean) => void;
};

type InternalPreStep = NonNullable<InternalAgent["preStep"]>;

type MarkerOutcome = "consumed" | "cancelled";

type MarkerLease = {
  readonly outcome: Promise<MarkerOutcome>;
  cancel(): void;
};

type MarkerValidator = (deferredTurns: number) => Promise<boolean>;

type MarkerTracker = {
  track(rpcId: string, sessionId: string, validate?: MarkerValidator): MarkerLease;
  defer(rpcIds: readonly string[]): void;
  validate(rpcIds: readonly string[]): Promise<boolean>;
  consume(rpcIds: readonly string[]): void;
  cancel(rpcIds: readonly string[]): void;
  finish(rpcIds: readonly string[]): void;
  cancelSession(sessionId: string): void;
  cancelAll(): void;
};

type ContinueRuntimeState = {
  readonly markers: MarkerTracker;
  readonly activeRequests: Set<Promise<void>>;
  readonly shutdown: AbortController;
  shuttingDown: boolean;
};

type AgentPatchState = {
  readonly originalPreStep: InternalPreStep;
  readonly wrappedPreStep: InternalPreStep;
  readonly previousPreStep: PropertyDescriptor | undefined;
  pending: boolean;
  active: boolean;
  disposeRequested: boolean;
  idlePromise: Promise<void> | undefined;
  readonly deferredMarkers: UserMessage[];
  deferredWakePromise: Promise<void> | undefined;
};

interface ResumeRequest {
  readonly sessionId: string;
  readonly turn: number;
  readonly boundarySeq: number;
}

export interface ContinueStatus {
  readonly available: boolean;
  readonly reason: ContinueReason;
  readonly turn?: number;
  readonly boundarySeq?: number;
}

type ContinueTarget = "session" | "subagent";

type ResolvedContinueStatus = ContinueStatus & {
  readonly target: ContinueTarget;
  readonly parentSessionId?: ReturnType<typeof SessionId>;
};

type ChildEntry = Extract<SubagentListEntry, { kind: "child" }>;
type ContinuableChildEntry = Extract<ChildEntry, { mode: "continuable" }>;

type SessionState = {
  readonly header: SessionHeader;
  readonly events: readonly SessionEvent[];
  readonly inheritedEventCount: number;
  readonly subagent: boolean;
  readonly agent?: Agent;
  readonly parentSessionId?: ReturnType<typeof SessionId>;
};

type LiveAgentResolution = {
  readonly agent: Agent;
  readonly accept?: () => void;
  readonly dispose?: () => Promise<void>;
};

const RUNTIME_STATES = new WeakMap<Context, ContinueRuntimeState>();

function createMarkerTracker(): MarkerTracker {
  const records = new Map<string, {
    readonly sessionId: string;
    readonly validate: MarkerValidator | undefined;
    deferredTurns: number;
    outcome: MarkerOutcome | undefined;
    readonly promise: Promise<MarkerOutcome>;
    readonly settle: (outcome: MarkerOutcome) => void;
  }>();

  const settle = (rpcId: string, outcome: MarkerOutcome) => {
    const record = records.get(rpcId);
    if (record === undefined || record.outcome !== undefined) return;
    record.outcome = outcome;
    record.settle(outcome);
  };

  return {
    track(rpcId, sessionId, validate) {
      if (records.has(rpcId)) throw new Error(`duplicate continuation marker: ${rpcId}`);
      let settlePromise!: (outcome: MarkerOutcome) => void;
      const promise = new Promise<MarkerOutcome>((resolve) => {
        settlePromise = resolve;
      });
      records.set(rpcId, {
        sessionId,
        validate,
        deferredTurns: 0,
        outcome: undefined,
        promise,
        settle: settlePromise,
      });
      return {
        outcome: promise,
        cancel: () => settle(rpcId, "cancelled"),
      };
    },
    defer(rpcIds) {
      for (const rpcId of new Set(rpcIds)) {
        const record = records.get(rpcId);
        if (record !== undefined && record.outcome === undefined) record.deferredTurns += 1;
      }
    },
    async validate(rpcIds) {
      for (const rpcId of new Set(rpcIds)) {
        const record = records.get(rpcId);
        if (record === undefined) continue;
        if (record.outcome === "cancelled") return false;
        if (record.validate !== undefined
          && !await record.validate(record.deferredTurns)) return false;
        if (records.get(rpcId)?.outcome === "cancelled") return false;
      }
      return true;
    },
    consume(rpcIds) {
      for (const rpcId of rpcIds) settle(rpcId, "consumed");
    },
    cancel(rpcIds) {
      for (const rpcId of rpcIds) settle(rpcId, "cancelled");
    },
    finish(rpcIds) {
      for (const rpcId of rpcIds) records.delete(rpcId);
    },
    cancelSession(sessionId) {
      for (const [rpcId, record] of records) {
        if (record.sessionId === sessionId) settle(rpcId, "cancelled");
      }
    },
    cancelAll() {
      for (const rpcId of records.keys()) settle(rpcId, "cancelled");
    },
  };
}

function createRuntimeState(): ContinueRuntimeState {
  return {
    markers: createMarkerTracker(),
    activeRequests: new Set(),
    shutdown: new AbortController(),
    shuttingDown: false,
  };
}

function beginShutdown(runtime: ContinueRuntimeState): void {
  runtime.shuttingDown = true;
  if (!runtime.shutdown.signal.aborted) {
    runtime.shutdown.abort(new Error("dsh-continue is shutting down"));
  }
  runtime.markers.cancelAll();
}

function awaitWithSignal<T>(operation: PromiseLike<T>, signal: AbortSignal): Promise<T> {
  if (signal.aborted) return Promise.reject(signal.reason);
  return new Promise<T>((resolve, reject) => {
    const onAbort = () => {
      signal.removeEventListener("abort", onAbort);
      reject(signal.reason);
    };
    signal.addEventListener("abort", onAbort, { once: true });
    void Promise.resolve(operation).then(
      (value) => {
        signal.removeEventListener("abort", onAbort);
        resolve(value);
      },
      (error: unknown) => {
        signal.removeEventListener("abort", onAbort);
        reject(error);
      },
    );
  });
}

/**
 * AgentLoop only checks `messages.length` before iterating the collection. The
 * continuation sentinel therefore opens a real model step while its iterator
 * contributes no message to the Session surface or durable event log.
 */
const EMPTY_CONTINUATION_MESSAGES = Object.freeze({
  length: 1,
  [Symbol.iterator]: function* (): IterableIterator<never> {
    // Deliberately empty: this is a control sentinel, not a user message.
  },
}) as unknown as UserMessage[];

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function exactKeys(value: Record<string, unknown>, keys: readonly string[]): boolean {
  const expected = new Set(keys);
  return Object.keys(value).every((key) => expected.delete(key)) && expected.size === 0;
}

function badRequest(message: string): ConnectionRpcResult<unknown> {
  return {
    ok: false,
    error: {
      code: "bad-request",
      message,
      details: { issues: [] },
    },
  };
}

function busy(message: string): ConnectionRpcResult<unknown> {
  return {
    ok: false,
    error: {
      code: "agent-busy",
      message,
      details: { reason: "dsh-continue" },
    },
  };
}

function internal(message: string): ConnectionRpcResult<unknown> {
  return {
    ok: false,
    error: {
      code: "internal",
      message,
      details: {},
    },
  };
}

function cancelled(): ConnectionRpcResult<unknown> {
  return {
    ok: false,
    error: {
      code: "cancelled",
      message: "续跑请求已取消。",
      details: {},
    },
  };
}

function validSessionId(value: unknown): value is string {
  return typeof value === "string"
    && value.length > 0
    && value.length <= MAX_SESSION_ID_LENGTH
    && value.trim() === value;
}

function validBoundaryNumber(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0;
}

function isSubagentContinuationWakeMessage(value: unknown): value is UserMessage {
  if (!isRecord(value) || value.role !== "user") return false;
  if (!Array.isArray(value.content) || value.content.length !== 0) return false;
  const source = value.source;
  return isRecord(source)
    && source.kind === "user"
    && typeof source.rpcId === "string"
    && source.rpcId.startsWith(SUBAGENT_WAKE_PREFIX);
}

function isLegacyContinuationWakeMessage(value: unknown): value is UserMessage {
  if (!isRecord(value) || value.role !== "user") return false;
  if (!Array.isArray(value.content) || value.content.length !== 0) return false;
  const source = value.source;
  return isRecord(source)
    && source.kind === "user"
    && typeof source.rpcId === "string"
    && source.rpcId.startsWith(CONTINUE_WAKE_PREFIX)
    && !source.rpcId.startsWith(SUBAGENT_WAKE_PREFIX);
}

function isContinuationWakeMessage(value: unknown): value is UserMessage {
  return isLegacyContinuationWakeMessage(value) || isSubagentContinuationWakeMessage(value);
}

function wakeRpcId(value: unknown): string | undefined {
  if (!isContinuationWakeMessage(value)) return undefined;
  const source: unknown = value.source;
  return isRecord(source) && typeof source.rpcId === "string" ? source.rpcId : undefined;
}

function wakeMessageId(value: unknown): string | undefined {
  return isRecord(value) && typeof value.id === "string" ? value.id : undefined;
}

function wakeMessageIds(values: readonly unknown[]): Set<string> {
  return new Set(
    values
      .map(wakeMessageId)
      .filter((value): value is string => value !== undefined),
  );
}

function wakeRpcIds(values: readonly unknown[]): string[] {
  return values
    .map(wakeRpcId)
    .filter((value): value is string => value !== undefined);
}

function listWakeMessages(
  agent: Agent,
  predicate: (value: unknown) => value is UserMessage,
): UserMessage[] {
  const inbox = agent.inbox as unknown as {
    readonly nextStep?: readonly UserMessage[];
    readonly nextTurn?: readonly UserMessage[];
  };
  if (!Array.isArray(inbox.nextStep) || !Array.isArray(inbox.nextTurn)) return [];
  return [...inbox.nextStep, ...inbox.nextTurn].filter(predicate);
}

function removeWakeMessages(
  agent: Agent,
  predicate: (value: unknown) => value is UserMessage,
): UserMessage[] {
  const inbox = agent.inbox as unknown as {
    readonly nextStep?: readonly UserMessage[];
    readonly nextTurn?: readonly UserMessage[];
    remove?: (id: string) => boolean;
  };
  if (typeof inbox.remove !== "function") return [];
  const removed: UserMessage[] = [];
  for (const message of listWakeMessages(agent, predicate)) {
    if (!predicate(message)) continue;
    const id = (message as unknown as { id?: unknown }).id;
    if (typeof id === "string" && inbox.remove(id)) removed.push(message);
  }
  return removed;
}

function removeWakeByMessageIds(agent: Agent, ids: ReadonlySet<string>): void {
  const inbox = agent.inbox as unknown as { remove?: (id: string) => boolean };
  if (typeof inbox.remove !== "function") return;
  for (const id of ids) inbox.remove(id);
}

function storeDeferredMarkers(
  state: AgentPatchState,
  messages: readonly UserMessage[],
  markers: MarkerTracker,
): void {
  const existingIds = wakeMessageIds(state.deferredMarkers);
  for (const message of messages) {
    const id = wakeMessageId(message);
    if (id !== undefined && existingIds.has(id)) continue;
    state.deferredMarkers.push(message);
    if (id !== undefined) existingIds.add(id);
  }
  markers.defer(wakeRpcIds(messages));
}

function cancelDeferredMarkers(state: AgentPatchState, markers: MarkerTracker): void {
  const rpcIds = wakeRpcIds(state.deferredMarkers);
  state.deferredMarkers.length = 0;
  markers.cancel(rpcIds);
  markers.finish(rpcIds);
}

function scheduleDeferredWake(
  agent: Agent,
  state: AgentPatchState,
  markers: MarkerTracker,
): void {
  if (state.deferredMarkers.length === 0 || state.deferredWakePromise !== undefined) return;
  const settled = agent.whenIdle().then(
    () => {
      state.deferredWakePromise = undefined;
      if (state.disposeRequested || patchStateOf(agent) !== state) return;
      const internal = agent as unknown as InternalAgent;
      if (state.deferredMarkers.length === 0
        || agent.status !== "idle"
        || internal.phase?.kind !== "idle"
        || typeof internal.wakeDriver !== "function") {
        cancelDeferredMarkers(state, markers);
        return;
      }
      try {
        state.pending = true;
        internal.wakeDriver();
      } catch {
        state.pending = false;
        cancelDeferredMarkers(state, markers);
      }
    },
    () => {
      state.deferredWakePromise = undefined;
      cancelDeferredMarkers(state, markers);
    },
  );
  state.deferredWakePromise = settled;
  void settled.catch(() => undefined);
}

function removeWakeByRpcId(agent: Agent, rpcId: string): number {
  return removeWakeMessages(
    agent,
    (value): value is UserMessage => wakeRpcId(value) === rpcId,
  ).length;
}

function clearLegacyWakeMessages(agent: Agent): UserMessage[] {
  return removeWakeMessages(agent, isLegacyContinuationWakeMessage);
}

function clearSubagentWakeMessages(agent: Agent): UserMessage[] {
  return removeWakeMessages(agent, isSubagentContinuationWakeMessage);
}

export function clearLegacyContinuationWakes(agent: Agent): number {
  return removeWakeMessages(agent, isContinuationWakeMessage).length;
}

function cleanupLegacyWakesWhenIdle(agent: Agent): void {
  const internal = agent as unknown as InternalAgent;
  if (agent.status === "idle" && internal.phase?.kind === "idle") {
    clearLegacyWakeMessages(agent);
  }
}

function decodeStatusPayload(payload: unknown): string | undefined {
  if (!isRecord(payload) || !exactKeys(payload, ["sessionId"])) return undefined;
  return validSessionId(payload.sessionId) ? payload.sessionId : undefined;
}

function decodeResumePayload(payload: unknown): ResumeRequest | undefined {
  if (!isRecord(payload) || !exactKeys(payload, ["sessionId", "turn", "boundarySeq"])) {
    return undefined;
  }
  if (!validSessionId(payload.sessionId)
    || !validBoundaryNumber(payload.turn)
    || !validBoundaryNumber(payload.boundarySeq)) {
    return undefined;
  }
  return {
    sessionId: payload.sessionId,
    turn: payload.turn,
    boundarySeq: payload.boundarySeq,
  };
}

function rpcId(prefix = CONTINUE_WAKE_PREFIX): ReturnType<typeof RpcId> {
  return RpcId(`${prefix}${randomUUID()}`);
}

function reasonFromTurnEnd(event: SessionEvent): ContinueReason {
  if (event.type !== "turn/end") return "none";
  const reason = event.data.reason;
  if (reason.kind === "error") return "request-error";
  if (reason.kind === "interrupted") return "interrupted";
  if (reason.kind === "max-tokens") return "max-tokens";
  if (reason.kind === "aborted" && reason.reason.kind === "disposed") return "disposed";
  return "none";
}

function latestTurnBoundaryEvent(
  events: readonly SessionEvent[],
  inheritedEventCount = 0,
): SessionEvent | undefined {
  const start = Math.max(0, Math.min(inheritedEventCount, events.length));
  for (let index = events.length - 1; index >= start; index -= 1) {
    const event = events[index];
    if (event?.type === "turn/end" || event?.type === "turn/start") return event;
  }
  return undefined;
}

function statusFromBoundaryEvent(event: SessionEvent): ContinueStatus | undefined {
  if (event.type === "turn/end") {
    const reason = reasonFromTurnEnd(event);
    return {
      available: reason !== "none",
      reason,
      turn: event.data.turn,
      boundarySeq: event.seq,
    };
  }
  if (event.type === "turn/start") {
    return {
      available: true,
      reason: "interrupted",
      turn: event.data.turn,
      boundarySeq: event.seq,
    };
  }
  return undefined;
}

function latestTurnBoundary(
  events: readonly SessionEvent[],
  inheritedEventCount = 0,
): ContinueStatus | undefined {
  const event = latestTurnBoundaryEvent(events, inheritedEventCount);
  return event === undefined ? undefined : statusFromBoundaryEvent(event);
}

function boundaryTime(
  events: readonly SessionEvent[],
  inheritedEventCount: number,
): number {
  return latestTurnBoundaryEvent(events, inheritedEventCount)?.time ?? -1;
}

function replayPendingMessages(
  events: readonly SessionEvent[],
  inheritedEventCount: number,
): UserMessage[] {
  const nextTurn: UserMessage[] = [];
  const nextStep: UserMessage[] = [];
  const start = Math.max(0, Math.min(inheritedEventCount, events.length));
  for (const event of events.slice(start)) {
    if (event.type !== "agent/inbox/spliced") continue;
    const list = event.data.target === "next-turn" ? nextTurn : nextStep;
    const position = Math.max(0, Math.min(list.length, event.data.start));
    list.splice(position, event.data.removedCount ?? 0, ...event.data.inserted);
  }
  return [...nextStep, ...nextTurn].filter(
    (message) => !isContinuationWakeMessage(message),
  );
}

function hasPendingMessages(
  events: readonly SessionEvent[],
  agent: Agent | undefined,
  inheritedEventCount = 0,
): boolean {
  if (agent !== undefined) return agent.inbox.hasPending;
  return replayPendingMessages(events, inheritedEventCount).length > 0;
}

/** Classify the latest durable turn and reject sessions with unrelated pending input. */
export function continueStatusFromEvents(
  events: readonly SessionEvent[],
  agent?: Agent,
  inheritedEventCount = 0,
): ContinueStatus {
  const boundary = latestTurnBoundary(events, inheritedEventCount);
  if (boundary === undefined) return { available: false, reason: "none" };
  if (!boundary.available) return boundary;
  const internal = agent as InternalAgent | undefined;
  if (agent?.status === "running"
    || (agent !== undefined && internal?.phase?.kind !== "idle")
    || hasPendingMessages(events, agent, inheritedEventCount)) {
    return { ...boundary, available: false };
  }
  return boundary;
}

function patchStateOf(agent: Agent): AgentPatchState | undefined {
  const value = (agent as unknown as Record<PropertyKey, unknown>)[PATCH_STATE];
  return isRecord(value)
    && typeof value.originalPreStep === "function"
    && typeof value.wrappedPreStep === "function"
    && (value.previousPreStep === undefined || isRecord(value.previousPreStep))
    && typeof value.pending === "boolean"
    && typeof value.active === "boolean"
    && typeof value.disposeRequested === "boolean"
    && Array.isArray(value.deferredMarkers)
    && (value.deferredWakePromise === undefined || value.deferredWakePromise instanceof Promise)
    ? value as unknown as AgentPatchState
    : undefined;
}

function restoreAgentPatch(agent: Agent, state: AgentPatchState): void {
  const internal = agent as unknown as InternalAgent;
  const object = agent as unknown as Record<PropertyKey, unknown>;
  if (internal.preStep === state.wrappedPreStep) {
    if (state.previousPreStep === undefined) {
      Reflect.deleteProperty(object, "preStep");
    } else {
      Object.defineProperty(agent, "preStep", state.previousPreStep);
    }
  }
  if (object[PATCH_STATE] === state) Reflect.deleteProperty(object, PATCH_STATE);
}

function installAgentPatch(
  agent: Agent,
  ctx: Context,
  markers: MarkerTracker,
): AgentPatchState | undefined {
  const existing = patchStateOf(agent);
  if (existing !== undefined) return existing;
  const internal = agent as unknown as InternalAgent;
  if (typeof internal.preStep !== "function" || typeof internal.wakeDriver !== "function") {
    try {
      ctx.logger.warn("dsh-continue: this DSH AgentLoop has no compatible empty-wake boundary");
    } catch {
      // Diagnostics must not affect the Agent lifecycle.
    }
    return undefined;
  }
  if (typeof agent.whenIdle !== "function") {
    try {
      ctx.logger.warn("dsh-continue: this DSH Agent has no quiescence API");
    } catch {
      // Diagnostics must not affect the Agent lifecycle.
    }
    return undefined;
  }

  const originalPreStep = internal.preStep;
  const previousPreStep = Object.getOwnPropertyDescriptor(agent, "preStep");
  if (previousPreStep !== undefined && previousPreStep.configurable !== true && previousPreStep.writable !== true) {
    try {
      ctx.logger.warn("dsh-continue: Agent.preStep is not patchable");
    } catch {
      // Diagnostics must not affect the Agent lifecycle.
    }
    return undefined;
  }
  let state: AgentPatchState;
  const wrappedPreStep: InternalPreStep = async function (
    this: InternalAgent,
    target,
    position,
  ): Promise<InternalDecision> {
    const continuationStep = state.pending && target === "next-turn" && position.step === 1;
    const continuationBoundary = target === "next-turn" && position.step === 1;
    const liveAgent = this as unknown as Agent;
    const inboxMessages = continuationBoundary
      ? [
        ...((Array.isArray(liveAgent.inbox.nextStep) ? liveAgent.inbox.nextStep : [])),
        ...((Array.isArray(liveAgent.inbox.nextTurn) ? liveAgent.inbox.nextTurn : [])),
      ]
      : [];
    const inboxMarkers = inboxMessages.filter(isContinuationWakeMessage);
    const ordinaryPending = inboxMessages.some((message) => !isContinuationWakeMessage(message));
    if (continuationBoundary && ordinaryPending && state.deferredMarkers.length > 0) {
      cancelDeferredMarkers(state, markers);
    }
    const deferredMarkers = continuationBoundary && !ordinaryPending
      ? state.deferredMarkers.splice(0)
      : [];
    const admittedIdsSeen = new Set<string>();
    const admittedMarkers = [...inboxMarkers, ...deferredMarkers].filter((message) => {
      const id = wakeMessageId(message);
      if (id === undefined || !admittedIdsSeen.has(id)) {
        if (id !== undefined) admittedIdsSeen.add(id);
        return true;
      }
      return false;
    });
    const admittedIds = wakeMessageIds(admittedMarkers);
    if (continuationStep) {
      state.pending = false;
    }
    const admittedRpcIds = wakeRpcIds(admittedMarkers);
    let decision: InternalDecision;
    try {
      decision = await originalPreStep.call(this, target, position);
    } catch (error) {
      removeWakeByMessageIds(liveAgent, admittedIds);
      markers.cancel(admittedRpcIds);
      markers.finish(admittedRpcIds);
      throw error;
    }
    if (decision.kind !== "enter") {
      removeWakeByMessageIds(liveAgent, admittedIds);
      markers.cancel(admittedRpcIds);
      markers.finish(admittedRpcIds);
      return decision;
    }

    const decisionMarkers = continuationBoundary
      ? decision.messages.filter((message) => {
        const id = wakeMessageId(message);
        return id !== undefined && admittedIds.has(id);
      })
      : [];
    const lateMarkers = continuationBoundary
      ? decision.messages.filter((message) => {
        const id = wakeMessageId(message);
        return isContinuationWakeMessage(message)
          && (id === undefined || !admittedIds.has(id));
      })
      : [];
    const ownsBoundary = continuationStep
      || admittedMarkers.length > 0
      || decisionMarkers.length > 0
      || lateMarkers.length > 0;
    if (state.disposeRequested && ownsBoundary) {
      removeWakeByMessageIds(liveAgent, admittedIds);
      const disposedMarkers = decision.messages.filter((message) => {
        const id = wakeMessageId(message);
        return isContinuationWakeMessage(message)
          || (id !== undefined && admittedIds.has(id));
      });
      const disposedRpcIds = wakeRpcIds([...admittedMarkers, ...disposedMarkers]);
      markers.cancel(disposedRpcIds);
      markers.finish(disposedRpcIds);
      const disposedIds = wakeMessageIds(disposedMarkers);
      const messages = decision.messages.filter((message) => {
        const id = wakeMessageId(message);
        return !isContinuationWakeMessage(message)
          && (id === undefined || !disposedIds.has(id));
      });
      return messages.length === 0
        ? { kind: "reject" }
        : { ...decision, messages };
    }

    removeWakeByMessageIds(liveAgent, wakeMessageIds(lateMarkers));
    storeDeferredMarkers(state, lateMarkers, markers);
    scheduleDeferredWake(liveAgent, state, markers);

    const controlMarkers = [...admittedMarkers, ...decisionMarkers];
    const hiddenIds = wakeMessageIds([...controlMarkers, ...lateMarkers]);
    const messages = decision.messages.filter((message) => {
      const id = wakeMessageId(message);
      return id === undefined || !hiddenIds.has(id);
    });
    if (controlMarkers.length > 0) {
      removeWakeByMessageIds(liveAgent, admittedIds);
      const controlRpcIds = wakeRpcIds(controlMarkers);
      if (messages.length > 0) {
        markers.cancel(controlRpcIds);
        markers.finish(controlRpcIds);
        return { ...decision, messages };
      }
      let valid = false;
      try {
        valid = await markers.validate(controlRpcIds);
      } catch {
        valid = false;
      }
      if (!valid) {
        markers.cancel(controlRpcIds);
        markers.finish(controlRpcIds);
        return messages.length === 0
          ? { kind: "reject" }
          : { ...decision, messages };
      }
      markers.consume(controlRpcIds);
      markers.finish(controlRpcIds);
      return {
        ...decision,
        messages: messages.length === 0 ? EMPTY_CONTINUATION_MESSAGES : messages,
      };
    }
    if (lateMarkers.length > 0) {
      return messages.length === 0
        ? { kind: "reject" }
        : { ...decision, messages };
    }
    if (!continuationStep || decision.messages.length !== 0) return decision;
    return {
      ...decision,
      messages: EMPTY_CONTINUATION_MESSAGES,
    };
  };
  state = {
    originalPreStep,
    wrappedPreStep,
    previousPreStep,
    pending: false,
    active: false,
    disposeRequested: false,
    idlePromise: undefined,
    deferredMarkers: [],
    deferredWakePromise: undefined,
  };

  Object.defineProperty(internal, "preStep", {
    configurable: true,
    enumerable: false,
    writable: true,
    value: wrappedPreStep,
  });
  Object.defineProperty(agent as unknown as Record<PropertyKey, unknown>, PATCH_STATE, {
    configurable: true,
    enumerable: false,
    writable: false,
    value: state,
  });
  return state;
}

function releaseAgentPatch(
  agent: Agent,
  state: AgentPatchState,
  markers: MarkerTracker,
): void {
  state.disposeRequested = true;
  state.active = false;
  state.pending = false;
  cancelDeferredMarkers(state, markers);
  const cleared = [
    ...clearLegacyWakeMessages(agent),
    ...clearSubagentWakeMessages(agent),
  ];
  markers.cancelSession(String(agent.id));
  markers.finish(wakeRpcIds(cleared));
  restoreAgentPatch(agent, state);
}

/**
 * Start a model turn with no inbox message. Returns false when the live Agent
 * changed after status admission or this DSH build lacks the private boundary.
 */
export function startContinuation(agent: Agent): boolean {
  const state = patchStateOf(agent);
  const internal = agent as unknown as InternalAgent;
  if (state === undefined
    || typeof internal.wakeDriver !== "function"
    || internal.phase?.kind !== "idle"
    || agent.status !== "idle"
    || agent.inbox.hasPending
    || state.pending
    || state.active
    || state.disposeRequested) {
    return false;
  }

  state.pending = true;
  state.active = true;
  try {
    internal.wakeDriver.call(internal);
    const idle = agent.whenIdle();
    state.idlePromise = idle.then(
      () => undefined,
      () => undefined,
    );
    void state.idlePromise.then(() => {
      state.pending = false;
      state.active = false;
      state.idlePromise = undefined;
      if (state.disposeRequested) restoreAgentPatch(agent, state);
    });
    return true;
  } catch {
    state.pending = false;
    state.active = false;
    state.idlePromise = undefined;
    return false;
  }
}

function isContinuableChild(entry: SubagentListEntry): entry is ContinuableChildEntry {
  return entry.kind === "child" && entry.mode === "continuable";
}

function sessionStateFromAgent(agent: Agent): SessionState {
  const parentSessionId = agent.session.header.parentSession;
  return {
    header: agent.session.header,
    events: agent.session.snapshotEvents(),
    inheritedEventCount: agent.session.inheritedEventCount,
    subagent: agent.session.header.origin === "subagent",
    agent,
    ...(parentSessionId === undefined ? {} : { parentSessionId }),
  };
}

async function readSessionState(
  ctx: Context,
  sessionId: ReturnType<typeof SessionId>,
  signal: AbortSignal,
): Promise<SessionState | undefined> {
  const live = ctx.agents.get(sessionId);
  if (live !== undefined) return sessionStateFromAgent(live);
  if (signal.aborted) {
    signal.throwIfAborted();
  }
  const persistence = ctx.get("sessionPersistence");
  if (persistence === undefined) {
    throw new Error("session persistence is not configured");
  }
  const meta = (await awaitWithSignal(persistence.list(signal), signal))
    .find((candidate) => candidate.id === sessionId);
  if (meta === undefined || meta.cwd === undefined) return undefined;
  signal.throwIfAborted();
  const inspected = await awaitWithSignal(persistence.inspect(sessionId, signal), signal);
  if (inspected.meta.cwd === undefined) return undefined;
  return {
    header: inspected.meta,
    events: inspected.events,
    inheritedEventCount: inspected.inheritedEventCount,
    subagent: inspected.meta.origin === "subagent",
    ...(inspected.meta.parentSession === undefined
      ? {}
      : { parentSessionId: inspected.meta.parentSession }),
  };
}

function unavailableSubagentStatus(
  parentSessionId: ReturnType<typeof SessionId>,
  status?: ContinueStatus,
): ResolvedContinueStatus {
  return {
    ...(status ?? { available: false, reason: "none" }),
    available: false,
    target: "subagent",
    parentSessionId,
  };
}

async function inspectSubagentStatus(
  ctx: Context,
  sessionId: ReturnType<typeof SessionId>,
  state: SessionState,
  signal: AbortSignal,
): Promise<ResolvedContinueStatus> {
  const parentSessionId = state.parentSessionId;
  if (parentSessionId === undefined) {
    return {
      ...continueStatusFromEvents(state.events, state.agent, state.inheritedEventCount),
      target: "session",
    };
  }
  if (ctx.agents.get(parentSessionId) === undefined) {
    return unavailableSubagentStatus(parentSessionId);
  }

  const entries = await awaitWithSignal(
    ctx.subagents.listChildren(parentSessionId, signal),
    signal,
  );
  const targetEntry = entries.find((entry) => entry.id === sessionId);
  if (targetEntry === undefined || !isContinuableChild(targetEntry)) {
    return unavailableSubagentStatus(parentSessionId);
  }

  let targetStatus: ContinueStatus | undefined;
  let selected: {
    readonly id: ReturnType<typeof SessionId>;
    readonly eventTime: number;
    readonly order: number;
  } | undefined;

  for (let order = 0; order < entries.length; order += 1) {
    const entry = entries[order];
    if (entry === undefined || !isContinuableChild(entry)) continue;
    const candidateState = await readSessionState(ctx, entry.id, signal);
    if (candidateState === undefined) continue;
    const candidateStatus = continueStatusFromEvents(
      candidateState.events,
      candidateState.agent,
      candidateState.inheritedEventCount,
    );
    const candidateBoundary = latestTurnBoundary(
      candidateState.events,
      candidateState.inheritedEventCount,
    );
    if (entry.id === sessionId) {
      targetStatus = candidateState.agent !== undefined
        && patchStateOf(candidateState.agent) === undefined
        ? { ...candidateStatus, available: false }
        : candidateStatus;
    }
    if (candidateBoundary?.available !== true) continue;
    const eventTime = boundaryTime(candidateState.events, candidateState.inheritedEventCount);
    if (selected === undefined
      || eventTime > selected.eventTime
      || (eventTime === selected.eventTime && order > selected.order)) {
      selected = { id: entry.id, eventTime, order };
    }
  }

  const status = targetStatus ?? { available: false, reason: "none" as const };
  if (status.available && selected?.id === sessionId) {
    return {
      ...status,
      target: "subagent",
      parentSessionId,
    };
  }
  return unavailableSubagentStatus(parentSessionId, status);
}

async function inspectStatus(
  ctx: Context,
  sessionId: ReturnType<typeof SessionId>,
  signal: AbortSignal,
): Promise<ResolvedContinueStatus> {
  const state = await readSessionState(ctx, sessionId, signal);
  if (state === undefined) return { available: false, reason: "none", target: "session" };
  if (state.subagent) {
    return inspectSubagentStatus(ctx, sessionId, state, signal);
  }
  return {
    ...continueStatusFromEvents(state.events, state.agent, state.inheritedEventCount),
    target: "session",
  };
}

// The preset a session actually runs: the newest `agent-preset/selected`
// event wins, otherwise the creation header's value — the same fold the
// `agentPreset` session projection in dsh-agent-presets performs.
function resolveSessionAgentPreset(
  header: SessionHeader,
  events: readonly SessionEvent[],
): string | undefined {
  for (let index = events.length - 1; index >= 0; index -= 1) {
    const event = events[index];
    if (event?.type === "agent-preset/selected") return event.data.agentPreset;
  }
  return header.agentPreset;
}

async function resolveLiveAgent(
  ctx: Context,
  runtime: ContinueRuntimeState,
  sessionId: ReturnType<typeof SessionId>,
  signal: AbortSignal,
): Promise<LiveAgentResolution | ConnectionRpcResult<unknown>> {
  const existing = ctx.agents.get(sessionId);
  if (existing !== undefined) return { agent: existing };
  if (signal.aborted) return cancelled();

  const state = await readSessionState(ctx, sessionId, signal);
  if (state === undefined) return internal("找不到可恢复的会话。");
  const presetId = resolveSessionAgentPreset(state.header, state.events);
  const defaultSelection = ctx.agentDefaultModel.currentSelection();
  const loggedSelection = foldRequestHeader(state.events)?.config;
  const initialSelection: ModelSelection = loggedSelection === undefined
    ? defaultSelection
    : {
      provider: loggedSelection.provider,
      model: loggedSelection.model,
      ...(loggedSelection.reasoningEffort === undefined
        ? {}
        : { reasoningEffort: loggedSelection.reasoningEffort }),
    };

  let publishedHandle: AgentHandle | undefined;
  let disposal: Promise<void> | undefined;
  let disposeRequested = false;
  let accepted = false;
  const disposeOwned = async () => {
    disposeRequested = true;
    signal.removeEventListener("abort", cancelUnaccepted);
    if (publishedHandle !== undefined && disposal === undefined) {
      disposal = publishedHandle.dispose();
    }
    if (disposal !== undefined) await disposal;
  };
  const cancelUnaccepted = () => {
    if (!accepted) void disposeOwned();
  };
  const acceptOwned = () => {
    accepted = true;
    signal.removeEventListener("abort", cancelUnaccepted);
  };
  signal.addEventListener("abort", cancelUnaccepted, { once: true });

  const resumeOperation = ctx.agents.resume({
        resumeSessionId: sessionId,
        agentOptions: {
          provider: initialSelection.provider,
          model: initialSelection.model,
        },
        signal,
        setup: async (agentCtx) => {
          signal.throwIfAborted();
          if (runtime.shuttingDown) throw new Error("dsh-continue is shutting down");
          const resumed = (agentCtx as Context & { readonly agent?: Agent }).agent;
          if (resumed === undefined) throw new Error("dsh-continue: resumed Agent scope is missing");
          let picked: ModelSelection | undefined;
          const selection: ModelSelectionRef = {
            get current() {
              if (picked !== undefined) return picked;
              const logged = resumed.session.requestHeader()?.config;
              if (logged === undefined) return ctx.agentDefaultModel.currentSelection();
              return {
                provider: logged.provider,
                model: logged.model,
                ...(logged.reasoningEffort === undefined
                  ? {}
                  : { reasoningEffort: logged.reasoningEffort }),
              };
            },
            set current(next) {
              picked = next;
            },
            assembled: undefined,
          };
          const disposeSelection = installModelSelection(agentCtx, selection);
          let disposeAfterRequest: (() => void) | undefined;
          disposeAfterRequest = agentCtx.on("agent/request", async (_payload, next) => {
            try {
              return await next();
            } finally {
              disposeAfterRequest?.();
              disposeAfterRequest = undefined;
              disposeSelection();
            }
          });
          await ctx.agentPresets.mount(agentCtx, presetId);
          return {
            commit() {
              signal.throwIfAborted();
              if (runtime.shuttingDown) throw new Error("dsh-continue is shutting down");
            },
          };
        },
      });
  const observedResume = resumeOperation.then(async (handle) => {
    publishedHandle = handle;
    if (disposeRequested || signal.aborted || runtime.shuttingDown) {
      await disposeOwned();
      throw signal.reason ?? new Error("dsh-continue resume was cancelled");
    }
    return handle;
  });

  try {
    const handle = await awaitWithSignal(observedResume, signal);
    if (signal.aborted || runtime.shuttingDown) {
      await disposeOwned();
      return cancelled();
    }
    return {
      agent: handle.agent,
      accept: acceptOwned,
      dispose: disposeOwned,
    };
  } catch (error) {
    if (signal.aborted || runtime.shuttingDown) {
      void disposeOwned();
      return cancelled();
    }
    signal.removeEventListener("abort", cancelUnaccepted);
    const raced = ctx.agents.get(sessionId);
    if (raced !== undefined) return { agent: raced };
    throw error;
  }
}

function boundaryStillMatches(
  advertised: ContinueStatus,
  current: ContinueStatus,
  request: ResumeRequest,
): boolean {
  if (advertised.turn !== request.turn || advertised.boundarySeq !== request.boundarySeq) {
    return false;
  }
  if (current.turn !== request.turn) return false;
  if (current.boundarySeq === request.boundarySeq) return true;
  // Persistence may append the authoritative `interrupted` turn/end while a
  // cold Agent is being resumed. The turn identity and reason remain stable;
  // only the boundary sequence moves forward.
  return advertised.reason === "interrupted"
    && current.reason === "interrupted"
    && typeof current.boundarySeq === "number"
    && current.boundarySeq > request.boundarySeq;
}

function expectedBoundaryEvent(
  events: readonly SessionEvent[],
  inheritedEventCount: number,
  expected: ContinueStatus,
  deferredTurns = 0,
): SessionEvent | undefined {
  if (expected.available !== true
    || expected.reason === "none"
    || expected.turn === undefined
    || expected.boundarySeq === undefined) {
    return undefined;
  }
  const start = Math.max(0, Math.min(inheritedEventCount, events.length));
  const boundaries = events.slice(start).filter(
    (event) => event.type === "turn/start" || event.type === "turn/end",
  );
  let matchedIndex = -1;
  for (let index = boundaries.length - 1; index >= 0; index -= 1) {
    const event = boundaries[index];
    if (event === undefined) continue;
    const status = statusFromBoundaryEvent(event);
    if (status?.turn !== expected.turn || status.reason !== expected.reason) continue;
    const exact = event.seq === expected.boundarySeq;
    const repairedInterrupted = expected.reason === "interrupted"
      && event.seq > expected.boundarySeq;
    if (exact || repairedInterrupted) {
      matchedIndex = index;
      break;
    }
  }
  if (matchedIndex < 0) return undefined;
  const matched = boundaries[matchedIndex];
  if (matched === undefined) return undefined;
  const later = boundaries.slice(matchedIndex + 1);
  let cursor = 0;
  let previousTurn = expected.turn;
  for (let count = 0; count < deferredTurns; count += 1) {
    const deferredStart = later[cursor];
    const deferredEnd = later[cursor + 1];
    if (deferredStart?.type !== "turn/start"
      || deferredEnd?.type !== "turn/end"
      || deferredStart.data.turn <= previousTurn
      || deferredEnd.data.turn !== deferredStart.data.turn
      || deferredEnd.data.reason.kind !== "blocked") {
      return undefined;
    }
    previousTurn = deferredStart.data.turn;
    cursor += 2;
  }
  const remaining = later.slice(cursor);
  if (remaining.length === 0) return matched;
  if (remaining.length === 1) {
    const current = remaining[0];
    if (current?.type === "turn/start" && current.data.turn > previousTurn) {
      return matched;
    }
  }
  return undefined;
}

async function validateSubagentMarker(
  ctx: Context,
  sessionId: ReturnType<typeof SessionId>,
  expected: ResolvedContinueStatus,
  signal: AbortSignal,
  deferredTurns: number,
): Promise<boolean> {
  if (signal.aborted || expected.target !== "subagent") return false;
  const parentSessionId = expected.parentSessionId;
  if (parentSessionId === undefined || ctx.agents.get(parentSessionId) === undefined) return false;
  const entries = await awaitWithSignal(
    ctx.subagents.listChildren(parentSessionId, signal),
    signal,
  );
  const targetEntry = entries.find((entry) => entry.id === sessionId);
  if (targetEntry === undefined || !isContinuableChild(targetEntry)) return false;

  let selected: {
    readonly id: ReturnType<typeof SessionId>;
    readonly eventTime: number;
    readonly order: number;
  } | undefined;
  for (let order = 0; order < entries.length; order += 1) {
    const entry = entries[order];
    if (entry === undefined || !isContinuableChild(entry)) continue;
    const state = await readSessionState(ctx, entry.id, signal);
    if (state === undefined) continue;
    const event = entry.id === sessionId
      ? expectedBoundaryEvent(state.events, state.inheritedEventCount, expected, deferredTurns)
      : latestTurnBoundaryEvent(state.events, state.inheritedEventCount);
    if (event === undefined) {
      if (entry.id === sessionId) return false;
      continue;
    }
    const status = statusFromBoundaryEvent(event);
    if (status?.available !== true) continue;
    const eventTime = event.time;
    if (selected === undefined
      || eventTime > selected.eventTime
      || (eventTime === selected.eventTime && order > selected.order)) {
      selected = { id: entry.id, eventTime, order };
    }
  }
  return selected?.id === sessionId;
}

async function resumeSubagent(
  ctx: Context,
  runtime: ContinueRuntimeState,
  sessionId: ReturnType<typeof SessionId>,
  advertised: ResolvedContinueStatus,
  request: ResumeRequest,
  signal: AbortSignal,
): Promise<ConnectionRpcResult<unknown>> {
  if (advertised.target !== "subagent") return busy("当前目标不是 subagent 会话。");
  const current = await inspectStatus(ctx, sessionId, signal);
  if (!current.available
    || current.target !== "subagent"
    || !boundaryStillMatches(advertised, current, request)) {
    return busy("subagent 状态已经变化，请重新读取后再试。");
  }
  const parentSessionId = current.parentSessionId;
  if (parentSessionId === undefined) return busy("subagent 的父会话当前不可用。");
  const parent = ctx.agents.get(parentSessionId);
  if (parent === undefined) return busy("subagent 的父会话当前不在线。");

  const child = ctx.agents.get(sessionId);
  if (child !== undefined) {
    if (patchStateOf(child) === undefined) {
      return busy("当前 DSH 版本不支持无消息续跑。");
    }
    const liveStatus = continueStatusFromEvents(
      child.session.snapshotEvents(),
      child,
      child.session.inheritedEventCount,
    );
    if (!liveStatus.available || !boundaryStillMatches(advertised, liveStatus, request)) {
      return busy("subagent 状态已经变化，请重新读取后再试。");
    }
  }
  if (signal.aborted || runtime.shuttingDown) return cancelled();

  const wakeSourceId = rpcId(SUBAGENT_WAKE_PREFIX);
  const markerId = String(wakeSourceId);
  const marker = runtime.markers.track(
    markerId,
    String(sessionId),
    (deferredTurns) => validateSubagentMarker(
      ctx,
      sessionId,
      current,
      signal,
      deferredTurns,
    ),
  );
  const cancelOnAbort = () => marker.cancel();
  signal.addEventListener("abort", cancelOnAbort, { once: true });
  let admitted = false;
  let admittedChild = child;
  try {
    await awaitWithSignal(
      queueHostSubagentPrompt(ctx.subagents, parent, sessionId, [], {
        kind: "user",
        rpcId: wakeSourceId,
      } as MessageSource, signal),
      signal,
    );
    admitted = true;
    admittedChild = ctx.agents.get(sessionId) ?? admittedChild;
    const outcome = await awaitWithSignal(marker.outcome, signal);
    if (outcome !== "consumed") {
      const removed = admittedChild === undefined
        ? 0
        : removeWakeByRpcId(admittedChild, markerId);
      if (removed > 0) runtime.markers.finish([markerId]);
      return cancelled();
    }
    return { ok: true, value: { accepted: true } };
  } catch (error) {
    marker.cancel();
    const removed = admittedChild === undefined
      ? 0
      : removeWakeByRpcId(admittedChild, markerId);
    if (removed > 0 || (!admitted && !signal.aborted && !runtime.shuttingDown)) {
      runtime.markers.finish([markerId]);
    }
    if (signal.aborted || runtime.shuttingDown) return cancelled();
    try {
      ctx.logger.warn(`dsh-continue: subagent followup failed: ${String(error)}`);
    } catch {
      // Diagnostics must not affect the RPC response.
    }
    return busy("subagent 状态已经变化，请稍后重新读取。");
  } finally {
    signal.removeEventListener("abort", cancelOnAbort);
    if (!admitted) marker.cancel();
  }
}

export function createContinueRpcHandler(ctx: Context): ConnectionRpcHandler {
  const runtime = RUNTIME_STATES.get(ctx) ?? createRuntimeState();
  const inFlight = new Set<string>();
  const parentInFlight = new Set<string>();

  const handle: ConnectionRpcHandler = async (endpoint, payload, signal) => {
    if (endpoint === ENDPOINT_STATUS) {
      const rawSessionId = decodeStatusPayload(payload);
      if (rawSessionId === undefined) return badRequest("dsh-continue: invalid status payload");
      try {
        return {
          ok: true,
          value: await inspectStatus(ctx, SessionId(rawSessionId), signal),
        };
      } catch (error) {
        if (signal.aborted || runtime.shuttingDown) return cancelled();
        try {
          ctx.logger.warn(`dsh-continue: status inspection failed: ${String(error)}`);
        } catch {
          // Diagnostics must not affect the RPC response.
        }
        return internal("无法读取当前会话状态。");
      }
    }

    if (endpoint !== ENDPOINT_RESUME) {
      return badRequest(`dsh-continue: unknown endpoint ${JSON.stringify(endpoint)}`);
    }
    const request = decodeResumePayload(payload);
    if (request === undefined) return badRequest("dsh-continue: invalid resume payload");
    if (inFlight.has(request.sessionId)) return busy("已有续跑请求正在处理。");
    inFlight.add(request.sessionId);
    try {
      const sessionId = SessionId(request.sessionId);
      const advertised = await inspectStatus(ctx, sessionId, signal);
      if (signal.aborted) return cancelled();
      if (!advertised.available
        || advertised.turn !== request.turn
        || advertised.boundarySeq !== request.boundarySeq) {
        return advertised.available
          ? busy("会话状态已经变化，请重新读取后再试。")
          : busy("当前会话没有可续跑的失败任务。");
      }

      if (advertised.target === "subagent") {
        const parentKey = advertised.parentSessionId === undefined
          ? undefined
          : String(advertised.parentSessionId);
        if (parentKey === undefined) return busy("subagent 的父会话当前不可用。");
        if (parentInFlight.has(parentKey)) return busy("该父会话已有 subagent 续跑请求正在处理。");
        parentInFlight.add(parentKey);
        try {
          return await resumeSubagent(ctx, runtime, sessionId, advertised, request, signal);
        } finally {
          parentInFlight.delete(parentKey);
        }
      }

      const resolved = await resolveLiveAgent(ctx, runtime, sessionId, signal);
      if (isRecord(resolved) && "ok" in resolved) {
        return resolved as ConnectionRpcResult<unknown>;
      }
      const resolution = resolved as LiveAgentResolution;
      const liveAgent = resolution.agent;
      const disposeColdAgent = async () => {
        if (resolution.dispose === undefined) return;
        try {
          await resolution.dispose();
        } catch {
          // Agent ownership also rolls back with the plugin fiber.
        }
      };
      const current = continueStatusFromEvents(
        liveAgent.session.snapshotEvents(),
        liveAgent,
        liveAgent.session.inheritedEventCount,
      );
      if (!current.available || !boundaryStillMatches(advertised, current, request)) {
        await disposeColdAgent();
        return busy("会话状态已经变化，请重新读取后再试。");
      }
      if (signal.aborted || runtime.shuttingDown) {
        await disposeColdAgent();
        return cancelled();
      }
      if (!startContinuation(liveAgent)) {
        await disposeColdAgent();
        return busy("会话正在处理其他输入，请稍后再试。");
      }
      resolution.accept?.();
      if (resolution.dispose !== undefined) {
        void liveAgent.whenIdle().then(disposeColdAgent, disposeColdAgent);
      }
      return { ok: true, value: { accepted: true } };
    } catch (error) {
      if (signal.aborted || runtime.shuttingDown) return cancelled();
      try {
        ctx.logger.warn(`dsh-continue: resume failed: ${String(error)}`);
      } catch {
        // Diagnostics must not affect the RPC response.
      }
      return internal("续跑请求未能启动。");
    } finally {
      inFlight.delete(request.sessionId);
    }
  };

  return async (endpoint, payload, signal) => {
    if (runtime.shuttingDown) return cancelled();
    let settleRequest!: () => void;
    const activeRequest = new Promise<void>((resolve) => {
      settleRequest = resolve;
    });
    runtime.activeRequests.add(activeRequest);
    try {
      if (runtime.shuttingDown) return cancelled();
      const operationSignal = AbortSignal.any([signal, runtime.shutdown.signal]);
      try {
        return await awaitWithSignal(
          handle(endpoint, payload, operationSignal),
          operationSignal,
        );
      } catch (error) {
        if (operationSignal.aborted) return cancelled();
        throw error;
      }
    } finally {
      runtime.activeRequests.delete(activeRequest);
      settleRequest();
    }
  };
}

export function apply(ctx: Context): void {
  const runtime = createRuntimeState();
  const patches = new Map<Agent, AgentPatchState>();
  RUNTIME_STATES.set(ctx, runtime);

  const releasePatch = (agent: Agent, state: AgentPatchState): void => {
    releaseAgentPatch(agent, state, runtime.markers);
  };

  // Registered first so Cordis removes the RPC and setup contribution before
  // the final Agent boundary is restored.
  ctx.effect(() => async () => {
    beginShutdown(runtime);
    await Promise.allSettled([...runtime.activeRequests]);
    for (const [agent, state] of patches) {
      patches.delete(agent);
      releasePatch(agent, state);
    }
    if (RUNTIME_STATES.get(ctx) === runtime) RUNTIME_STATES.delete(ctx);
  }, "dsh-continue: restore Agent wake patches");

  const install = ({ agent }: { agent: Agent }) => {
    if (runtime.shuttingDown) return;
    const state = installAgentPatch(agent, ctx, runtime.markers);
    if (state !== undefined) patches.set(agent, state);
    cleanupLegacyWakesWhenIdle(agent);
  };
  const release = ({ agent }: { agent: Agent }) => {
    const state = patches.get(agent);
    if (state === undefined) return;
    patches.delete(agent);
    releasePatch(agent, state);
  };

  // `agent/created` fires synchronously during publication, before an Agent's
  // loop can take a step, so it also covers continuable children created or
  // cold-resumed by the subagent service.
  for (const agent of ctx.agents.list()) install({ agent });
  ctx.on("agent/created", install);
  ctx.on("agent/status", ({ agent, status }) => {
    if (status === "idle") cleanupLegacyWakesWhenIdle(agent);
  });
  ctx.on("agent/disposed", release);

  // Registered last so shutdown first closes admission, settles every entered
  // request, and only then lets Cordis dispose setup hooks and Agent patches.
  ctx.effect(() => {
    const disposeRpc = ctx.connection.rpc.handle(
      RPC_CHANNEL,
      createContinueRpcHandler(ctx),
    );
    return async () => {
      beginShutdown(runtime);
      await disposeRpc();
      await Promise.allSettled([...runtime.activeRequests]);
    };
  }, "dsh-continue: RPC channel");
}
