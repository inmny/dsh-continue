import { installModelSelection } from "@deepseek-ai/dsh-agent";
import { RpcId, } from "@deepseek-ai/dsh-client-connection";
import { queueHostSubagentPrompt } from "@deepseek-ai/dsh-subagent/internal";
import { foldRequestHeader, SessionId, } from "@deepseek-ai/dsh-session";
import { randomUUID } from "node:crypto";
export const name = "continue";
export const inject = [
    "agents",
    "agentDefaultModel",
    "agentPresets",
    "connection",
    "sessionPersistence",
    "subagents",
    // `connection.rpc.handle` registers its route on the caller's webServer
    // service since dsh 0.1.5.
    "webServer",
];
export const RPC_CHANNEL = "/dsh-continue";
const ENDPOINT_STATUS = "status";
const ENDPOINT_RESUME = "resume";
const MAX_SESSION_ID_LENGTH = 512;
const CONTINUE_WAKE_PREFIX = "dsh-continue:";
const SUBAGENT_WAKE_PREFIX = "dsh-continue:subagent:";
const PATCH_STATE = Symbol.for("dsh-continue.agent-wake-patch.v2");
const RUNTIME_STATES = new WeakMap();
function createMarkerTracker() {
    const records = new Map();
    const settle = (rpcId, outcome) => {
        const record = records.get(rpcId);
        if (record === undefined || record.outcome !== undefined)
            return;
        record.outcome = outcome;
        record.settle(outcome);
    };
    return {
        track(rpcId, sessionId, validate) {
            if (records.has(rpcId))
                throw new Error(`duplicate continuation marker: ${rpcId}`);
            let settlePromise;
            const promise = new Promise((resolve) => {
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
                if (record !== undefined && record.outcome === undefined)
                    record.deferredTurns += 1;
            }
        },
        async validate(rpcIds) {
            for (const rpcId of new Set(rpcIds)) {
                const record = records.get(rpcId);
                if (record === undefined)
                    continue;
                if (record.outcome === "cancelled")
                    return false;
                if (record.validate !== undefined
                    && !await record.validate(record.deferredTurns))
                    return false;
                if (records.get(rpcId)?.outcome === "cancelled")
                    return false;
            }
            return true;
        },
        consume(rpcIds) {
            for (const rpcId of rpcIds)
                settle(rpcId, "consumed");
        },
        cancel(rpcIds) {
            for (const rpcId of rpcIds)
                settle(rpcId, "cancelled");
        },
        finish(rpcIds) {
            for (const rpcId of rpcIds)
                records.delete(rpcId);
        },
        cancelSession(sessionId) {
            for (const [rpcId, record] of records) {
                if (record.sessionId === sessionId)
                    settle(rpcId, "cancelled");
            }
        },
        cancelAll() {
            for (const rpcId of records.keys())
                settle(rpcId, "cancelled");
        },
    };
}
function createRuntimeState() {
    return {
        markers: createMarkerTracker(),
        activeRequests: new Set(),
        shutdown: new AbortController(),
        shuttingDown: false,
    };
}
function beginShutdown(runtime) {
    runtime.shuttingDown = true;
    if (!runtime.shutdown.signal.aborted) {
        runtime.shutdown.abort(new Error("dsh-continue is shutting down"));
    }
    runtime.markers.cancelAll();
}
function awaitWithSignal(operation, signal) {
    if (signal.aborted)
        return Promise.reject(signal.reason);
    return new Promise((resolve, reject) => {
        const onAbort = () => {
            signal.removeEventListener("abort", onAbort);
            reject(signal.reason);
        };
        signal.addEventListener("abort", onAbort, { once: true });
        void Promise.resolve(operation).then((value) => {
            signal.removeEventListener("abort", onAbort);
            resolve(value);
        }, (error) => {
            signal.removeEventListener("abort", onAbort);
            reject(error);
        });
    });
}
/**
 * AgentLoop only checks `messages.length` before iterating the collection. The
 * continuation sentinel therefore opens a real model step while its iterator
 * contributes no message to the Session surface or durable event log.
 */
const EMPTY_CONTINUATION_MESSAGES = Object.freeze({
    length: 1,
    [Symbol.iterator]: function* () {
        // Deliberately empty: this is a control sentinel, not a user message.
    },
});
function isRecord(value) {
    return typeof value === "object" && value !== null && !Array.isArray(value);
}
function exactKeys(value, keys) {
    const expected = new Set(keys);
    return Object.keys(value).every((key) => expected.delete(key)) && expected.size === 0;
}
function badRequest(message) {
    return {
        ok: false,
        error: {
            code: "bad-request",
            message,
            details: { issues: [] },
        },
    };
}
function busy(message) {
    return {
        ok: false,
        error: {
            code: "agent-busy",
            message,
            details: { reason: "dsh-continue" },
        },
    };
}
function internal(message) {
    return {
        ok: false,
        error: {
            code: "internal",
            message,
            details: {},
        },
    };
}
function cancelled() {
    return {
        ok: false,
        error: {
            code: "cancelled",
            message: "续跑请求已取消。",
            details: {},
        },
    };
}
function validSessionId(value) {
    return typeof value === "string"
        && value.length > 0
        && value.length <= MAX_SESSION_ID_LENGTH
        && value.trim() === value;
}
function validBoundaryNumber(value) {
    return typeof value === "number" && Number.isSafeInteger(value) && value >= 0;
}
function isSubagentContinuationWakeMessage(value) {
    if (!isRecord(value) || value.role !== "user")
        return false;
    if (!Array.isArray(value.content) || value.content.length !== 0)
        return false;
    const source = value.source;
    return isRecord(source)
        && source.kind === "user"
        && typeof source.rpcId === "string"
        && source.rpcId.startsWith(SUBAGENT_WAKE_PREFIX);
}
function isLegacyContinuationWakeMessage(value) {
    if (!isRecord(value) || value.role !== "user")
        return false;
    if (!Array.isArray(value.content) || value.content.length !== 0)
        return false;
    const source = value.source;
    return isRecord(source)
        && source.kind === "user"
        && typeof source.rpcId === "string"
        && source.rpcId.startsWith(CONTINUE_WAKE_PREFIX)
        && !source.rpcId.startsWith(SUBAGENT_WAKE_PREFIX);
}
function isContinuationWakeMessage(value) {
    return isLegacyContinuationWakeMessage(value) || isSubagentContinuationWakeMessage(value);
}
function wakeRpcId(value) {
    if (!isContinuationWakeMessage(value))
        return undefined;
    const source = value.source;
    return isRecord(source) && typeof source.rpcId === "string" ? source.rpcId : undefined;
}
function wakeMessageId(value) {
    return isRecord(value) && typeof value.id === "string" ? value.id : undefined;
}
function wakeMessageIds(values) {
    return new Set(values
        .map(wakeMessageId)
        .filter((value) => value !== undefined));
}
function wakeRpcIds(values) {
    return values
        .map(wakeRpcId)
        .filter((value) => value !== undefined);
}
function listWakeMessages(agent, predicate) {
    const inbox = agent.inbox;
    if (!Array.isArray(inbox.nextStep) || !Array.isArray(inbox.nextTurn))
        return [];
    return [...inbox.nextStep, ...inbox.nextTurn].filter(predicate);
}
function removeWakeMessages(agent, predicate) {
    const inbox = agent.inbox;
    if (typeof inbox.remove !== "function")
        return [];
    const removed = [];
    for (const message of listWakeMessages(agent, predicate)) {
        if (!predicate(message))
            continue;
        const id = message.id;
        if (typeof id === "string" && inbox.remove(id))
            removed.push(message);
    }
    return removed;
}
function removeWakeByMessageIds(agent, ids) {
    const inbox = agent.inbox;
    if (typeof inbox.remove !== "function")
        return;
    for (const id of ids)
        inbox.remove(id);
}
function storeDeferredMarkers(state, messages, markers) {
    const existingIds = wakeMessageIds(state.deferredMarkers);
    for (const message of messages) {
        const id = wakeMessageId(message);
        if (id !== undefined && existingIds.has(id))
            continue;
        state.deferredMarkers.push(message);
        if (id !== undefined)
            existingIds.add(id);
    }
    markers.defer(wakeRpcIds(messages));
}
function cancelDeferredMarkers(state, markers) {
    const rpcIds = wakeRpcIds(state.deferredMarkers);
    state.deferredMarkers.length = 0;
    markers.cancel(rpcIds);
    markers.finish(rpcIds);
}
function scheduleDeferredWake(agent, state, markers) {
    if (state.deferredMarkers.length === 0 || state.deferredWakePromise !== undefined)
        return;
    const settled = agent.whenIdle().then(() => {
        state.deferredWakePromise = undefined;
        if (state.disposeRequested || patchStateOf(agent) !== state)
            return;
        const internal = agent;
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
        }
        catch {
            state.pending = false;
            cancelDeferredMarkers(state, markers);
        }
    }, () => {
        state.deferredWakePromise = undefined;
        cancelDeferredMarkers(state, markers);
    });
    state.deferredWakePromise = settled;
    void settled.catch(() => undefined);
}
function removeWakeByRpcId(agent, rpcId) {
    return removeWakeMessages(agent, (value) => wakeRpcId(value) === rpcId).length;
}
function clearLegacyWakeMessages(agent) {
    return removeWakeMessages(agent, isLegacyContinuationWakeMessage);
}
function clearSubagentWakeMessages(agent) {
    return removeWakeMessages(agent, isSubagentContinuationWakeMessage);
}
export function clearLegacyContinuationWakes(agent) {
    return removeWakeMessages(agent, isContinuationWakeMessage).length;
}
function cleanupLegacyWakesWhenIdle(agent) {
    const internal = agent;
    if (agent.status === "idle" && internal.phase?.kind === "idle") {
        clearLegacyWakeMessages(agent);
    }
}
function decodeStatusPayload(payload) {
    if (!isRecord(payload) || !exactKeys(payload, ["sessionId"]))
        return undefined;
    return validSessionId(payload.sessionId) ? payload.sessionId : undefined;
}
function decodeResumePayload(payload) {
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
function rpcId(prefix = CONTINUE_WAKE_PREFIX) {
    return RpcId(`${prefix}${randomUUID()}`);
}
function reasonFromTurnEnd(event) {
    if (event.type !== "turn/end")
        return "none";
    const reason = event.data.reason;
    if (reason.kind === "error")
        return "request-error";
    if (reason.kind === "interrupted")
        return "interrupted";
    if (reason.kind === "max-tokens")
        return "max-tokens";
    if (reason.kind === "aborted" && reason.reason.kind === "disposed")
        return "disposed";
    if (reason.kind === "aborted" && reason.reason.kind === "user")
        return "user";
    return "none";
}
function latestTurnBoundaryEvent(events, inheritedEventCount = 0) {
    const start = Math.max(0, Math.min(inheritedEventCount, events.length));
    for (let index = events.length - 1; index >= start; index -= 1) {
        const event = events[index];
        if (event?.type === "turn/end" || event?.type === "turn/start")
            return event;
    }
    return undefined;
}
function statusFromBoundaryEvent(event) {
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
function latestTurnBoundary(events, inheritedEventCount = 0) {
    const event = latestTurnBoundaryEvent(events, inheritedEventCount);
    return event === undefined ? undefined : statusFromBoundaryEvent(event);
}
function boundaryTime(events, inheritedEventCount) {
    return latestTurnBoundaryEvent(events, inheritedEventCount)?.time ?? -1;
}
function replayPendingMessages(events, inheritedEventCount) {
    const nextTurn = [];
    const nextStep = [];
    const start = Math.max(0, Math.min(inheritedEventCount, events.length));
    for (const event of events.slice(start)) {
        if (event.type !== "agent/inbox/spliced")
            continue;
        const list = event.data.target === "next-turn" ? nextTurn : nextStep;
        const position = Math.max(0, Math.min(list.length, event.data.start));
        list.splice(position, event.data.removedCount ?? 0, ...event.data.inserted);
    }
    return [...nextStep, ...nextTurn].filter((message) => !isContinuationWakeMessage(message));
}
function hasPendingMessages(events, agent, inheritedEventCount = 0) {
    if (agent !== undefined)
        return inboxHasPending(agent);
    return replayPendingMessages(events, inheritedEventCount).length > 0;
}
// Wake markers are the plugin's own control-plane messages: only ordinary
// input counts as pending, matching the boundary logic in the preStep patch.
function inboxHasPending(agent) {
    return [...agent.inbox.nextStep, ...agent.inbox.nextTurn].some((message) => !isContinuationWakeMessage(message));
}
/** Classify the latest durable turn and reject sessions with unrelated pending input. */
export function continueStatusFromEvents(events, agent, inheritedEventCount = 0) {
    const boundary = latestTurnBoundary(events, inheritedEventCount);
    if (boundary === undefined)
        return { available: false, reason: "none" };
    if (!boundary.available)
        return boundary;
    const internal = agent;
    if (agent?.status === "running"
        || (agent !== undefined && internal?.phase?.kind !== "idle")
        || hasPendingMessages(events, agent, inheritedEventCount)) {
        return { ...boundary, available: false };
    }
    return boundary;
}
function patchStateOf(agent) {
    const value = agent[PATCH_STATE];
    return isRecord(value)
        && typeof value.originalPreStep === "function"
        && typeof value.wrappedPreStep === "function"
        && (value.previousPreStep === undefined || isRecord(value.previousPreStep))
        && typeof value.pending === "boolean"
        && typeof value.active === "boolean"
        && typeof value.disposeRequested === "boolean"
        && Array.isArray(value.deferredMarkers)
        && (value.deferredWakePromise === undefined || value.deferredWakePromise instanceof Promise)
        ? value
        : undefined;
}
function restoreAgentPatch(agent, state) {
    const internal = agent;
    const object = agent;
    if (internal.preStep === state.wrappedPreStep) {
        if (state.previousPreStep === undefined) {
            Reflect.deleteProperty(object, "preStep");
        }
        else {
            Object.defineProperty(agent, "preStep", state.previousPreStep);
        }
    }
    if (object[PATCH_STATE] === state)
        Reflect.deleteProperty(object, PATCH_STATE);
}
function installAgentPatch(agent, ctx, markers) {
    const existing = patchStateOf(agent);
    if (existing !== undefined)
        return existing;
    const internal = agent;
    if (typeof internal.preStep !== "function" || typeof internal.wakeDriver !== "function") {
        try {
            ctx.logger.warn("dsh-continue: this DSH AgentLoop has no compatible empty-wake boundary");
        }
        catch {
            // Diagnostics must not affect the Agent lifecycle.
        }
        return undefined;
    }
    if (typeof agent.whenIdle !== "function") {
        try {
            ctx.logger.warn("dsh-continue: this DSH Agent has no quiescence API");
        }
        catch {
            // Diagnostics must not affect the Agent lifecycle.
        }
        return undefined;
    }
    const originalPreStep = internal.preStep;
    const previousPreStep = Object.getOwnPropertyDescriptor(agent, "preStep");
    if (previousPreStep !== undefined && previousPreStep.configurable !== true && previousPreStep.writable !== true) {
        try {
            ctx.logger.warn("dsh-continue: Agent.preStep is not patchable");
        }
        catch {
            // Diagnostics must not affect the Agent lifecycle.
        }
        return undefined;
    }
    let state;
    const wrappedPreStep = async function (target, position) {
        const continuationStep = state.pending && target === "next-turn" && position.step === 1;
        const continuationBoundary = target === "next-turn" && position.step === 1;
        const liveAgent = this;
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
        const admittedIdsSeen = new Set();
        const admittedMarkers = [...inboxMarkers, ...deferredMarkers].filter((message) => {
            const id = wakeMessageId(message);
            if (id === undefined || !admittedIdsSeen.has(id)) {
                if (id !== undefined)
                    admittedIdsSeen.add(id);
                return true;
            }
            return false;
        });
        const admittedIds = wakeMessageIds(admittedMarkers);
        if (continuationStep) {
            state.pending = false;
        }
        const admittedRpcIds = wakeRpcIds(admittedMarkers);
        let decision;
        try {
            decision = await originalPreStep.call(this, target, position);
        }
        catch (error) {
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
            }
            catch {
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
        if (!continuationStep || decision.messages.length !== 0)
            return decision;
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
    Object.defineProperty(agent, PATCH_STATE, {
        configurable: true,
        enumerable: false,
        writable: false,
        value: state,
    });
    return state;
}
function releaseAgentPatch(agent, state, markers) {
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
export function startContinuation(agent) {
    const state = patchStateOf(agent);
    const internal = agent;
    if (state === undefined
        || typeof internal.wakeDriver !== "function"
        || internal.phase?.kind !== "idle"
        || agent.status !== "idle"
        || inboxHasPending(agent)
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
        state.idlePromise = idle.then(() => undefined, () => undefined);
        void state.idlePromise.then(() => {
            state.pending = false;
            state.active = false;
            state.idlePromise = undefined;
            if (state.disposeRequested)
                restoreAgentPatch(agent, state);
        });
        return true;
    }
    catch {
        state.pending = false;
        state.active = false;
        state.idlePromise = undefined;
        return false;
    }
}
function isContinuableChild(entry) {
    return entry.mode === "continuable";
}
function sessionStateFromAgent(agent) {
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
async function readSessionState(ctx, sessionId, signal) {
    const live = ctx.agents.get(sessionId);
    if (live !== undefined)
        return sessionStateFromAgent(live);
    if (signal.aborted) {
        signal.throwIfAborted();
    }
    const persistence = ctx.get("sessionPersistence");
    if (persistence === undefined) {
        throw new Error("session persistence is not configured");
    }
    const snapshot = await awaitWithSignal(persistence.stat(sessionId, { signal }), signal);
    if (snapshot === undefined || snapshot.header.cwd === undefined)
        return undefined;
    let handle;
    try {
        handle = await awaitWithSignal(persistence.open(sessionId, "read", { signal }), signal);
    }
    catch (error) {
        // The failure carries no cross-module-copy-safe brand; a matching session
        // id means the stored log vanished between stat and open.
        if (isRecord(error) && error.sessionId === sessionId)
            return undefined;
        throw error;
    }
    try {
        if (handle.header.cwd === undefined)
            return undefined;
        const { events } = await awaitWithSignal(handle.read(0, undefined, { signal }), signal);
        return {
            header: handle.header,
            events,
            inheritedEventCount: handle.inheritedEventCount,
            subagent: handle.header.origin === "subagent",
            ...(handle.header.parentSession === undefined
                ? {}
                : { parentSessionId: handle.header.parentSession }),
        };
    }
    finally {
        await handle.close();
    }
}
function unavailableSubagentStatus(parentSessionId, status) {
    return {
        ...(status ?? { available: false, reason: "none" }),
        available: false,
        target: "subagent",
        parentSessionId,
    };
}
async function inspectSubagentStatus(ctx, sessionId, state, signal) {
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
    const entries = await awaitWithSignal(ctx.subagents.listChildren(parentSessionId, signal), signal);
    const targetEntry = entries.find((entry) => entry.id === sessionId);
    if (targetEntry === undefined || !isContinuableChild(targetEntry)) {
        return unavailableSubagentStatus(parentSessionId);
    }
    let targetStatus;
    let selected;
    for (let order = 0; order < entries.length; order += 1) {
        const entry = entries[order];
        if (entry === undefined || !isContinuableChild(entry))
            continue;
        const candidateState = await readSessionState(ctx, entry.id, signal);
        if (candidateState === undefined)
            continue;
        const candidateStatus = continueStatusFromEvents(candidateState.events, candidateState.agent, candidateState.inheritedEventCount);
        const candidateBoundary = latestTurnBoundary(candidateState.events, candidateState.inheritedEventCount);
        if (entry.id === sessionId) {
            targetStatus = candidateState.agent !== undefined
                && patchStateOf(candidateState.agent) === undefined
                ? { ...candidateStatus, available: false }
                : candidateStatus;
        }
        if (candidateBoundary?.available !== true)
            continue;
        const eventTime = boundaryTime(candidateState.events, candidateState.inheritedEventCount);
        if (selected === undefined
            || eventTime > selected.eventTime
            || (eventTime === selected.eventTime && order > selected.order)) {
            selected = { id: entry.id, eventTime, order };
        }
    }
    const status = targetStatus ?? { available: false, reason: "none" };
    if (status.available && selected?.id === sessionId) {
        return {
            ...status,
            target: "subagent",
            parentSessionId,
        };
    }
    return unavailableSubagentStatus(parentSessionId, status);
}
async function inspectStatus(ctx, sessionId, signal) {
    const state = await readSessionState(ctx, sessionId, signal);
    if (state === undefined)
        return { available: false, reason: "none", target: "session" };
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
function resolveSessionAgentPreset(header, events) {
    for (let index = events.length - 1; index >= 0; index -= 1) {
        const event = events[index];
        if (event?.type === "agent-preset/selected")
            return event.data.agentPreset;
    }
    return header.agentPreset;
}
async function resolveLiveAgent(ctx, runtime, sessionId, signal) {
    const existing = ctx.agents.get(sessionId);
    if (existing !== undefined)
        return { agent: existing };
    if (signal.aborted)
        return cancelled();
    const state = await readSessionState(ctx, sessionId, signal);
    if (state === undefined)
        return internal("找不到可恢复的会话。");
    const presetId = resolveSessionAgentPreset(state.header, state.events);
    const defaultSelection = ctx.agentDefaultModel.currentSelection();
    const loggedSelection = foldRequestHeader(state.events)?.config;
    const initialSelection = loggedSelection === undefined
        ? defaultSelection
        : {
            provider: loggedSelection.provider,
            model: loggedSelection.model,
            ...(loggedSelection.reasoningEffort === undefined
                ? {}
                : { reasoningEffort: loggedSelection.reasoningEffort }),
        };
    let publishedHandle;
    let disposal;
    let disposeRequested = false;
    let accepted = false;
    const disposeOwned = async () => {
        disposeRequested = true;
        signal.removeEventListener("abort", cancelUnaccepted);
        if (publishedHandle !== undefined && disposal === undefined) {
            disposal = publishedHandle.dispose();
        }
        if (disposal !== undefined)
            await disposal;
    };
    const cancelUnaccepted = () => {
        if (!accepted)
            void disposeOwned();
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
            if (runtime.shuttingDown)
                throw new Error("dsh-continue is shutting down");
            const resumed = agentCtx.agent;
            if (resumed === undefined)
                throw new Error("dsh-continue: resumed Agent scope is missing");
            let picked;
            const selection = {
                get current() {
                    if (picked !== undefined)
                        return picked;
                    const logged = resumed.session.requestHeader()?.config;
                    if (logged === undefined)
                        return ctx.agentDefaultModel.currentSelection();
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
            let disposeAfterRequest;
            disposeAfterRequest = agentCtx.on("agent/request", async (_payload, next) => {
                try {
                    return await next();
                }
                finally {
                    disposeAfterRequest?.();
                    disposeAfterRequest = undefined;
                    disposeSelection();
                }
            });
            await ctx.agentPresets.mount(agentCtx, presetId);
            return {
                commit() {
                    signal.throwIfAborted();
                    if (runtime.shuttingDown)
                        throw new Error("dsh-continue is shutting down");
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
    }
    catch (error) {
        if (signal.aborted || runtime.shuttingDown) {
            void disposeOwned();
            return cancelled();
        }
        signal.removeEventListener("abort", cancelUnaccepted);
        const raced = ctx.agents.get(sessionId);
        if (raced !== undefined)
            return { agent: raced };
        throw error;
    }
}
function boundaryStillMatches(advertised, current, request) {
    if (advertised.turn !== request.turn || advertised.boundarySeq !== request.boundarySeq) {
        return false;
    }
    if (current.turn !== request.turn)
        return false;
    if (current.boundarySeq === request.boundarySeq)
        return true;
    // Persistence may append the authoritative `interrupted` turn/end while a
    // cold Agent is being resumed. The turn identity and reason remain stable;
    // only the boundary sequence moves forward.
    return advertised.reason === "interrupted"
        && current.reason === "interrupted"
        && typeof current.boundarySeq === "number"
        && current.boundarySeq > request.boundarySeq;
}
function expectedBoundaryEvent(events, inheritedEventCount, expected, deferredTurns = 0) {
    if (expected.available !== true
        || expected.reason === "none"
        || expected.turn === undefined
        || expected.boundarySeq === undefined) {
        return undefined;
    }
    const start = Math.max(0, Math.min(inheritedEventCount, events.length));
    const boundaries = events.slice(start).filter((event) => event.type === "turn/start" || event.type === "turn/end");
    let matchedIndex = -1;
    for (let index = boundaries.length - 1; index >= 0; index -= 1) {
        const event = boundaries[index];
        if (event === undefined)
            continue;
        const status = statusFromBoundaryEvent(event);
        if (status?.turn !== expected.turn || status.reason !== expected.reason)
            continue;
        const exact = event.seq === expected.boundarySeq;
        const repairedInterrupted = expected.reason === "interrupted"
            && event.seq > expected.boundarySeq;
        if (exact || repairedInterrupted) {
            matchedIndex = index;
            break;
        }
    }
    if (matchedIndex < 0)
        return undefined;
    const matched = boundaries[matchedIndex];
    if (matched === undefined)
        return undefined;
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
    if (remaining.length === 0)
        return matched;
    if (remaining.length === 1) {
        const current = remaining[0];
        if (current?.type === "turn/start" && current.data.turn > previousTurn) {
            return matched;
        }
    }
    return undefined;
}
async function validateSubagentMarker(ctx, sessionId, expected, signal, deferredTurns) {
    if (signal.aborted || expected.target !== "subagent")
        return false;
    const parentSessionId = expected.parentSessionId;
    if (parentSessionId === undefined || ctx.agents.get(parentSessionId) === undefined)
        return false;
    const entries = await awaitWithSignal(ctx.subagents.listChildren(parentSessionId, signal), signal);
    const targetEntry = entries.find((entry) => entry.id === sessionId);
    if (targetEntry === undefined || !isContinuableChild(targetEntry))
        return false;
    let selected;
    for (let order = 0; order < entries.length; order += 1) {
        const entry = entries[order];
        if (entry === undefined || !isContinuableChild(entry))
            continue;
        const state = await readSessionState(ctx, entry.id, signal);
        if (state === undefined)
            continue;
        const event = entry.id === sessionId
            ? expectedBoundaryEvent(state.events, state.inheritedEventCount, expected, deferredTurns)
            : latestTurnBoundaryEvent(state.events, state.inheritedEventCount);
        if (event === undefined) {
            if (entry.id === sessionId)
                return false;
            continue;
        }
        const status = statusFromBoundaryEvent(event);
        if (status?.available !== true)
            continue;
        const eventTime = event.time;
        if (selected === undefined
            || eventTime > selected.eventTime
            || (eventTime === selected.eventTime && order > selected.order)) {
            selected = { id: entry.id, eventTime, order };
        }
    }
    return selected?.id === sessionId;
}
async function resumeSubagent(ctx, runtime, sessionId, advertised, request, signal) {
    if (advertised.target !== "subagent")
        return busy("当前目标不是 subagent 会话。");
    const current = await inspectStatus(ctx, sessionId, signal);
    if (!current.available
        || current.target !== "subagent"
        || !boundaryStillMatches(advertised, current, request)) {
        return busy("subagent 状态已经变化，请重新读取后再试。");
    }
    const parentSessionId = current.parentSessionId;
    if (parentSessionId === undefined)
        return busy("subagent 的父会话当前不可用。");
    const parent = ctx.agents.get(parentSessionId);
    if (parent === undefined)
        return busy("subagent 的父会话当前不在线。");
    const child = ctx.agents.get(sessionId);
    if (child !== undefined) {
        if (patchStateOf(child) === undefined) {
            return busy("当前 DSH 版本不支持无消息续跑。");
        }
        const liveStatus = continueStatusFromEvents(child.session.snapshotEvents(), child, child.session.inheritedEventCount);
        if (!liveStatus.available || !boundaryStillMatches(advertised, liveStatus, request)) {
            return busy("subagent 状态已经变化，请重新读取后再试。");
        }
    }
    if (signal.aborted || runtime.shuttingDown)
        return cancelled();
    const wakeSourceId = rpcId(SUBAGENT_WAKE_PREFIX);
    const markerId = String(wakeSourceId);
    const marker = runtime.markers.track(markerId, String(sessionId), (deferredTurns) => validateSubagentMarker(ctx, sessionId, current, signal, deferredTurns));
    const cancelOnAbort = () => marker.cancel();
    signal.addEventListener("abort", cancelOnAbort, { once: true });
    let admitted = false;
    let admittedChild = child;
    try {
        await awaitWithSignal(queueHostSubagentPrompt(ctx.subagents, parent, sessionId, [], {
            kind: "user",
            rpcId: wakeSourceId,
        }, signal), signal);
        admitted = true;
        admittedChild = ctx.agents.get(sessionId) ?? admittedChild;
        const outcome = await awaitWithSignal(marker.outcome, signal);
        if (outcome !== "consumed") {
            const removed = admittedChild === undefined
                ? 0
                : removeWakeByRpcId(admittedChild, markerId);
            if (removed > 0)
                runtime.markers.finish([markerId]);
            return cancelled();
        }
        return { ok: true, value: { accepted: true } };
    }
    catch (error) {
        marker.cancel();
        const removed = admittedChild === undefined
            ? 0
            : removeWakeByRpcId(admittedChild, markerId);
        if (removed > 0 || (!admitted && !signal.aborted && !runtime.shuttingDown)) {
            runtime.markers.finish([markerId]);
        }
        if (signal.aborted || runtime.shuttingDown)
            return cancelled();
        try {
            ctx.logger.warn(`dsh-continue: subagent followup failed: ${String(error)}`);
        }
        catch {
            // Diagnostics must not affect the RPC response.
        }
        return busy("subagent 状态已经变化，请稍后重新读取。");
    }
    finally {
        signal.removeEventListener("abort", cancelOnAbort);
        if (!admitted)
            marker.cancel();
    }
}
export function createContinueRpcHandler(ctx) {
    const runtime = RUNTIME_STATES.get(ctx) ?? createRuntimeState();
    const inFlight = new Set();
    const parentInFlight = new Set();
    const handle = async (endpoint, payload, signal, _peer) => {
        if (endpoint === ENDPOINT_STATUS) {
            const rawSessionId = decodeStatusPayload(payload);
            if (rawSessionId === undefined)
                return badRequest("dsh-continue: invalid status payload");
            try {
                return {
                    ok: true,
                    value: await inspectStatus(ctx, SessionId(rawSessionId), signal),
                };
            }
            catch (error) {
                if (signal.aborted || runtime.shuttingDown)
                    return cancelled();
                try {
                    ctx.logger.warn(`dsh-continue: status inspection failed: ${String(error)}`);
                }
                catch {
                    // Diagnostics must not affect the RPC response.
                }
                return internal("无法读取当前会话状态。");
            }
        }
        if (endpoint !== ENDPOINT_RESUME) {
            return badRequest(`dsh-continue: unknown endpoint ${JSON.stringify(endpoint)}`);
        }
        const request = decodeResumePayload(payload);
        if (request === undefined)
            return badRequest("dsh-continue: invalid resume payload");
        if (inFlight.has(request.sessionId))
            return busy("已有续跑请求正在处理。");
        inFlight.add(request.sessionId);
        try {
            const sessionId = SessionId(request.sessionId);
            const advertised = await inspectStatus(ctx, sessionId, signal);
            if (signal.aborted)
                return cancelled();
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
                if (parentKey === undefined)
                    return busy("subagent 的父会话当前不可用。");
                if (parentInFlight.has(parentKey))
                    return busy("该父会话已有 subagent 续跑请求正在处理。");
                parentInFlight.add(parentKey);
                try {
                    return await resumeSubagent(ctx, runtime, sessionId, advertised, request, signal);
                }
                finally {
                    parentInFlight.delete(parentKey);
                }
            }
            const resolved = await resolveLiveAgent(ctx, runtime, sessionId, signal);
            if (isRecord(resolved) && "ok" in resolved) {
                return resolved;
            }
            const resolution = resolved;
            const liveAgent = resolution.agent;
            const disposeColdAgent = async () => {
                if (resolution.dispose === undefined)
                    return;
                try {
                    await resolution.dispose();
                }
                catch {
                    // Agent ownership also rolls back with the plugin fiber.
                }
            };
            const current = continueStatusFromEvents(liveAgent.session.snapshotEvents(), liveAgent, liveAgent.session.inheritedEventCount);
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
        }
        catch (error) {
            if (signal.aborted || runtime.shuttingDown)
                return cancelled();
            try {
                ctx.logger.warn(`dsh-continue: resume failed: ${String(error)}`);
            }
            catch {
                // Diagnostics must not affect the RPC response.
            }
            return internal("续跑请求未能启动。");
        }
        finally {
            inFlight.delete(request.sessionId);
        }
    };
    return async (endpoint, payload, signal, peer) => {
        if (runtime.shuttingDown)
            return cancelled();
        let settleRequest;
        const activeRequest = new Promise((resolve) => {
            settleRequest = resolve;
        });
        runtime.activeRequests.add(activeRequest);
        try {
            if (runtime.shuttingDown)
                return cancelled();
            const operationSignal = AbortSignal.any([signal, runtime.shutdown.signal]);
            try {
                return await awaitWithSignal(handle(endpoint, payload, operationSignal, peer), operationSignal);
            }
            catch (error) {
                if (operationSignal.aborted)
                    return cancelled();
                throw error;
            }
        }
        finally {
            runtime.activeRequests.delete(activeRequest);
            settleRequest();
        }
    };
}
export function apply(ctx) {
    const runtime = createRuntimeState();
    const patches = new Map();
    RUNTIME_STATES.set(ctx, runtime);
    const releasePatch = (agent, state) => {
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
        if (RUNTIME_STATES.get(ctx) === runtime)
            RUNTIME_STATES.delete(ctx);
    }, "dsh-continue: restore Agent wake patches");
    const install = ({ agent }) => {
        if (runtime.shuttingDown)
            return;
        const state = installAgentPatch(agent, ctx, runtime.markers);
        if (state !== undefined)
            patches.set(agent, state);
        cleanupLegacyWakesWhenIdle(agent);
    };
    const release = ({ agent }) => {
        const state = patches.get(agent);
        if (state === undefined)
            return;
        patches.delete(agent);
        releasePatch(agent, state);
    };
    // `agent/created` is async-serial: the listener must resolve to `undefined`,
    // and returning it keeps the serial initialization handoff well-typed.
    for (const agent of ctx.agents.list())
        install({ agent });
    ctx.on("agent/created", ({ agent }) => {
        install({ agent });
        return undefined;
    });
    ctx.on("agent/status", ({ agent, status }) => {
        if (status === "idle")
            cleanupLegacyWakesWhenIdle(agent);
    });
    ctx.on("agent/disposed", release);
    // Registered last so shutdown first closes admission, settles every entered
    // request, and only then lets Cordis dispose the web route and Agent patches.
    ctx.effect(() => {
        const connection = ctx.connection;
        const handler = createContinueRpcHandler(ctx);
        const disposeRoute = ctx.webServer.register({
            kind: "prefix",
            path: RPC_CHANNEL,
            handler: (req, res) => {
                void serveChannelRpc(connection, handler, req, res);
            },
        });
        return async () => {
            beginShutdown(runtime);
            disposeRoute();
            await Promise.allSettled([...runtime.activeRequests]);
        };
    }, "dsh-continue: web route");
}
// --- /dsh-continue web transport --------------------------------------------
// dsh 0.1.5 registers web routes on the webServer service; the connection
// service contributes the trust/auth fence. The wire envelope mirrors the
// shared transport: a `client-request` JSON object in, a `server-response`
// JSON object carrying the ConnectionRpcResult out.
const ENDPOINT_SEGMENT_PATTERN = /^[A-Za-z0-9_$.-]+$/;
const MAX_REQUEST_BODY_BYTES = 300 * 1024 * 1024;
function channelEndpoint(pathname) {
    if (!pathname.startsWith(`${RPC_CHANNEL}/`))
        return undefined;
    const endpoint = pathname.slice(RPC_CHANNEL.length + 1);
    if (endpoint.split("/").some((segment) => segment === "" || segment === "." || segment === ".."
        || !ENDPOINT_SEGMENT_PATTERN.test(segment))) {
        return undefined;
    }
    return endpoint;
}
function rpcResponse(rpcId, result) {
    return JSON.stringify({ type: "server-response", rpcId, result });
}
function invalidRequestResponse(body) {
    const rpcId = isRecord(body) && typeof body.rpcId === "string" ? body.rpcId : "invalid-request";
    return rpcResponse(rpcId, {
        ok: false,
        error: { code: "gateway/bad-request", message: "invalid client-request message", details: {} },
    });
}
function writeJson(res, body) {
    res.writeHead(200, { "content-type": "application/json" });
    res.end(body);
}
async function readRequestBody(req, signal) {
    const declared = req.headers["content-length"];
    if (declared !== undefined && Number(declared) > MAX_REQUEST_BODY_BYTES)
        return undefined;
    const chunks = [];
    let received = 0;
    for await (const chunk of req) {
        if (signal.aborted)
            return undefined;
        chunks.push(chunk);
        received += chunk.byteLength;
        if (received > MAX_REQUEST_BODY_BYTES)
            return undefined;
    }
    return Buffer.concat(chunks).toString("utf8");
}
async function serveChannelRpc(connection, handler, req, res) {
    const rejection = connection.requestRejection({ headers: req.headers });
    if (rejection !== undefined) {
        res.writeHead(rejection);
        res.end(rejection === 401 ? "unauthorized" : "forbidden");
        return;
    }
    const abort = new AbortController();
    res.on("close", () => {
        if (!res.writableEnded)
            abort.abort();
    });
    const url = new URL(req.url ?? "/", "http://dsh.internal");
    const endpoint = channelEndpoint(url.pathname);
    if (req.method !== "POST" || endpoint === undefined) {
        res.writeHead(404);
        res.end("not found");
        return;
    }
    if (req.headers["content-type"]?.split(";", 1)[0]?.trim().toLowerCase() !== "application/json") {
        res.writeHead(415);
        res.end("content type must be application/json");
        return;
    }
    const bodyText = await readRequestBody(req, abort.signal);
    if (bodyText === undefined || abort.signal.aborted)
        return;
    let body;
    try {
        body = JSON.parse(bodyText);
    }
    catch {
        res.writeHead(400);
        res.end("body is not JSON");
        return;
    }
    const message = isRecord(body)
        && body.type === "client-request"
        && typeof body.rpcId === "string"
        && typeof body.method === "string"
        && "payload" in body
        ? body
        : undefined;
    if (message === undefined) {
        writeJson(res, invalidRequestResponse(body));
        return;
    }
    if (message.method !== endpoint) {
        writeJson(res, rpcResponse(message.rpcId, {
            ok: false,
            error: {
                code: "gateway/bad-request",
                message: `method ${JSON.stringify(message.method)} does not match endpoint ${JSON.stringify(endpoint)}`,
                details: {},
            },
        }));
        return;
    }
    let result;
    try {
        result = await handler(endpoint, message.payload, abort.signal, connection.operator);
    }
    catch (error) {
        res.writeHead(500);
        res.end(`handler failure: ${String(error)}`);
        return;
    }
    writeJson(res, rpcResponse(message.rpcId, result));
}
//# sourceMappingURL=runtime.js.map