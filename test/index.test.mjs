import assert from "node:assert/strict";
import test from "node:test";
import {
  apply,
  clearLegacyContinuationWakes,
  createContinueRpcHandler,
  continueStatusFromEvents,
  startContinuation,
} from "../lib/index.js";
import { Context } from "@deepseek-ai/cordis";
import { Session, SessionId } from "@deepseek-ai/dsh-session";

function sessionWithReason(reason) {
  const session = Session.create(SessionId(`test-${Math.random().toString(16).slice(2)}`));
  session.append("turn/start", { turn: 1 });
  session.append("turn/end", { turn: 1, reason });
  return session;
}

function fakeAgent({
  id = SessionId(`fake-${Math.random().toString(16).slice(2)}`),
  pending = false,
  session = {},
  phaseKind = "idle",
  preStepMessages = [],
} = {}) {
  let publicStatus = phaseKind === "running" ? "running" : "idle";
  let resolveIdle;
  let idlePromise = Promise.resolve();
  const originalPreStep = async () => ({ kind: "enter", messages: [...preStepMessages] });
  const inbox = {
    hasPending: pending,
    nextStep: [],
    nextTurn: preStepMessages,
    remove(id) {
      for (const list of [inbox.nextStep, inbox.nextTurn]) {
        const index = list.findIndex((message) => message.id === id);
        if (index >= 0) {
          list.splice(index, 1);
          return true;
        }
      }
      return false;
    },
    prepend(target, message) {
      const list = target === "next-turn" ? inbox.nextTurn : inbox.nextStep;
      list.unshift(message);
    },
  };
  const agent = {
    id,
    phase: { kind: phaseKind },
    get status() {
      return publicStatus;
    },
    inbox,
    session,
    preStep: originalPreStep,
    wakeDriver() {
      assert.equal(agent.phase.kind, "idle");
      publicStatus = "running";
      agent.phase = { kind: "running" };
      idlePromise = new Promise((resolve) => {
        resolveIdle = resolve;
      });
      queueMicrotask(() => {
        publicStatus = "idle";
        agent.phase = { kind: "idle" };
        resolveIdle();
      });
    },
    whenIdle() {
      return idlePromise;
    },
  };
  return { agent, originalPreStep };
}

function event(seq, type, data, time = seq) {
  return { seq, time, type, data };
}

function legacyWake(id) {
  return {
    id,
    role: "user",
    content: [],
    source: { kind: "user", rpcId: `dsh-continue:${id}` },
  };
}

function subagentWake(id = "subagent-wake") {
  return {
    id,
    role: "user",
    content: [],
    source: { kind: "user", rpcId: `dsh-continue:subagent:${id}` },
  };
}

function provideSubagents(ctx, overrides = {}) {
  ctx.provide("subagents", {
    listChildren: async () => [],
    followup: async () => "subagent-message",
    registerContinuableSetup: () => () => {},
    ...overrides,
  });
}
function childSession(id, parentId, suffix) {
  return {
    header: {
      id,
      parentSession: parentId,
      seedLength: 1,
      origin: "subagent",
    },
    events: [event(0, "turn/end", {
      turn: 99,
      reason: { kind: "completed" },
    }), ...suffix],
  };
}

test("classifies abnormal turn endings as continuable", () => {
  assert.equal(continueStatusFromEvents(sessionWithReason({
    kind: "error",
    error: { message: "upstream", code: "UPSTREAM" },
  }).events).available, true);
  assert.equal(continueStatusFromEvents(sessionWithReason({ kind: "interrupted" }).events).reason, "interrupted");
  assert.equal(continueStatusFromEvents(sessionWithReason({ kind: "max-tokens" }).events).reason, "max-tokens");
  assert.equal(continueStatusFromEvents(sessionWithReason({
    kind: "aborted",
    reason: { kind: "disposed" },
  }).events).reason, "disposed");
});

test("completed and user-aborted turns are not continuable", () => {
  assert.equal(continueStatusFromEvents(sessionWithReason({ kind: "completed" }).events).available, false);
  assert.equal(continueStatusFromEvents(sessionWithReason({
    kind: "aborted",
    reason: { kind: "user" },
  }).events).available, false);
});

test("an open turn is treated as a crash tail", () => {
  const session = Session.create(SessionId("open-turn"));
  session.append("turn/start", { turn: 3 });
  const status = continueStatusFromEvents(session.events);
  assert.equal(status.available, true);
  assert.equal(status.reason, "interrupted");
  assert.equal(status.turn, 3);
});

test("legacy empty continuation wakes are ignored and removable", () => {
  const removed = [];
  const agent = {
    inbox: {
      nextStep: [legacyWake("step-wake"), subagentWake("subagent-wake")],
      nextTurn: [legacyWake("turn-wake"), {
        id: "ordinary",
        role: "user",
        content: [{ type: "text", text: "keep" }],
        source: { kind: "user" },
      }],
      remove(id) {
        removed.push(id);
        return true;
      },
    },
  };
  assert.equal(clearLegacyContinuationWakes(agent), 3);
  assert.deepEqual(removed, ["step-wake", "subagent-wake", "turn-wake"]);
});

test("cold inbox replay starts after the persisted seed boundary", () => {
  const events = [
    event(0, "agent/inbox/spliced", {
      target: "next-turn",
      start: 0,
      inserted: [{ id: "seed-message" }],
    }),
    event(1, "turn/start", { turn: 1 }),
    event(2, "turn/end", {
      turn: 1,
      reason: { kind: "error", error: { message: "x", code: "X" } },
    }),
    event(3, "agent/inbox/spliced", {
      target: "next-turn",
      start: 0,
      inserted: [legacyWake("old-wake")],
    }),
  ];
  assert.equal(continueStatusFromEvents(events, undefined, 0).available, false);
  assert.equal(continueStatusFromEvents(events, undefined, 1).available, true);
});

test("only the latest abnormal continuable subagent can be resumed", async () => {
  const parentId = SessionId("parent-session");
  const olderId = SessionId("child-older");
  const latestId = SessionId("child-latest");
  const parent = fakeAgent({
    id: parentId,
    session: { header: { id: parentId }, events: [] },
  }).agent;
  const older = fakeAgent({
    id: olderId,
    session: childSession(olderId, parentId, [
      event(1, "turn/start", { turn: 1 }, 100),
      event(2, "turn/end", {
        turn: 1,
        reason: { kind: "error", error: { message: "old", code: "OLD" } },
      }, 110),
    ]),
  }).agent;
  const latestMessages = [];
  const latest = fakeAgent({
    id: latestId,
    preStepMessages: latestMessages,
    session: childSession(latestId, parentId, [
      event(1, "turn/start", { turn: 2 }, 200),
      event(2, "turn/end", {
        turn: 2,
        reason: { kind: "interrupted" },
      }, 210),
    ]),
  }).agent;
  const agents = new Map([
    [String(parentId), parent],
    [String(olderId), older],
    [String(latestId), latest],
  ]);
  let followupArgs;
  let reportFollowup;
  const followupAccepted = new Promise((resolve) => {
    reportFollowup = resolve;
  });
  const ctx = new Context();
  ctx.provide("agents", {
    list: () => [parent, older, latest],
    get: (id) => agents.get(String(id)),
  });
  ctx.provide("connection", { rpc: { handle: () => () => {} } });
  ctx.provide("apiProxy", { sessions: {} });
  provideSubagents(ctx, {
    listChildren: async () => [
      { kind: "child", id: olderId, activity: "inactive", hasChildren: false, mode: "continuable", label: "old" },
      { kind: "child", id: latestId, activity: "inactive", hasChildren: false, mode: "continuable", label: "latest" },
    ],
    followup: async (...args) => {
      followupArgs = args;
      latestMessages.push({
        id: "accepted-marker",
        role: "user",
        content: [],
        source: args[3].source,
      });
      reportFollowup();
      return "accepted-marker";
    },
  });
  ctx.provide("logger", { warn: () => {} });
  apply(ctx);

  const handler = createContinueRpcHandler(ctx);
  const signal = new AbortController().signal;
  const oldStatus = await handler("status", { sessionId: String(olderId) }, signal);
  const latestStatus = await handler("status", { sessionId: String(latestId) }, signal);
  assert.equal(oldStatus.ok, true);
  assert.equal(oldStatus.value.available, false);
  assert.equal(latestStatus.ok, true);
  assert.equal(latestStatus.value.available, true);
  assert.equal(latestStatus.value.target, "subagent");

  const resultPromise = handler("resume", {
    sessionId: String(latestId),
    turn: latestStatus.value.turn,
    boundarySeq: latestStatus.value.boundarySeq,
  }, signal);
  await followupAccepted;
  let settled = false;
  void resultPromise.then(() => {
    settled = true;
  });
  await Promise.resolve();
  assert.equal(settled, false);
  await latest.preStep("next-turn", { turn: 3, step: 1 });
  latestMessages.length = 0;
  const result = await resultPromise;
  assert.deepEqual(result, { ok: true, value: { accepted: true } });
  assert.equal(followupArgs[0], parent);
  assert.equal(String(followupArgs[1]), String(latestId));
  assert.deepEqual(followupArgs[2], []);
  assert.equal(followupArgs[3].source.kind, "user");
  assert.equal(String(followupArgs[3].source.rpcId).startsWith("dsh-continue:subagent:"), true);
  await ctx.fiber.dispose();
});

test("a persisted marker and its recovery marker open exactly one subagent turn", async () => {
  const parentId = SessionId("recovery-parent");
  const childId = SessionId("recovery-child");
  const parent = fakeAgent({
    id: parentId,
    session: { header: { id: parentId }, events: [] },
  }).agent;
  const preStepMessages = [subagentWake("persisted")];
  const { agent: child } = fakeAgent({
    id: childId,
    preStepMessages,
    session: childSession(childId, parentId, [
      event(1, "turn/start", { turn: 2 }),
      event(2, "turn/end", { turn: 2, reason: { kind: "interrupted" } }),
    ]),
  });
  child.inbox.nextStep = [];
  child.inbox.nextTurn = preStepMessages;
  const removed = [];
  child.inbox.remove = (id) => {
    const before = child.inbox.nextTurn.length;
    child.inbox.nextTurn = child.inbox.nextTurn.filter((message) => message.id !== id);
    if (before === child.inbox.nextTurn.length) return false;
    removed.push(id);
    return true;
  };
  const agents = new Map([
    [String(parentId), parent],
    [String(childId), child],
  ]);
  let reportFollowup;
  const followupAccepted = new Promise((resolve) => {
    reportFollowup = resolve;
  });
  const ctx = new Context();
  ctx.provide("agents", {
    list: () => [parent, child],
    get: (id) => agents.get(String(id)),
  });
  ctx.provide("connection", { rpc: { handle: () => () => {} } });
  ctx.provide("apiProxy", { sessions: {} });
  provideSubagents(ctx, {
    listChildren: async () => [
      { kind: "child", id: childId, activity: "inactive", hasChildren: false, mode: "continuable", label: "recovery" },
    ],
    followup: async (_parent, _childId, _content, options) => {
      child.inbox.nextTurn.push({
        id: "recovery-marker",
        role: "user",
        content: [],
        source: options.source,
      });
      reportFollowup();
      return "recovery-marker";
    },
  });
  ctx.provide("logger", { warn: () => {} });
  apply(ctx);

  const handler = createContinueRpcHandler(ctx);
  const signal = new AbortController().signal;
  const status = await handler("status", { sessionId: String(childId) }, signal);
  const resume = handler("resume", {
    sessionId: String(childId),
    turn: status.value.turn,
    boundarySeq: status.value.boundarySeq,
  }, signal);
  await followupAccepted;
  const decision = await child.preStep("next-turn", { turn: 3, step: 1 });
  preStepMessages.length = 0;
  const result = await resume;

  assert.deepEqual(result, { ok: true, value: { accepted: true } });
  assert.equal(decision.messages.length, 1);
  assert.deepEqual([...decision.messages], []);
  assert.deepEqual(removed, ["persisted", "recovery-marker"]);
  assert.deepEqual(child.inbox.nextTurn, []);
  await ctx.fiber.dispose();
});

test("a rejected subagent pre-step cancels and removes its exact marker", async () => {
  const parentId = SessionId("reject-parent");
  const childId = SessionId("reject-child");
  const parent = fakeAgent({
    id: parentId,
    session: { header: { id: parentId }, events: [] },
  }).agent;
  const { agent: child } = fakeAgent({
    id: childId,
    session: childSession(childId, parentId, [
      event(1, "turn/start", { turn: 1 }),
      event(2, "turn/end", { turn: 1, reason: { kind: "interrupted" } }),
    ]),
  });
  child.preStep = async () => ({ kind: "reject" });
  child.inbox.nextStep = [];
  child.inbox.nextTurn = [];
  const removed = [];
  child.inbox.remove = (id) => {
    const before = child.inbox.nextTurn.length;
    child.inbox.nextTurn = child.inbox.nextTurn.filter((message) => message.id !== id);
    if (before === child.inbox.nextTurn.length) return false;
    removed.push(id);
    return true;
  };
  const agents = new Map([
    [String(parentId), parent],
    [String(childId), child],
  ]);
  let reportFollowup;
  const followupAccepted = new Promise((resolve) => {
    reportFollowup = resolve;
  });
  const ctx = new Context();
  ctx.provide("agents", {
    list: () => [parent, child],
    get: (id) => agents.get(String(id)),
  });
  ctx.provide("connection", { rpc: { handle: () => () => {} } });
  ctx.provide("apiProxy", { sessions: {} });
  provideSubagents(ctx, {
    listChildren: async () => [
      { kind: "child", id: childId, activity: "inactive", hasChildren: false, mode: "continuable", label: "reject" },
    ],
    followup: async (_parent, _childId, _content, options) => {
      child.inbox.nextTurn.push({
        id: "reject-marker",
        role: "user",
        content: [],
        source: options.source,
      });
      reportFollowup();
      return "reject-marker";
    },
  });
  ctx.provide("logger", { warn: () => {} });
  apply(ctx);

  const handler = createContinueRpcHandler(ctx);
  const signal = new AbortController().signal;
  const status = await handler("status", { sessionId: String(childId) }, signal);
  const resume = handler("resume", {
    sessionId: String(childId),
    turn: status.value.turn,
    boundarySeq: status.value.boundarySeq,
  }, signal);
  await followupAccepted;
  const decision = await child.preStep("next-turn", { turn: 2, step: 1 });
  const result = await resume;

  assert.deepEqual(decision, { kind: "reject" });
  assert.equal(result.ok, false);
  assert.equal(result.error.code, "cancelled");
  assert.deepEqual(removed, ["reject-marker"]);
  assert.deepEqual(child.inbox.nextTurn, []);
  await ctx.fiber.dispose();
});

test("an enter decision that already claimed the marker still settles its RPC", async () => {
  const parentId = SessionId("claimed-parent");
  const childId = SessionId("claimed-child");
  const parent = fakeAgent({
    id: parentId,
    session: { header: { id: parentId }, events: [] },
  }).agent;
  const { agent: child } = fakeAgent({
    id: childId,
    session: childSession(childId, parentId, [
      event(1, "turn/start", { turn: 1 }),
      event(2, "turn/end", { turn: 1, reason: { kind: "interrupted" } }),
    ]),
  });
  child.inbox.nextStep = [];
  child.inbox.nextTurn = [];
  child.inbox.remove = (id) => {
    const before = child.inbox.nextTurn.length;
    child.inbox.nextTurn = child.inbox.nextTurn.filter((message) => message.id !== id);
    return before !== child.inbox.nextTurn.length;
  };
  child.preStep = async () => {
    child.inbox.nextTurn = [];
    return { kind: "enter", messages: [] };
  };
  const agents = new Map([
    [String(parentId), parent],
    [String(childId), child],
  ]);
  let reportFollowup;
  const followupAccepted = new Promise((resolve) => {
    reportFollowup = resolve;
  });
  const ctx = new Context();
  ctx.provide("agents", {
    list: () => [parent, child],
    get: (id) => agents.get(String(id)),
  });
  ctx.provide("connection", { rpc: { handle: () => () => {} } });
  ctx.provide("apiProxy", { sessions: {} });
  provideSubagents(ctx, {
    listChildren: async () => [
      { kind: "child", id: childId, activity: "inactive", hasChildren: false, mode: "continuable", label: "claimed" },
    ],
    followup: async (_parent, _childId, _content, options) => {
      child.inbox.nextTurn.push({
        id: "claimed-marker",
        role: "user",
        content: [],
        source: options.source,
      });
      reportFollowup();
      return "claimed-marker";
    },
  });
  ctx.provide("logger", { warn: () => {} });
  apply(ctx);

  const handler = createContinueRpcHandler(ctx);
  const signal = new AbortController().signal;
  const status = await handler("status", { sessionId: String(childId) }, signal);
  const resume = handler("resume", {
    sessionId: String(childId),
    turn: status.value.turn,
    boundarySeq: status.value.boundarySeq,
  }, signal);
  await followupAccepted;
  const decision = await child.preStep("next-turn", { turn: 2, step: 1 });
  const result = await resume;

  assert.deepEqual(result, { ok: true, value: { accepted: true } });
  assert.equal(decision.messages.length, 1);
  assert.deepEqual([...decision.messages], []);
  assert.deepEqual(child.inbox.nextTurn, []);
  await ctx.fiber.dispose();
});

test("a newer sibling failure cancels the marker at the model-step boundary", async () => {
  const parentId = SessionId("gate-parent");
  const targetId = SessionId("gate-target");
  const siblingId = SessionId("gate-sibling");
  const parent = fakeAgent({
    id: parentId,
    session: { header: { id: parentId }, events: [] },
  }).agent;
  const targetMessages = [];
  const target = fakeAgent({
    id: targetId,
    preStepMessages: targetMessages,
    session: childSession(targetId, parentId, [
      event(1, "turn/start", { turn: 1 }, 200),
      event(2, "turn/end", { turn: 1, reason: { kind: "interrupted" } }, 210),
    ]),
  }).agent;
  const sibling = fakeAgent({
    id: siblingId,
    session: childSession(siblingId, parentId, [
      event(1, "turn/start", { turn: 1 }, 100),
      event(2, "turn/end", {
        turn: 1,
        reason: { kind: "error", error: { message: "older", code: "OLDER" } },
      }, 110),
    ]),
  }).agent;
  const agents = new Map([
    [String(parentId), parent],
    [String(targetId), target],
    [String(siblingId), sibling],
  ]);
  let reportFollowup;
  const followupAccepted = new Promise((resolve) => {
    reportFollowup = resolve;
  });
  const entries = [
    { kind: "child", id: targetId, activity: "inactive", hasChildren: false, mode: "continuable", label: "target" },
    { kind: "child", id: siblingId, activity: "inactive", hasChildren: false, mode: "continuable", label: "sibling" },
  ];
  const ctx = new Context();
  ctx.provide("agents", {
    list: () => [parent, target, sibling],
    get: (id) => agents.get(String(id)),
  });
  ctx.provide("connection", { rpc: { handle: () => () => {} } });
  ctx.provide("apiProxy", { sessions: {} });
  provideSubagents(ctx, {
    listChildren: async () => entries,
    followup: async (_parent, _childId, _content, options) => {
      targetMessages.push({
        id: "gate-marker",
        role: "user",
        content: [],
        source: options.source,
      });
      reportFollowup();
      return "gate-marker";
    },
  });
  ctx.provide("logger", { warn: () => {} });
  apply(ctx);

  const handler = createContinueRpcHandler(ctx);
  const signal = new AbortController().signal;
  const status = await handler("status", { sessionId: String(targetId) }, signal);
  assert.equal(status.value.available, true);
  const resume = handler("resume", {
    sessionId: String(targetId),
    turn: status.value.turn,
    boundarySeq: status.value.boundarySeq,
  }, signal);
  await followupAccepted;
  sibling.session.events.push(
    event(3, "turn/start", { turn: 2 }, 500),
    event(4, "turn/end", {
      turn: 2,
      reason: { kind: "error", error: { message: "newer", code: "NEWER" } },
    }, 510),
  );
  const decision = await target.preStep("next-turn", { turn: 2, step: 1 });
  targetMessages.length = 0;
  const result = await resume;

  assert.deepEqual(decision, { kind: "reject" });
  assert.equal(result.ok, false);
  assert.equal(result.error.code, "cancelled");
  await ctx.fiber.dispose();
});

test("an intervening target turn cancels the queued continuation marker", async () => {
  const parentId = SessionId("intervening-parent");
  const childId = SessionId("intervening-child");
  const parent = fakeAgent({
    id: parentId,
    session: { header: { id: parentId }, events: [] },
  }).agent;
  const messages = [];
  const child = fakeAgent({
    id: childId,
    preStepMessages: messages,
    session: childSession(childId, parentId, [
      event(1, "turn/start", { turn: 1 }, 100),
      event(2, "turn/end", { turn: 1, reason: { kind: "interrupted" } }, 110),
    ]),
  }).agent;
  const agents = new Map([
    [String(parentId), parent],
    [String(childId), child],
  ]);
  let reportFollowup;
  const followupAccepted = new Promise((resolve) => {
    reportFollowup = resolve;
  });
  const ctx = new Context();
  ctx.provide("agents", {
    list: () => [parent, child],
    get: (id) => agents.get(String(id)),
  });
  ctx.provide("connection", { rpc: { handle: () => () => {} } });
  ctx.provide("apiProxy", { sessions: {} });
  provideSubagents(ctx, {
    listChildren: async () => [
      { kind: "child", id: childId, activity: "inactive", hasChildren: false, mode: "continuable", label: "intervening" },
    ],
    followup: async (_parent, _childId, _content, options) => {
      messages.push({
        id: "intervening-marker",
        role: "user",
        content: [],
        source: options.source,
      });
      reportFollowup();
      return "intervening-marker";
    },
  });
  ctx.provide("logger", { warn: () => {} });
  apply(ctx);

  const handler = createContinueRpcHandler(ctx);
  const signal = new AbortController().signal;
  const status = await handler("status", { sessionId: String(childId) }, signal);
  const resume = handler("resume", {
    sessionId: String(childId),
    turn: status.value.turn,
    boundarySeq: status.value.boundarySeq,
  }, signal);
  await followupAccepted;
  child.session.events.push(
    event(3, "turn/start", { turn: 2 }, 200),
    event(4, "turn/end", { turn: 2, reason: { kind: "completed" } }, 210),
  );
  const decision = await child.preStep("next-turn", { turn: 3, step: 1 });
  messages.length = 0;
  const result = await resume;

  assert.deepEqual(decision, { kind: "reject" });
  assert.equal(result.ok, false);
  assert.equal(result.error.code, "cancelled");
  await ctx.fiber.dispose();
});

test("a busy latest abnormal subagent does not expose an older failure", async () => {
  const parentId = SessionId("busy-parent");
  const olderId = SessionId("busy-child-older");
  const latestId = SessionId("busy-child-latest");
  const parent = fakeAgent({
    id: parentId,
    session: { header: { id: parentId }, events: [] },
  }).agent;
  const older = fakeAgent({
    id: olderId,
    session: childSession(olderId, parentId, [
      event(1, "turn/start", { turn: 1 }, 100),
      event(2, "turn/end", {
        turn: 1,
        reason: { kind: "error", error: { message: "old", code: "OLD" } },
      }, 110),
    ]),
  }).agent;
  const latest = fakeAgent({
    id: latestId,
    phaseKind: "running",
    session: childSession(latestId, parentId, [
      event(1, "turn/start", { turn: 2 }, 200),
      event(2, "turn/end", { turn: 2, reason: { kind: "interrupted" } }, 210),
    ]),
  }).agent;
  const agents = new Map([
    [String(parentId), parent],
    [String(olderId), older],
    [String(latestId), latest],
  ]);
  const ctx = new Context();
  ctx.provide("agents", {
    list: () => [parent, older, latest],
    get: (id) => agents.get(String(id)),
  });
  ctx.provide("connection", { rpc: { handle: () => () => {} } });
  ctx.provide("apiProxy", { sessions: {} });
  provideSubagents(ctx, {
    listChildren: async () => [
      { kind: "child", id: olderId, activity: "inactive", hasChildren: false, mode: "continuable", label: "old" },
      { kind: "child", id: latestId, activity: "running", hasChildren: false, mode: "continuable", label: "latest" },
    ],
  });
  ctx.provide("logger", { warn: () => {} });
  apply(ctx);

  const handler = createContinueRpcHandler(ctx);
  const signal = new AbortController().signal;
  const olderStatus = await handler("status", { sessionId: String(olderId) }, signal);
  const latestStatus = await handler("status", { sessionId: String(latestId) }, signal);
  assert.equal(olderStatus.ok, true);
  assert.equal(olderStatus.value.available, false);
  assert.equal(latestStatus.ok, true);
  assert.equal(latestStatus.value.available, false);
  assert.equal(latestStatus.value.reason, "interrupted");
  await ctx.fiber.dispose();
});

test("a live subagent without the continuation boundary stays unavailable", async () => {
  const parentId = SessionId("cold-parent");
  const childId = SessionId("cold-child");
  const parent = fakeAgent({
    id: parentId,
    session: { header: { id: parentId }, events: [] },
  }).agent;
  const child = fakeAgent({
    id: childId,
    session: childSession(childId, parentId, [
      event(1, "turn/start", { turn: 1 }),
      event(2, "turn/end", { turn: 1, reason: { kind: "interrupted" } }),
    ]),
  }).agent;
  const ctx = new Context();
  ctx.provide("agents", {
    list: () => [parent, child],
    get: (id) => String(id) === String(parentId)
      ? parent
      : String(id) === String(childId) ? child : undefined,
  });
  ctx.provide("connection", { rpc: { handle: () => () => {} } });
  ctx.provide("apiProxy", { sessions: {} });
  provideSubagents(ctx, {
    listChildren: async () => [
      { kind: "child", id: childId, activity: "inactive", hasChildren: false, mode: "continuable", label: "cold" },
    ],
  });
  ctx.provide("logger", { warn: () => {} });

  const status = await createContinueRpcHandler(ctx)(
    "status",
    { sessionId: String(childId) },
    new AbortController().signal,
  );
  assert.equal(status.ok, true);
  assert.equal(status.value.available, false);
  assert.equal(status.value.target, "subagent");
  await ctx.fiber.dispose();
});

test("official continuable setup installs the boundary before publication", async () => {
  let setup;
  const ctx = new Context();
  ctx.provide("agents", { list: () => [], get: () => undefined });
  ctx.provide("connection", { rpc: { handle: () => () => {} } });
  ctx.provide("apiProxy", { sessions: {} });
  provideSubagents(ctx, {
    registerContinuableSetup: (contribution) => {
      setup = contribution;
      return () => {};
    },
  });
  ctx.provide("logger", { warn: () => {} });
  apply(ctx);

  const { agent } = fakeAgent();
  const release = setup({ agent });
  assert.equal(startContinuation(agent), true);
  await agent.whenIdle();
  release();
  await ctx.fiber.dispose();
});

test("ordinary fork sessions remain ordinary continuation targets", async () => {
  const parentId = SessionId("fork-parent");
  const forkId = SessionId("ordinary-fork");
  const fork = fakeAgent({
    id: forkId,
    session: {
      header: { id: forkId, parentSession: parentId, seedLength: 0 },
      events: [
        event(1, "turn/start", { turn: 1 }),
        event(2, "turn/end", {
          turn: 1,
          reason: { kind: "error", error: { message: "fork", code: "FORK" } },
        }),
      ],
    },
  }).agent;
  let catalogReads = 0;
  const ctx = new Context();
  ctx.provide("agents", {
    list: () => [fork],
    get: (id) => String(id) === String(forkId) ? fork : undefined,
  });
  ctx.provide("connection", { rpc: { handle: () => () => {} } });
  ctx.provide("apiProxy", { sessions: {} });
  provideSubagents(ctx, {
    listChildren: async () => {
      catalogReads += 1;
      return [];
    },
  });
  ctx.provide("logger", { warn: () => {} });
  apply(ctx);

  const status = await createContinueRpcHandler(ctx)(
    "status",
    { sessionId: String(forkId) },
    new AbortController().signal,
  );
  assert.equal(status.ok, true);
  assert.equal(status.value.available, true);
  assert.equal(status.value.target, "session");
  assert.equal(catalogReads, 0);
  await ctx.fiber.dispose();
});

test("empty continuation enters a model step without iterating a user message", async () => {
  const { agent, originalPreStep } = fakeAgent();
  const ctx = new Context();
  ctx.provide("agents", {
    list: () => [agent],
    get: () => agent,
  });
  ctx.provide("connection", {
    rpc: { handle: () => () => {} },
  });
  ctx.provide("apiProxy", { sessions: {} });
  ctx.provide("logger", { warn: () => {} });
  apply(ctx);

  assert.equal(startContinuation(agent), true);
  const decision = await agent.preStep("next-turn", { turn: 2, step: 1 });
  assert.equal(decision.kind, "enter");
  assert.equal(decision.messages.length, 1);
  assert.deepEqual([...decision.messages], []);
  assert.equal(agent.preStep === originalPreStep, false);

  await agent.whenIdle();
  await ctx.fiber.dispose();
  assert.equal(agent.preStep, originalPreStep);
});

test("a legacy session marker cannot become a visible user message", async () => {
  const messages = [];
  const { agent } = fakeAgent({ preStepMessages: messages });
  const ctx = new Context();
  ctx.provide("agents", { list: () => [agent], get: () => agent });
  ctx.provide("connection", { rpc: { handle: () => () => {} } });
  ctx.provide("apiProxy", { sessions: {} });
  provideSubagents(ctx);
  ctx.provide("logger", { warn: () => {} });
  apply(ctx);
  messages.push(legacyWake("pre-step-legacy"));

  const decision = await agent.preStep("next-turn", { turn: 2, step: 1 });
  assert.equal(decision.kind, "enter");
  assert.equal(decision.messages.length, 1);
  assert.deepEqual([...decision.messages], []);
  await ctx.fiber.dispose();
});

test("marker identity remains hidden when another pre-step contribution rewrites its shape", async () => {
  const original = subagentWake("rewritten-marker");
  const rewritten = {
    ...original,
    content: [{ type: "text", text: "rewritten control content" }],
    source: { kind: "user", rpcId: "ordinary" },
  };
  const { agent } = fakeAgent({ preStepMessages: [rewritten] });
  agent.inbox.nextStep = [];
  agent.inbox.nextTurn = [original];
  agent.inbox.remove = (id) => {
    const before = agent.inbox.nextTurn.length;
    agent.inbox.nextTurn = agent.inbox.nextTurn.filter((message) => message.id !== id);
    return before !== agent.inbox.nextTurn.length;
  };
  const ctx = new Context();
  ctx.provide("agents", { list: () => [agent], get: () => agent });
  ctx.provide("connection", { rpc: { handle: () => () => {} } });
  ctx.provide("apiProxy", { sessions: {} });
  provideSubagents(ctx);
  ctx.provide("logger", { warn: () => {} });
  apply(ctx);

  const decision = await agent.preStep("next-turn", { turn: 2, step: 1 });
  assert.equal(decision.kind, "enter");
  assert.equal(decision.messages.length, 1);
  assert.deepEqual([...decision.messages], []);
  assert.deepEqual(agent.inbox.nextTurn, []);
  await ctx.fiber.dispose();
});

test("a marker arriving after the pre-step snapshot is deferred to the next turn", async () => {
  const marker = subagentWake("late-pre-step");
  let releasePreStep;
  const preStepGate = new Promise((resolve) => {
    releasePreStep = resolve;
  });
  let reportPreStep;
  const preStepStarted = new Promise((resolve) => {
    reportPreStep = resolve;
  });
  const { agent } = fakeAgent({ preStepMessages: [] });
  let wakeCalls = 0;
  const originalWakeDriver = agent.wakeDriver.bind(agent);
  agent.wakeDriver = (...args) => {
    wakeCalls += 1;
    originalWakeDriver(...args);
  };
  agent.preStep = async () => {
    reportPreStep();
    await preStepGate;
    agent.inbox.nextTurn = [];
    return { kind: "enter", messages: [marker] };
  };
  const ctx = new Context();
  ctx.provide("agents", { list: () => [agent], get: () => agent });
  ctx.provide("connection", { rpc: { handle: () => () => {} } });
  provideSubagents(ctx);
  ctx.provide("logger", { warn: () => {} });
  apply(ctx);

  const first = agent.preStep("next-turn", { turn: 2, step: 1 });
  await preStepStarted;
  agent.inbox.nextTurn.push(marker);
  releasePreStep();
  const firstDecision = await first;
  assert.deepEqual(firstDecision, { kind: "reject" });
  assert.deepEqual(agent.inbox.nextTurn, []);
  await Promise.resolve();
  await Promise.resolve();
  assert.equal(wakeCalls, 1);

  const secondDecision = await agent.preStep("next-turn", { turn: 3, step: 1 });
  assert.equal(secondDecision.kind, "enter");
  assert.equal(secondDecision.messages.length, 1);
  assert.deepEqual([...secondDecision.messages], []);
  await ctx.fiber.dispose();
});

test("a tracked late marker crosses its recorded blocked turn and resumes", async () => {
  const parentId = SessionId("late-gate-parent");
  const childId = SessionId("late-gate-child");
  const parent = fakeAgent({
    id: parentId,
    session: { header: { id: parentId }, events: [] },
  }).agent;
  const messages = [];
  const { agent: child } = fakeAgent({
    id: childId,
    preStepMessages: messages,
    session: childSession(childId, parentId, [
      event(1, "turn/start", { turn: 1 }, 100),
      event(2, "turn/end", { turn: 1, reason: { kind: "interrupted" } }, 110),
    ]),
  });
  let preStepCalls = 0;
  let reportPreStep;
  const preStepStarted = new Promise((resolve) => {
    reportPreStep = resolve;
  });
  let releasePreStep;
  const preStepGate = new Promise((resolve) => {
    releasePreStep = resolve;
  });
  child.preStep = async () => {
    preStepCalls += 1;
    if (preStepCalls === 1) {
      reportPreStep();
      await preStepGate;
      const claimed = [...child.inbox.nextTurn];
      child.inbox.nextTurn.length = 0;
      return { kind: "enter", messages: claimed };
    }
    return { kind: "enter", messages: [] };
  };
  let reportIdle;
  const driverIdle = new Promise((resolve) => {
    reportIdle = resolve;
  });
  child.whenIdle = () => driverIdle;
  let reportWake;
  const wakeRequested = new Promise((resolve) => {
    reportWake = resolve;
  });
  child.wakeDriver = () => reportWake();
  const agents = new Map([
    [String(parentId), parent],
    [String(childId), child],
  ]);
  let reportFollowupEntered;
  const followupEntered = new Promise((resolve) => {
    reportFollowupEntered = resolve;
  });
  let releaseFollowup;
  const followupGate = new Promise((resolve) => {
    releaseFollowup = resolve;
  });
  let reportFollowupAccepted;
  const followupAccepted = new Promise((resolve) => {
    reportFollowupAccepted = resolve;
  });
  const ctx = new Context();
  ctx.provide("agents", {
    list: () => [parent, child],
    get: (id) => agents.get(String(id)),
  });
  ctx.provide("connection", { rpc: { handle: () => () => {} } });
  provideSubagents(ctx, {
    listChildren: async () => [
      { kind: "child", id: childId, activity: "inactive", hasChildren: false, mode: "continuable", label: "late-gate" },
    ],
    followup: async (_parent, _childId, _content, options) => {
      reportFollowupEntered();
      await followupGate;
      messages.push({
        id: "late-gate-marker",
        role: "user",
        content: [],
        source: options.source,
      });
      reportFollowupAccepted();
      return "late-gate-marker";
    },
  });
  ctx.provide("logger", { warn: () => {} });
  apply(ctx);

  const handler = createContinueRpcHandler(ctx);
  const signal = new AbortController().signal;
  const status = await handler("status", { sessionId: String(childId) }, signal);
  const resume = handler("resume", {
    sessionId: String(childId),
    turn: status.value.turn,
    boundarySeq: status.value.boundarySeq,
  }, signal);
  await followupEntered;
  child.session.events.push(event(3, "turn/start", { turn: 2 }, 200));
  const firstStep = child.preStep("next-turn", { turn: 2, step: 1 });
  await preStepStarted;
  releaseFollowup();
  await followupAccepted;
  releasePreStep();
  const firstDecision = await firstStep;
  assert.deepEqual(firstDecision, { kind: "reject" });
  child.session.events.push(event(4, "turn/end", { turn: 2, reason: { kind: "blocked" } }, 210));
  reportIdle();
  await wakeRequested;
  child.session.events.push(event(5, "turn/start", { turn: 3 }, 220));
  const secondDecision = await child.preStep("next-turn", { turn: 3, step: 1 });
  const result = await resume;

  assert.equal(secondDecision.kind, "enter");
  assert.equal(secondDecision.messages.length, 1);
  assert.deepEqual([...secondDecision.messages], []);
  assert.deepEqual(result, { ok: true, value: { accepted: true } });
  await ctx.fiber.dispose();
});

test("ordinary input cancels a deferred tracked marker without waiting", async () => {
  const parentId = SessionId("deferred-ordinary-parent");
  const childId = SessionId("deferred-ordinary-child");
  const parent = fakeAgent({
    id: parentId,
    session: { header: { id: parentId }, events: [] },
  }).agent;
  const messages = [];
  const { agent: child } = fakeAgent({
    id: childId,
    preStepMessages: messages,
    session: childSession(childId, parentId, [
      event(1, "turn/start", { turn: 1 }),
      event(2, "turn/end", { turn: 1, reason: { kind: "interrupted" } }),
    ]),
  });
  let calls = 0;
  let reportFirstStep;
  const firstStepStarted = new Promise((resolve) => {
    reportFirstStep = resolve;
  });
  let releaseFirstStep;
  const firstStepGate = new Promise((resolve) => {
    releaseFirstStep = resolve;
  });
  child.preStep = async () => {
    calls += 1;
    if (calls === 1) {
      reportFirstStep();
      await firstStepGate;
    }
    const claimed = [...child.inbox.nextTurn];
    child.inbox.nextTurn.length = 0;
    return { kind: "enter", messages: claimed };
  };
  let reportIdle;
  const idle = new Promise((resolve) => {
    reportIdle = resolve;
  });
  child.whenIdle = () => idle;
  let reportWake;
  const wakeRequested = new Promise((resolve) => {
    reportWake = resolve;
  });
  child.wakeDriver = () => reportWake();
  const agents = new Map([
    [String(parentId), parent],
    [String(childId), child],
  ]);
  let reportFollowupEntered;
  const followupEntered = new Promise((resolve) => {
    reportFollowupEntered = resolve;
  });
  let releaseFollowup;
  const followupGate = new Promise((resolve) => {
    releaseFollowup = resolve;
  });
  let reportFollowupAccepted;
  const followupAccepted = new Promise((resolve) => {
    reportFollowupAccepted = resolve;
  });
  const ctx = new Context();
  ctx.provide("agents", {
    list: () => [parent, child],
    get: (id) => agents.get(String(id)),
  });
  ctx.provide("connection", { rpc: { handle: () => () => {} } });
  provideSubagents(ctx, {
    listChildren: async () => [
      { kind: "child", id: childId, activity: "inactive", hasChildren: false, mode: "continuable", label: "ordinary" },
    ],
    followup: async (_parent, _childId, _content, options) => {
      reportFollowupEntered();
      await followupGate;
      messages.push({
        id: "deferred-ordinary-marker",
        role: "user",
        content: [],
        source: options.source,
      });
      reportFollowupAccepted();
      return "deferred-ordinary-marker";
    },
  });
  ctx.provide("logger", { warn: () => {} });
  apply(ctx);

  const handler = createContinueRpcHandler(ctx);
  const signal = new AbortController().signal;
  const status = await handler("status", { sessionId: String(childId) }, signal);
  const resume = handler("resume", {
    sessionId: String(childId),
    turn: status.value.turn,
    boundarySeq: status.value.boundarySeq,
  }, signal);
  await followupEntered;
  const first = child.preStep("next-turn", { turn: 2, step: 1 });
  await firstStepStarted;
  releaseFollowup();
  await followupAccepted;
  releaseFirstStep();
  assert.deepEqual(await first, { kind: "reject" });
  reportIdle();
  await wakeRequested;
  const ordinary = {
    id: "deferred-ordinary-input",
    role: "user",
    content: [{ type: "text", text: "ordinary input" }],
    source: { kind: "user", rpcId: "ordinary" },
  };
  messages.push(ordinary);
  const second = await child.preStep("next-turn", { turn: 3, step: 1 });
  const result = await resume;

  assert.deepEqual(second, { kind: "enter", messages: [ordinary] });
  assert.equal(result.ok, false);
  assert.equal(result.error.code, "cancelled");
  await ctx.fiber.dispose();
});

test("plugin shutdown discards a late marker already claimed by pre-step", async () => {
  const marker = subagentWake("late-shutdown");
  let releaseClaim;
  const claimGate = new Promise((resolve) => {
    releaseClaim = resolve;
  });
  let reportReady;
  const ready = new Promise((resolve) => {
    reportReady = resolve;
  });
  let reportClaimed;
  const claimed = new Promise((resolve) => {
    reportClaimed = resolve;
  });
  let releasePreStep;
  const preStepGate = new Promise((resolve) => {
    releasePreStep = resolve;
  });
  const { agent, originalPreStep: initialPreStep } = fakeAgent({ preStepMessages: [] });
  const delayedPreStep = async () => {
    reportReady();
    await claimGate;
    agent.inbox.nextTurn = [];
    reportClaimed();
    await preStepGate;
    return { kind: "enter", messages: [marker] };
  };
  agent.preStep = delayedPreStep;
  const ctx = new Context();
  ctx.provide("agents", { list: () => [agent], get: () => agent });
  ctx.provide("connection", { rpc: { handle: () => () => {} } });
  provideSubagents(ctx);
  ctx.provide("logger", { warn: () => {} });
  apply(ctx);

  const decision = agent.preStep("next-turn", { turn: 2, step: 1 });
  await ready;
  agent.inbox.nextTurn.push(marker);
  releaseClaim();
  await claimed;
  await ctx.fiber.dispose();
  assert.equal(agent.preStep, delayedPreStep);
  releasePreStep();
  const settled = await decision;

  assert.deepEqual(settled, { kind: "reject" });
  assert.deepEqual(agent.inbox.nextTurn, []);
  assert.notEqual(agent.preStep, initialPreStep);
});

test("subagent continuation markers do not become visible user messages", async () => {
  const ordinary = {
    id: "ordinary-followup",
    role: "user",
    content: [{ type: "text", text: "保留这条输入" }],
    source: { kind: "user", rpcId: "ordinary" },
  };
  const { agent } = fakeAgent({ preStepMessages: [subagentWake(), ordinary] });
  const ctx = new Context();
  ctx.provide("agents", { list: () => [agent], get: () => agent });
  ctx.provide("connection", { rpc: { handle: () => () => {} } });
  ctx.provide("apiProxy", { sessions: {} });
  provideSubagents(ctx);
  ctx.provide("logger", { warn: () => {} });
  apply(ctx);

  const decision = await agent.preStep("next-turn", { turn: 2, step: 1 });
  assert.equal(decision.kind, "enter");
  assert.deepEqual([...decision.messages], [ordinary]);
  await ctx.fiber.dispose();
});

test("empty subagent marker still enters a model step", async () => {
  const { agent } = fakeAgent({ preStepMessages: [subagentWake()] });
  const ctx = new Context();
  ctx.provide("agents", { list: () => [agent], get: () => agent });
  ctx.provide("connection", { rpc: { handle: () => () => {} } });
  ctx.provide("apiProxy", { sessions: {} });
  provideSubagents(ctx);
  ctx.provide("logger", { warn: () => {} });
  apply(ctx);

  const decision = await agent.preStep("next-turn", { turn: 2, step: 1 });
  assert.equal(decision.kind, "enter");
  assert.equal(decision.messages.length, 1);
  assert.deepEqual([...decision.messages], []);
  await ctx.fiber.dispose();
});
test("persisted and newly admitted subagent markers collapse into one model step", async () => {
  const removed = [];
  const markers = [
    subagentWake("persisted"),
    subagentWake("new"),
    subagentWake("queued"),
  ];
  const { agent } = fakeAgent({ preStepMessages: markers });
  agent.inbox.nextStep = [];
  agent.inbox.nextTurn = markers;
  agent.inbox.remove = (id) => {
    removed.push(id);
    agent.inbox.nextTurn = agent.inbox.nextTurn.filter((message) => message.id !== id);
    return true;
  };
  const ctx = new Context();
  ctx.provide("agents", { list: () => [agent], get: () => agent });
  ctx.provide("connection", { rpc: { handle: () => () => {} } });
  ctx.provide("apiProxy", { sessions: {} });
  provideSubagents(ctx);
  ctx.provide("logger", { warn: () => {} });
  apply(ctx);

  const decision = await agent.preStep("next-turn", { turn: 2, step: 1 });
  assert.equal(decision.kind, "enter");
  assert.equal(decision.messages.length, 1);
  assert.deepEqual([...decision.messages], []);
  assert.deepEqual(removed, ["persisted", "new", "queued"]);
  assert.deepEqual(agent.inbox.nextTurn, []);
  await ctx.fiber.dispose();
});

test("a throwing pre-step removes queued subagent markers before rethrowing", async () => {
  const removed = [];
  const { agent } = fakeAgent();
  agent.preStep = async () => {
    throw new Error("pre-step failed");
  };
  agent.inbox.nextStep = [];
  agent.inbox.nextTurn = [subagentWake("throw-marker")];
  agent.inbox.remove = (id) => {
    removed.push(id);
    agent.inbox.nextTurn = agent.inbox.nextTurn.filter((message) => message.id !== id);
    return true;
  };
  const ctx = new Context();
  ctx.provide("agents", { list: () => [agent], get: () => agent });
  ctx.provide("connection", { rpc: { handle: () => () => {} } });
  ctx.provide("apiProxy", { sessions: {} });
  provideSubagents(ctx);
  ctx.provide("logger", { warn: () => {} });
  apply(ctx);

  await assert.rejects(
    agent.preStep("next-turn", { turn: 2, step: 1 }),
    /pre-step failed/,
  );
  assert.deepEqual(removed, ["throw-marker"]);
  assert.deepEqual(agent.inbox.nextTurn, []);
  await ctx.fiber.dispose();
});

test("RPC admission starts the direct continuation without appending a prompt", async () => {
  const session = sessionWithReason({
    kind: "error",
    error: { message: "upstream", code: "UPSTREAM" },
  });
  const { agent } = fakeAgent({ session });
  const ctx = new Context();
  ctx.provide("agents", {
    list: () => [agent],
    get: () => agent,
  });
  ctx.provide("connection", {
    rpc: { handle: () => () => {} },
  });
  ctx.provide("apiProxy", { sessions: {} });
  ctx.provide("logger", { warn: () => {} });
  apply(ctx);

  const handler = createContinueRpcHandler(ctx);
  const signal = new AbortController().signal;
  const status = await handler("status", { sessionId: String(agent.id) }, signal);
  assert.equal(status.ok, true);
  assert.equal(status.value.available, true);

  const result = await handler("resume", {
    sessionId: String(agent.id),
    turn: status.value.turn,
    boundarySeq: status.value.boundarySeq,
  }, signal);
  assert.deepEqual(result, { ok: true, value: { accepted: true } });
  await agent.preStep("next-turn", { turn: 2, step: 1 });
  assert.deepEqual(session.events.map((item) => item.type), ["turn/start", "turn/end"]);
  await agent.whenIdle();
  await ctx.fiber.dispose();
});

test("continuation admission rejects maintenance and queued agents", async () => {
  const queued = fakeAgent({ pending: true });
  const maintenance = fakeAgent({ phaseKind: "maintenance" });
  const ctx = new Context();
  ctx.provide("agents", {
    list: () => [queued.agent, maintenance.agent],
    get: () => undefined,
  });
  ctx.provide("connection", { rpc: { handle: () => () => {} } });
  ctx.provide("apiProxy", { sessions: {} });
  ctx.provide("logger", { warn: () => {} });
  apply(ctx);
  assert.equal(startContinuation(queued.agent), false);
  assert.equal(startContinuation(maintenance.agent), false);
  await ctx.fiber.dispose();
});

test("a wake cancelled before pre-step releases its continuation state", async () => {
  const { agent } = fakeAgent();
  const ctx = new Context();
  ctx.provide("agents", { list: () => [agent], get: () => agent });
  ctx.provide("connection", { rpc: { handle: () => () => {} } });
  ctx.provide("apiProxy", { sessions: {} });
  ctx.provide("logger", { warn: () => {} });
  apply(ctx);

  assert.equal(startContinuation(agent), true);
  await agent.whenIdle();
  await Promise.resolve();
  assert.equal(startContinuation(agent), true);
  await agent.whenIdle();
  await ctx.fiber.dispose();
});

test("a claimed marker remains cancelled while pre-step is still running", async () => {
  const parentId = SessionId("claimed-cancel-parent");
  const childId = SessionId("claimed-cancel-child");
  const parent = fakeAgent({
    id: parentId,
    session: { header: { id: parentId }, events: [] },
  }).agent;
  const messages = [];
  const { agent: child } = fakeAgent({
    id: childId,
    preStepMessages: messages,
    session: childSession(childId, parentId, [
      event(1, "turn/start", { turn: 1 }),
      event(2, "turn/end", { turn: 1, reason: { kind: "interrupted" } }),
    ]),
  });
  let releasePreStep;
  const preStepGate = new Promise((resolve) => {
    releasePreStep = resolve;
  });
  let reportClaimed;
  const markerClaimed = new Promise((resolve) => {
    reportClaimed = resolve;
  });
  child.preStep = async () => {
    const claimed = [...child.inbox.nextTurn];
    child.inbox.nextTurn.length = 0;
    reportClaimed();
    await preStepGate;
    return { kind: "enter", messages: claimed };
  };
  const agents = new Map([
    [String(parentId), parent],
    [String(childId), child],
  ]);
  let reportFollowup;
  const followupAccepted = new Promise((resolve) => {
    reportFollowup = resolve;
  });
  const ctx = new Context();
  ctx.provide("agents", {
    list: () => [parent, child],
    get: (id) => agents.get(String(id)),
  });
  ctx.provide("connection", { rpc: { handle: () => () => {} } });
  provideSubagents(ctx, {
    listChildren: async () => [
      { kind: "child", id: childId, activity: "inactive", hasChildren: false, mode: "continuable", label: "claimed" },
    ],
    followup: async (_parent, _childId, _content, options) => {
      messages.push({
        id: "claimed-cancel-marker",
        role: "user",
        content: [],
        source: options.source,
      });
      reportFollowup();
      return "claimed-cancel-marker";
    },
  });
  ctx.provide("logger", { warn: () => {} });
  apply(ctx);

  const handler = createContinueRpcHandler(ctx);
  const controller = new AbortController();
  const status = await handler("status", { sessionId: String(childId) }, controller.signal);
  const resume = handler("resume", {
    sessionId: String(childId),
    turn: status.value.turn,
    boundarySeq: status.value.boundarySeq,
  }, controller.signal);
  await followupAccepted;
  const decision = child.preStep("next-turn", { turn: 2, step: 1 });
  await markerClaimed;
  controller.abort();
  const result = await resume;
  releasePreStep();
  const settledDecision = await decision;

  assert.equal(result.ok, false);
  assert.equal(result.error.code, "cancelled");
  assert.deepEqual(settledDecision, { kind: "reject" });
  await ctx.fiber.dispose();
});

test("shutdown aborts cold persistence before a second read can start", async () => {
  let rpcHandler;
  let inspectCalls = 0;
  let reportList;
  const listStarted = new Promise((resolve) => {
    reportList = resolve;
  });
  const ctx = new Context();
  ctx.provide("agents", { list: () => [], get: () => undefined });
  ctx.provide("connection", {
    rpc: {
      handle: (_channel, handler) => {
        rpcHandler = handler;
        return async () => {};
      },
    },
  });
  ctx.provide("apiProxy", { sessions: {} });
  ctx.provide("sessionPersistence", {
    list: async (signal) => {
      reportList();
      return new Promise((resolve) => {
        const finish = () => resolve([{ id: SessionId("cold-stalled-session"), cwd: "C:/workspace" }]);
        if (signal.aborted) finish();
        else signal.addEventListener("abort", finish, { once: true });
      });
    },
    inspect: async () => {
      inspectCalls += 1;
      throw new Error("inspect must not start after shutdown");
    },
  });
  provideSubagents(ctx);
  ctx.provide("logger", { warn: () => {} });
  apply(ctx);

  const status = rpcHandler(
    "status",
    { sessionId: "cold-stalled-session" },
    new AbortController().signal,
  );
  await listStarted;
  const disposal = ctx.fiber.dispose();
  const result = await status;
  await disposal;

  assert.equal(result.ok, false);
  assert.equal(result.error.code, "cancelled");
  assert.equal(inspectCalls, 0);
});

test("a cold handle published at cancellation is always disposed", async () => {
  const sessionId = SessionId("cold-cancelled-publication");
  const meta = { id: sessionId, cwd: "C:/workspace", seedLength: 0 };
  const events = [
    event(0, "turn/start", { turn: 1 }),
    event(1, "turn/end", { turn: 1, reason: { kind: "interrupted" } }),
  ];
  const session = {
    header: meta,
    events,
    requestHeader: () => undefined,
  };
  const { agent } = fakeAgent({ id: sessionId, session });
  let live = false;
  let resolveResume;
  const resumeHandle = new Promise((resolve) => {
    resolveResume = resolve;
  });
  let reportResume;
  const resumeStarted = new Promise((resolve) => {
    reportResume = resolve;
  });
  let reportDisposed;
  const disposed = new Promise((resolve) => {
    reportDisposed = resolve;
  });
  let disposeCalls = 0;
  let rpcHandler;
  const ctx = new Context();
  ctx.provide("agentDefaultModel", {
    currentSelection: () => ({ provider: "default-provider", model: "default-model" }),
  });
  ctx.provide("agentPresets", { mount: async () => ({ id: "default" }) });
  ctx.provide("agents", {
    list: () => [agent],
    get: () => live ? agent : undefined,
    resume: async () => {
      reportResume();
      return resumeHandle;
    },
  });
  ctx.provide("connection", {
    rpc: {
      handle: (_channel, handler) => {
        rpcHandler = handler;
        return async () => {};
      },
    },
  });
  ctx.provide("sessionPersistence", {
    list: async () => [meta],
    inspect: async () => ({ meta, events }),
  });
  provideSubagents(ctx);
  ctx.provide("logger", { warn: () => {} });
  apply(ctx);

  const controller = new AbortController();
  const status = await rpcHandler("status", { sessionId: String(sessionId) }, controller.signal);
  const resume = rpcHandler("resume", {
    sessionId: String(sessionId),
    turn: status.value.turn,
    boundarySeq: status.value.boundarySeq,
  }, controller.signal);
  await resumeStarted;
  live = true;
  resolveResume({
    agent,
    async dispose() {
      disposeCalls += 1;
      live = false;
      reportDisposed();
    },
  });
  controller.abort();
  const result = await resume;
  await disposed;

  assert.equal(result.ok, false);
  assert.equal(result.error.code, "cancelled");
  assert.equal(disposeCalls, 1);
  assert.equal(live, false);
  await ctx.fiber.dispose();
});

test("cancellation after cold resolution prevents the model wake", async () => {
  const sessionId = SessionId("cold-cancelled-before-wake");
  const meta = { id: sessionId, cwd: "C:/workspace", seedLength: 0 };
  const durableEvents = [
    event(0, "turn/start", { turn: 1 }),
    event(1, "turn/end", { turn: 1, reason: { kind: "interrupted" } }),
  ];
  const controller = new AbortController();
  let abortOnRead = false;
  const session = {
    header: meta,
    get events() {
      if (abortOnRead) {
        abortOnRead = false;
        controller.abort();
      }
      return durableEvents;
    },
    requestHeader: () => undefined,
  };
  const { agent } = fakeAgent({ id: sessionId, session });
  let wakeCalls = 0;
  agent.wakeDriver = () => {
    wakeCalls += 1;
  };
  let live = false;
  let disposeCalls = 0;
  let rpcHandler;
  const ctx = new Context();
  ctx.provide("agentDefaultModel", {
    currentSelection: () => ({ provider: "default-provider", model: "default-model" }),
  });
  ctx.provide("agentPresets", { mount: async () => ({ id: "default" }) });
  ctx.provide("agents", {
    list: () => [agent],
    get: () => live ? agent : undefined,
    resume: async () => {
      live = true;
      abortOnRead = true;
      return {
        agent,
        async dispose() {
          disposeCalls += 1;
          live = false;
        },
      };
    },
  });
  ctx.provide("connection", {
    rpc: {
      handle: (_channel, handler) => {
        rpcHandler = handler;
        return async () => {};
      },
    },
  });
  ctx.provide("sessionPersistence", {
    list: async () => [meta],
    inspect: async () => ({ meta, events: durableEvents }),
  });
  provideSubagents(ctx);
  ctx.provide("logger", { warn: () => {} });
  apply(ctx);

  const status = await rpcHandler("status", { sessionId: String(sessionId) }, controller.signal);
  const result = await rpcHandler("resume", {
    sessionId: String(sessionId),
    turn: status.value.turn,
    boundarySeq: status.value.boundarySeq,
  }, controller.signal);

  assert.equal(result.ok, false);
  assert.equal(result.error.code, "cancelled");
  assert.equal(wakeCalls, 0);
  assert.equal(disposeCalls, 1);
  assert.equal(live, false);
  await ctx.fiber.dispose();
});

test("cold resume restores the recorded preset and logged model selection", async () => {
  const sessionId = SessionId("cold-composed-session");
  const meta = {
    id: sessionId,
    cwd: "C:/workspace",
    seedLength: 0,
    agentPreset: "created-preset",
  };
  const events = [
    event(0, "agent-preset/selected", { agentPreset: "selected-preset" }),
    event(1, "turn/start", { turn: 1 }),
    event(2, "turn/end", { turn: 1, reason: { kind: "interrupted" } }),
  ];
  const session = {
    header: meta,
    events,
    requestHeader: () => ({
      config: {
        provider: "logged-provider",
        model: "logged-model",
        reasoningEffort: "high",
      },
    }),
  };
  const { agent } = fakeAgent({ id: sessionId, session });
  let live = false;
  let disposeCalls = 0;
  let mountedPreset;
  let rpcHandler;
  const ctx = new Context();
  ctx.provide("agentDefaultModel", {
    currentSelection: () => ({ provider: "default-provider", model: "default-model" }),
  });
  ctx.provide("agentPresets", {
    mount: async (_agentCtx, presetId) => {
      mountedPreset = presetId;
      return { id: presetId };
    },
  });
  ctx.provide("agents", {
    list: () => [agent],
    get: () => live ? agent : undefined,
    resume: async (options) => {
      assert.deepEqual(options.agentOptions, {
        provider: "default-provider",
        model: "default-model",
      });
      const listeners = new Map();
      const agentCtx = {
        agent,
        on(name, listener) {
          const entries = listeners.get(name) ?? [];
          entries.push(listener);
          listeners.set(name, entries);
          return () => {
            const index = entries.indexOf(listener);
            if (index >= 0) entries.splice(index, 1);
          };
        },
      };
      const runWaterfall = async (name, args, terminal) => {
        const entries = [...(listeners.get(name) ?? [])];
        const dispatch = async (index) => index >= entries.length
          ? terminal()
          : entries[index](...args, () => dispatch(index + 1));
        return dispatch(0);
      };
      const commit = await options.setup(agentCtx);
      const assembled = await runWaterfall(
        "system-prompt/assemble",
        [{}, {}],
        async () => ({ variables: { existing: "value" } }),
      );
      assert.deepEqual(assembled.variables, {
        existing: "value",
        provider: "logged-provider",
        model: "logged-model",
      });
      const request = await runWaterfall(
        "agent/request",
        [{}],
        async () => ({
          provider: "inherited-provider",
          model: "inherited-model",
          reasoningEffort: "low",
        }),
      );
      assert.deepEqual(request, {
        provider: "logged-provider",
        model: "logged-model",
        reasoningEffort: "high",
      });
      assert.deepEqual(listeners.get("agent/request"), []);
      commit.commit();
      live = true;
      return {
        agent,
        async dispose() {
          disposeCalls += 1;
          live = false;
        },
      };
    },
  });
  ctx.provide("connection", {
    rpc: {
      handle: (_channel, handler) => {
        rpcHandler = handler;
        return async () => {};
      },
    },
  });
  ctx.provide("sessionPersistence", {
    list: async () => [meta],
    inspect: async () => ({ meta, events }),
  });
  provideSubagents(ctx);
  ctx.provide("logger", { warn: () => {} });
  apply(ctx);

  const signal = new AbortController().signal;
  const status = await rpcHandler("status", { sessionId: String(sessionId) }, signal);
  const result = await rpcHandler("resume", {
    sessionId: String(sessionId),
    turn: status.value.turn,
    boundarySeq: status.value.boundarySeq,
  }, signal);

  assert.equal(result.ok, true);
  assert.equal(result.value.accepted, true);
  assert.equal(mountedPreset, "selected-preset");
  await agent.whenIdle();
  await Promise.resolve();
  assert.equal(disposeCalls, 1);
  assert.equal(live, false);
  await ctx.fiber.dispose();
});

test("shutdown rolls back a cold Agent before publication", async () => {
  const sessionId = SessionId("cold-stalled-restoration");
  const session = sessionWithReason({ kind: "interrupted" });
  const meta = { id: sessionId, cwd: "C:/workspace", seedLength: 0 };
  let rpcHandler;
  let published = false;
  let releaseResume;
  const resumeGate = new Promise((resolve) => {
    releaseResume = resolve;
  });
  let reportResume;
  const resumeStarted = new Promise((resolve) => {
    reportResume = resolve;
  });
  let reportResumeFinished;
  const resumeFinished = new Promise((resolve) => {
    reportResumeFinished = resolve;
  });
  const ctx = new Context();
  ctx.provide("agentDefaultModel", {
    currentSelection: () => ({ provider: "test-provider", model: "test-model" }),
  });
  ctx.provide("agentPresets", { mount: async () => ({ id: "test-preset" }) });
  ctx.provide("agents", {
    list: () => [],
    get: () => undefined,
    resume: async (options) => {
      try {
        assert.equal(options.resumeSessionId, sessionId);
        assert.deepEqual(options.agentOptions, {
          provider: "test-provider",
          model: "test-model",
        });
        assert.equal(options.signal instanceof AbortSignal, true);
        assert.equal(typeof options.setup, "function");
        reportResume();
        await resumeGate;
        const commit = await options.setup({});
        commit?.commit();
        published = true;
        return { agent: {} };
      } finally {
        reportResumeFinished();
      }
    },
  });
  ctx.provide("connection", {
    rpc: {
      handle: (_channel, handler) => {
        rpcHandler = handler;
        return async () => {};
      },
    },
  });
  ctx.provide("sessionPersistence", {
    list: async () => [meta],
    inspect: async () => ({ meta, events: session.events }),
  });
  provideSubagents(ctx);
  ctx.provide("logger", { warn: () => {} });
  apply(ctx);

  const signal = new AbortController().signal;
  const status = await rpcHandler("status", { sessionId: String(sessionId) }, signal);
  assert.equal(status.ok, true);
  assert.equal(status.value.available, true);
  const resume = rpcHandler("resume", {
    sessionId: String(sessionId),
    turn: status.value.turn,
    boundarySeq: status.value.boundarySeq,
  }, signal);
  await resumeStarted;
  const disposal = ctx.fiber.dispose();
  const result = await resume;
  await disposal;

  assert.equal(result.ok, false);
  assert.equal(result.error.code, "cancelled");
  releaseResume();
  await resumeFinished;
  assert.equal(published, false);
});

test("shutdown completes while subagent status lookup ignores cancellation", async () => {
  const parentId = SessionId("stalled-status-parent");
  const childId = SessionId("stalled-status-child");
  const parent = fakeAgent({
    id: parentId,
    session: { header: { id: parentId }, events: [] },
  }).agent;
  const { agent: child, originalPreStep } = fakeAgent({
    id: childId,
    session: childSession(childId, parentId, [
      event(1, "turn/start", { turn: 1 }),
      event(2, "turn/end", { turn: 1, reason: { kind: "interrupted" } }),
    ]),
  });
  const agents = new Map([
    [String(parentId), parent],
    [String(childId), child],
  ]);
  let rpcHandler;
  let reportLookup;
  const lookupStarted = new Promise((resolve) => {
    reportLookup = resolve;
  });
  const ctx = new Context();
  ctx.provide("agents", {
    list: () => [parent, child],
    get: (id) => agents.get(String(id)),
  });
  ctx.provide("connection", {
    rpc: {
      handle: (_channel, handler) => {
        rpcHandler = handler;
        return async () => {};
      },
    },
  });
  ctx.provide("apiProxy", { sessions: {} });
  provideSubagents(ctx, {
    listChildren: async () => {
      reportLookup();
      await new Promise(() => {});
      return [];
    },
  });
  ctx.provide("logger", { warn: () => {} });
  apply(ctx);

  const status = rpcHandler(
    "status",
    { sessionId: String(childId) },
    new AbortController().signal,
  );
  await lookupStarted;
  const disposal = ctx.fiber.dispose();
  const result = await status;
  await disposal;

  assert.equal(result.ok, false);
  assert.equal(result.error.code, "cancelled");
  assert.equal(child.preStep, originalPreStep);
});

test("plugin disposal does not wait for an active ordinary continuation", async () => {
  const { agent, originalPreStep } = fakeAgent();
  const neverIdle = new Promise(() => {});
  agent.wakeDriver = () => {};
  agent.whenIdle = () => neverIdle;
  const ctx = new Context();
  ctx.provide("agents", { list: () => [agent], get: () => agent });
  ctx.provide("connection", { rpc: { handle: () => () => {} } });
  ctx.provide("apiProxy", { sessions: {} });
  ctx.provide("logger", { warn: () => {} });
  apply(ctx);

  assert.equal(startContinuation(agent), true);
  const disposal = ctx.fiber.dispose();
  const completed = await Promise.race([
    disposal.then(() => true),
    new Promise((resolve) => setTimeout(() => resolve(false), 100)),
  ]);
  assert.equal(completed, true);
  assert.equal(agent.preStep, originalPreStep);
});

test("RPC shutdown cancels an admitted marker before restoring the Agent boundary", async () => {
  const parentId = SessionId("shutdown-parent");
  const childId = SessionId("shutdown-child");
  const parent = fakeAgent({
    id: parentId,
    session: { header: { id: parentId }, events: [] },
  }).agent;
  const { agent: child, originalPreStep } = fakeAgent({
    id: childId,
    session: childSession(childId, parentId, [
      event(1, "turn/start", { turn: 1 }),
      event(2, "turn/end", { turn: 1, reason: { kind: "interrupted" } }),
    ]),
  });
  child.inbox.nextStep = [];
  child.inbox.nextTurn = [];
  const removed = [];
  child.inbox.remove = (id) => {
    const before = child.inbox.nextTurn.length;
    child.inbox.nextTurn = child.inbox.nextTurn.filter((message) => message.id !== id);
    if (child.inbox.nextTurn.length === before) return false;
    removed.push(id);
    return true;
  };
  const agents = new Map([
    [String(parentId), parent],
    [String(childId), child],
  ]);
  let rpcHandler;
  let rpcReleased = false;
  let reportFollowup;
  const followupAccepted = new Promise((resolve) => {
    reportFollowup = resolve;
  });
  const ctx = new Context();
  ctx.provide("agents", {
    list: () => [parent, child],
    get: (id) => agents.get(String(id)),
  });
  ctx.provide("connection", {
    rpc: {
      handle: (_channel, handler) => {
        rpcHandler = handler;
        return async () => {
          rpcReleased = true;
        };
      },
    },
  });
  ctx.provide("apiProxy", { sessions: {} });
  provideSubagents(ctx, {
    listChildren: async () => [
      { kind: "child", id: childId, activity: "inactive", hasChildren: false, mode: "continuable", label: "shutdown" },
    ],
    followup: async (_parent, _childId, _content, options) => {
      child.inbox.nextTurn.push({
        id: "shutdown-marker",
        role: "user",
        content: [],
        source: options.source,
      });
      reportFollowup();
      await new Promise(() => {});
      return "shutdown-marker";
    },
  });
  ctx.provide("logger", { warn: () => {} });
  apply(ctx);

  const signal = new AbortController().signal;
  const status = await rpcHandler("status", { sessionId: String(childId) }, signal);
  const resume = rpcHandler("resume", {
    sessionId: String(childId),
    turn: status.value.turn,
    boundarySeq: status.value.boundarySeq,
  }, signal);
  await followupAccepted;
  const disposal = ctx.fiber.dispose();
  const result = await resume;
  await disposal;

  assert.equal(result.ok, false);
  assert.equal(result.error.code, "cancelled");
  assert.equal(rpcReleased, true);
  assert.deepEqual(removed, ["shutdown-marker"]);
  assert.deepEqual(child.inbox.nextTurn, []);
  assert.equal(child.preStep, originalPreStep);
});

test("plugin disposal clears a legacy session marker without driving it", async () => {
  const removed = [];
  let wakeCalls = 0;
  const { agent, originalPreStep } = fakeAgent();
  agent.inbox.nextStep = [];
  agent.inbox.nextTurn = [];
  agent.inbox.remove = (id) => {
    removed.push(id);
    agent.inbox.nextTurn = agent.inbox.nextTurn.filter((message) => message.id !== id);
    return true;
  };
  agent.wakeDriver = () => {
    wakeCalls += 1;
  };
  const ctx = new Context();
  ctx.provide("agents", { list: () => [agent], get: () => agent });
  ctx.provide("connection", { rpc: { handle: () => () => {} } });
  ctx.provide("apiProxy", { sessions: {} });
  provideSubagents(ctx);
  ctx.provide("logger", { warn: () => {} });
  apply(ctx);
  agent.inbox.nextTurn.push(legacyWake("release-legacy"));

  await ctx.fiber.dispose();
  assert.deepEqual(removed, ["release-legacy"]);
  assert.equal(wakeCalls, 0);
  assert.equal(agent.preStep, originalPreStep);
});

test("plugin disposal cancels an unconsumed subagent marker safely", async () => {
  const removed = [];
  let wakeCalls = 0;
  const { agent, originalPreStep } = fakeAgent();
  agent.inbox.nextStep = [subagentWake("pending-subagent")];
  agent.inbox.nextTurn = [];
  agent.inbox.remove = (id) => {
    removed.push(id);
    return true;
  };
  agent.wakeDriver = () => {
    wakeCalls += 1;
  };
  const ctx = new Context();
  ctx.provide("agents", { list: () => [agent], get: () => agent });
  ctx.provide("connection", { rpc: { handle: () => () => {} } });
  ctx.provide("apiProxy", { sessions: {} });
  provideSubagents(ctx);
  ctx.provide("logger", { warn: () => {} });
  apply(ctx);

  await ctx.fiber.dispose();
  assert.deepEqual(removed, ["pending-subagent"]);
  assert.equal(wakeCalls, 0);
  assert.equal(agent.preStep, originalPreStep);
});

test("plugin disposal releases its RPC channel", async () => {
  let released = false;
  const ctx = new Context();
  ctx.provide("agents", { list: () => [], get: () => undefined });
  ctx.provide("connection", {
    rpc: {
      handle: () => async () => {
        released = true;
      },
    },
  });
  ctx.provide("apiProxy", { sessions: {} });
  ctx.provide("logger", { warn: () => {} });
  apply(ctx);
  await ctx.fiber.dispose();
  assert.equal(released, true);
});
