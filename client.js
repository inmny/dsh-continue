window.__ModuleLoader__.load({
  id: "dsh-plugin-continue",
  factory: (require) => {
    const module = { exports: {} };
    const {
      createElement,
      memo,
      useCallback,
      useEffect,
      useMemo,
      useRef,
      useState,
    } = require("react");
    const {
      IconLoadingOutline16,
      IconPlayOutline16,
      Tooltip,
    } = require("@deepseek-ai/dsh-client-ui-primitives");

    const NS = "dsh-continue";
    const SLOT = "conversation.input.right";
    const CHANNEL = "/dsh-continue";
    const STYLE_ID = "dsh-plugin-continue";

    const css = `
.dsh-continue-wrap{display:inline-flex;align-items:center;gap:5px;min-width:0}
.dsh-continue-button{appearance:none;display:inline-flex;align-items:center;justify-content:center;width:30px;height:30px;padding:0;border:1px solid var(--dsw-alias-border-l2);border-radius:6px;background:transparent;color:var(--dsw-alias-label-secondary);cursor:pointer}
.dsh-continue-button:hover:not(:disabled){border-color:var(--dsw-alias-brand-primary);color:var(--dsw-alias-label-primary);background:var(--dsw-alias-bg-layer-2)}
.dsh-continue-button:focus-visible{outline:2px solid var(--dsw-alias-brand-primary);outline-offset:1px}
.dsh-continue-button:disabled{cursor:default;opacity:.55}
.dsh-continue-error{max-width:180px;overflow:hidden;color:var(--dsw-alias-label-error);font-size:12px;line-height:1.4;text-overflow:ellipsis;white-space:nowrap}
`;

    const zh = {
      continue: "继续运行",
      continuing: "正在继续运行",
      continueSubagent: "继续运行子代理",
      continuingSubagent: "正在继续子代理",
      error: "续跑失败",
    };
    const en = {
      continue: "Continue running",
      continuing: "Continuing",
      continueSubagent: "Continue subagent",
      continuingSubagent: "Continuing subagent",
      error: "Continuation failed",
    };

    function installStyle() {
      if (typeof document === "undefined") return () => {};
      const existing = document.getElementById(STYLE_ID);
      if (existing !== null) existing.remove();
      const style = document.createElement("style");
      style.id = STYLE_ID;
      style.dataset.plugin = "dsh-plugin-continue";
      style.textContent = css;
      document.head.appendChild(style);
      return () => {
        if (style.parentNode !== null) style.remove();
      };
    }

    function isObject(value) {
      return typeof value === "object" && value !== null && !Array.isArray(value);
    }

    function isStatus(value) {
      return isObject(value)
        && typeof value.available === "boolean"
        && ["request-error", "interrupted", "max-tokens", "disposed", "user", "none"].includes(value.reason)
        && (value.turn === undefined || Number.isSafeInteger(value.turn))
        && (value.boundarySeq === undefined || Number.isSafeInteger(value.boundarySeq));
    }

    function errorMessage(result, fallback) {
      return isObject(result) && isObject(result.error) && typeof result.error.message === "string"
        ? result.error.message
        : fallback;
    }

    function canContinueSubagent(session) {
      if (session?.subagent === null) return true;
      return isObject(session?.subagent)
        && isObject(session.subagent.address)
        && session.subagent.address.mode === "continuable"
        && session.subagent.parentAvailable === true;
    }

    function canAskHost(session, input) {
      const pass = session !== undefined
        && input !== undefined
        && input.phase === "plain"
        && input.draft === ""
        && input.attachmentIds.length === 0
        && input.queue.length === 0
        && session.removed !== true
        && session.blank !== true
        && session.openState === "open"
        && session.running !== true
        && canContinueSubagent(session)
        && session.pendingSubmissions.length === 0
        && session.queue.length === 0;
      return pass;
    }

    const ContinueControl = memo(function ContinueControl({
      connection,
      sessionId,
      useSession,
      useInput,
      t,
    }) {
      const session = useSession((s) => s);
      const input = useInput((s) => s);
      const [status, setStatus] = useState(null);
      const [loading, setLoading] = useState(false);
      const [accepted, setAccepted] = useState(false);
      const [error, setError] = useState(null);
      const resumeRequest = useRef({
        controller: null,
        generation: 0,
      });

      const tailKey = useMemo(() => {
        return [
          session?.running === true,
          session?.removed === true,
          session?.openState,
          session?.blank === true,
          session?.subagent?.address?.mode ?? "ordinary",
          session?.subagent?.address?.parentSessionId ?? "",
          session?.subagent?.address?.childSessionId ?? "",
          session?.subagent?.parentAvailable === true,
          session?.lastAgentError ?? "",
          session?.pendingSubmissions?.length ?? 0,
          session?.queue?.length ?? 0,
          input?.phase,
          input?.draft ?? "",
          input?.attachmentIds?.length ?? 0,
          input?.queue?.length ?? 0,
        ].join("|");
      }, [session, input]);

      useEffect(() => {
        resumeRequest.current.generation += 1;
        resumeRequest.current.controller?.abort();
        resumeRequest.current.controller = null;
        setLoading(false);
        return () => {
          resumeRequest.current.generation += 1;
          resumeRequest.current.controller?.abort();
          resumeRequest.current.controller = null;
        };
      }, [sessionId, tailKey]);

      useEffect(() => {
        let cancelled = false;
        setStatus(null);
        setAccepted(false);
        setError(null);
        if (!canAskHost(session, input)) return () => {
          cancelled = true;
        };

        const controller = new AbortController();
        connection.rpc.call(CHANNEL, "status", { sessionId }, controller.signal).then((result) => {
          if (cancelled) return;
          if (result?.ok === true && isStatus(result.value)) {
            setStatus(result.value);
          } else if (result?.ok === false) {
            setError(errorMessage(result, t("error")));
          }
        }).catch((cause) => {
          if (!cancelled && cause?.name !== "AbortError") setError(t("error"));
        });
        return () => {
          cancelled = true;
          controller.abort();
        };
      }, [connection, sessionId, tailKey, t]);

      const continueRun = useCallback(async () => {
        if (loading || accepted || status?.available !== true) return;
        if (!Number.isSafeInteger(status.turn) || !Number.isSafeInteger(status.boundarySeq)) return;
        const controller = new AbortController();
        const generation = resumeRequest.current.generation;
        resumeRequest.current.controller?.abort();
        resumeRequest.current.controller = controller;
        setLoading(true);
        setError(null);
        try {
          const result = await connection.rpc.call(CHANNEL, "resume", {
            sessionId,
            turn: status.turn,
            boundarySeq: status.boundarySeq,
          }, controller.signal);
          if (controller.signal.aborted || resumeRequest.current.generation !== generation) return;
          if (result?.ok === true) {
            setAccepted(true);
            setStatus({ ...status, available: false });
          } else {
            setError(errorMessage(result, t("error")));
          }
        } catch (cause) {
          if (!controller.signal.aborted
            && resumeRequest.current.generation === generation
            && cause?.name !== "AbortError") {
            setError(t("error"));
          }
        } finally {
          if (resumeRequest.current.generation === generation
            && resumeRequest.current.controller === controller) {
            resumeRequest.current.controller = null;
            setLoading(false);
          }
        }
      }, [accepted, connection, loading, sessionId, status, t]);

      if (!canAskHost(session, input) || accepted) return null;
      if (status?.available !== true) {
        return error === null ? null : createElement("span", {
          className: "dsh-continue-error",
          role: "alert",
          title: error,
        }, error);
      }
      const isSubagent = session?.subagent !== null;
      const label = loading
        ? t(isSubagent ? "continuingSubagent" : "continuing")
        : t(isSubagent ? "continueSubagent" : "continue");
      return createElement("span", {
        className: "dsh-continue-wrap",
        title: error ?? undefined,
      },
      createElement(Tooltip, {
        label,
        side: "bottom",
        delayMs: 500,
      }, createElement("button", {
        type: "button",
        className: "dsh-continue-button",
        "aria-label": label,
        disabled: loading,
        onClick: continueRun,
      }, createElement(loading ? IconLoadingOutline16 : IconPlayOutline16, { size: 15 }))),
      error !== null ? createElement("span", {
        className: "dsh-continue-error",
        role: "alert",
      }, error) : null);
    });

    function apply(ctx) {
      const styleDispose = installStyle();
      ctx.effect(() => styleDispose, "dsh-continue: styles");
      ctx.effect(() => ctx.locale.register(NS, { zh, en }), "dsh-continue: dictionaries");
      ctx.slots.inject(SLOT, () => ctx.slots.register({
        name: SLOT,
        id: "dsh-continue",
        order: 90,
        locale: NS,
      }, (props) => createElement(ContinueControl, {
        connection: ctx.connection,
        sessionId: props.sessionId,
        useSession: props.useSession,
        useInput: props.useInput,
        t: props.t,
      })));
    }

    const inject = ["slots", "locale", "connection"];
    module.exports = { apply, inject };
    return module.exports;
  },
});
