import type { Context } from "@deepseek-ai/cordis";
import type { Agent } from "@deepseek-ai/dsh-agent";
import type { ConnectionRpcHandler } from "@deepseek-ai/dsh-client-connection";
import { type SessionEvent } from "@deepseek-ai/dsh-session";
export declare const name = "continue";
export declare const inject: string[];
export declare const RPC_CHANNEL = "/dsh-continue";
type ContinueReason = "request-error" | "interrupted" | "max-tokens" | "disposed" | "none";
export interface ContinueStatus {
    readonly available: boolean;
    readonly reason: ContinueReason;
    readonly turn?: number;
    readonly boundarySeq?: number;
}
export declare function clearLegacyContinuationWakes(agent: Agent): number;
/** Classify the latest durable turn and reject sessions with unrelated pending input. */
export declare function continueStatusFromEvents(events: readonly SessionEvent[], agent?: Agent, inheritedEventCount?: number): ContinueStatus;
/**
 * Start a model turn with no inbox message. Returns false when the live Agent
 * changed after status admission or this DSH build lacks the private boundary.
 */
export declare function startContinuation(agent: Agent): boolean;
export declare function createContinueRpcHandler(ctx: Context): ConnectionRpcHandler;
export declare function apply(ctx: Context): void;
export {};
//# sourceMappingURL=runtime.d.ts.map