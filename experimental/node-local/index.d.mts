export interface InvocationContext {
  readonly executionId: string;
  readonly traceId: string;
}

export interface Int32Record {
  readonly value: number;
}

export type Failure<StepId extends string = string> = {
  readonly owner: 'LOCAL_SHELL';
  readonly stepId: StepId;
} & (
  | { readonly code: 'TRANSIENT'; readonly message: string; readonly retryable: true }
  | { readonly code: 'BUSINESS_REJECTION'; readonly message: string; readonly retryable: false }
  | { readonly code: 'UNCLASSIFIED'; readonly message: 'Unclassified business function failure'; readonly retryable: false }
);

export type TerminalRecord<StepId extends string = string> =
  | { readonly kind: 'COMPLETED'; readonly outcome: { readonly kind: 'VALUE'; readonly value: Int32Record } }
  | { readonly kind: 'FAILED'; readonly failure: Failure<StepId> }
  | { readonly kind: 'REJECTED'; readonly diagnostic: string }
  | { readonly kind: 'CANCELLED' };

export type Observation =
  | {
    readonly kind: 'ATTEMPT_STARTED' | 'RETURNED' | 'RETRY_SCHEDULED';
    readonly pipelineId: string;
    readonly stepId: string;
    readonly inputType: string;
    readonly outputType: string;
    readonly attempt: number;
    readonly context: InvocationContext;
  }
  | {
    readonly kind: 'TERMINAL';
    readonly pipelineId: string;
    readonly context: InvocationContext | undefined;
    readonly record: TerminalRecord;
  };

export interface RuntimeBinding {
  readonly schemaVersion: 1;
  readonly projectionJson: string;
  readonly projectionSha256: string;
  readonly handlers: object;
}

export interface Application<StepId extends string = string> {
  run(input: Int32Record, context: InvocationContext, signal?: AbortSignal): Promise<TerminalRecord<StepId>>;
}

export interface NodeLocalRuntime {
  /** The generated application supplies the StepId type for its admitted projection. */
  load<StepId extends string = string>(binding: RuntimeBinding): Application<StepId>;
}

export class NodeLocalDiagnostic extends Error {
  readonly code: string;
  constructor(code: string, detail?: string);
}

export class StructuredFailure extends Error {
  constructor(envelope: Failure);
}

export interface SerializedTransitionPayload {
  readonly payloadTypeId: string;
  readonly payloadEncoding: string;
  readonly payload: string;
}

/** An opt-in inner payload binding for four verified compiler logical type IDs. */
export class PolyglotInt32PayloadBinding {
  constructor(...typeIds: [string, string, string, string]);
  encode(typeId: string, value: number): SerializedTransitionPayload;
  decode(expectedTypeId: string, envelope: SerializedTransitionPayload): number;
}

export function createNodeLocalRuntime(options?: {
  readonly onObservation?: (observation: Observation) => void;
}): NodeLocalRuntime;
