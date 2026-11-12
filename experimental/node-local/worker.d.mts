import type { Server } from 'node:https';
import type { Buffer } from 'node:buffer';
import type { Application, NodeLocalRuntime, Observation } from './index.mjs';

export interface NativeTransitionWorkerOptions {
  readonly bind: (runtime: NodeLocalRuntime) => Application;
  readonly expectedProjection: Buffer;
  readonly tenantId: string;
  readonly pipelineId: string;
  readonly contractVersion: string;
  readonly releaseVersion: string;
  readonly artifactId: string;
  readonly artifactDigest: string;
  readonly profiles: {
    readonly outerEncoding: 'application/tpf-transition-envelope+json';
    readonly payloadEncoding: 'tpf-polyglot-int32-json/1';
    readonly executionProfile: 'tpf-polyglot-unary-policy/1';
    readonly resultProfile: 'tpf-polyglot-failed-result/1';
  };
  readonly sharedSecret: string;
  readonly key: string | Buffer;
  readonly cert: string | Buffer;
  readonly onObservation?: (observation: Observation) => void;
}

/** Use only the verified, deployment-owned prepared application and projection. */
export function createNativeTransitionWorker(options: NativeTransitionWorkerOptions): Server;
