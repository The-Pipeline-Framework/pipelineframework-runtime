import { createHash } from 'node:crypto';
import { NodeLocalDiagnostic, requireKeys, snapshot } from './records.mjs';

const profile = 'tpf-polyglot-unary-policy/1';
const capabilities = ['pure', 'linear', 'unary', 'local', 'retry', 'cancellation', 'context'];
const contextKeys = ['executionId', 'traceId'];
const policy = {
  failureMapping: 'structured-failure/1',
  retryOwner: 'LOCAL_SHELL',
  resultMapping: 'explicit-value/1',
  cancellation: 'first-accepted-terminal/1',
  context: 'explicit-snapshot/1',
  recovery: 'REJECT',
  legacyPolicy: 'REJECT',
};
const valueField = {name: 'value', type: 'int32', presence: 'REQUIRED', nullability: 'NON_NULL'};
const pipelineId = /^[a-z][a-z0-9-]*$/;
const identifier = /^[A-Za-z][A-Za-z0-9_]*$/;
const digest = /^[a-f0-9]{64}$/;

function matches(value, pattern) {
  return typeof value === 'string' && pattern.test(value);
}

function requireRecord(value, keys, code, detail) {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) {
    throw new NodeLocalDiagnostic(code, detail);
  }
  requireKeys(value, keys, code, detail);
}

function requireConstants(value, constants, code, detail) {
  requireRecord(value, Object.keys(constants), code, detail);
  if (Object.entries(constants).some(([key, expected]) => value[key] !== expected)) {
    throw new NodeLocalDiagnostic(code, detail);
  }
}

function requireList(value, expected, code, detail) {
  if (!Array.isArray(value) || value.length !== expected.length
      || !expected.every((entry, index) => value[index] === entry)) {
    throw new NodeLocalDiagnostic(code, detail);
  }
}

function exactInteger(number, source) {
  // Check the original JSON token because an out-of-profile fraction can round to 0, 1, or 2.
  if (!Number.isSafeInteger(number)) throw new NodeLocalDiagnostic('INVALID_NUMBER');
  const [, sign, whole, fraction = '', exponent = '0'] =
    /^(-?)(\d+)(?:\.(\d+))?(?:[eE]([+-]?\d+))?$/.exec(source);
  const digits = (whole + fraction).replace(/^0+/, '');
  if (digits === '') return number;
  const significant = digits.replace(/0+$/, '');
  const zeros = Number(exponent) - fraction.length + digits.length - significant.length;
  if (zeros < 0 || zeros > 15 || significant.length + zeros > 16
      || BigInt(sign + significant + '0'.repeat(zeros)) !== BigInt(number)) {
    throw new NodeLocalDiagnostic('INVALID_NUMBER');
  }
  return number;
}

function parseProjection(source) {
  let projection;
  try {
    projection = JSON.parse(source, (_key, value, context) =>
      typeof value === 'number' ? exactInteger(value, context.source) : value);
  } catch (error) {
    if (error instanceof NodeLocalDiagnostic) throw error;
    throw new NodeLocalDiagnostic('INVALID_JSON', 'expected compiler projection JSON');
  }
  // JSON.parse has already checked syntax. Visit keys in the valid source to reject
  // shadowed fields that JSON.parse would otherwise silently discard.
  const objects = [];
  for (const [token, string, colon] of source.matchAll(/("(?:\\.|[^"\\])*")([ \t\r\n]*:)?|[{}\[\]]/g)) {
    if (token === '{') objects.push(new Set());
    else if (token === '[') objects.push(undefined);
    else if (token === '}' || token === ']') objects.pop();
    else if (colon) {
      const key = JSON.parse(string);
      const keys = objects.at(-1);
      if (keys.has(key)) throw new NodeLocalDiagnostic('INVALID_JSON', `duplicate field ${key}`);
      keys.add(key);
    }
  }
  return projection;
}

function admitTypes(types) {
  const detail = 'expected four distinct canonical records with one required non-null value:int32';
  if (!Array.isArray(types) || types.length !== 4) {
    throw new NodeLocalDiagnostic('UNSUPPORTED_TYPE', detail);
  }
  const ids = new Set();
  for (const type of types) {
    requireRecord(type, ['id', 'fields'], 'UNSUPPORTED_TYPE', detail);
    if (!matches(type.id, identifier) || ids.has(type.id)
        || !Array.isArray(type.fields) || type.fields.length !== 1) {
      throw new NodeLocalDiagnostic('UNSUPPORTED_TYPE', detail);
    }
    requireConstants(type.fields[0], valueField, 'UNSUPPORTED_TYPE', detail);
    ids.add(type.id);
  }
  return ids;
}

function admitSteps(steps, pipeline, typeIds) {
  if (!Array.isArray(steps) || steps.length !== 3) {
    throw new NodeLocalDiagnostic('UNSUPPORTED_STEP_SHAPE', 'expected three ordered unary steps');
  }
  const stepIds = new Set();
  const handlerNames = new Set();
  const boundaries = new Set([pipeline.inputType]);
  let preceding = pipeline.inputType;
  for (const step of steps) {
    requireRecord(step, ['id', 'inputType', 'outputType', 'handler', 'retryLimit'],
      'UNSUPPORTED_STEP', 'expected a closed unary step binding');
    if (!matches(step.id, identifier) || stepIds.has(step.id)) {
      throw new NodeLocalDiagnostic('INVALID_STEP_ID', 'step identifiers must be valid and distinct');
    }
    if (!matches(step.handler, identifier) || handlerNames.has(step.handler)) {
      throw new NodeLocalDiagnostic('AMBIGUOUS_HANDLER', `invalid or repeated binding for ${step.id}`);
    }
    // All admitted types are named records. The compiler profile excludes unions and aliases.
    if (step.inputType !== preceding || !typeIds.has(step.outputType) || boundaries.has(step.outputType)) {
      throw new NodeLocalDiagnostic('TYPE_MISMATCH', `invalid nominal boundary at ${step.id}`);
    }
    if (!Number.isInteger(step.retryLimit) || step.retryLimit < 0 || step.retryLimit > 2) {
      throw new NodeLocalDiagnostic('UNSUPPORTED_RETRY', `expected retry budget 0, 1 or 2 at ${step.id}`);
    }
    stepIds.add(step.id);
    handlerNames.add(step.handler);
    boundaries.add(step.outputType);
    preceding = step.outputType;
  }
  if (!typeIds.has(pipeline.inputType) || preceding !== pipeline.outputType) {
    throw new NodeLocalDiagnostic('TYPE_MISMATCH', 'pipeline endpoints do not match the ordered step boundaries');
  }
  return handlerNames;
}

export function admitBinding(binding) {
  const bound = snapshot(binding, 'UNSUPPORTED_BINDING');
  requireKeys(bound, ['schemaVersion', 'projectionJson', 'projectionSha256', 'handlers'], 'UNSUPPORTED_BINDING');
  if (bound.schemaVersion !== 1) throw new NodeLocalDiagnostic('UNSUPPORTED_SCHEMA', 'expected binding schema 1');
  if (typeof bound.projectionJson !== 'string' || !matches(bound.projectionSha256, digest)
      || createHash('sha256').update(bound.projectionJson, 'utf8').digest('hex') !== bound.projectionSha256) {
    throw new NodeLocalDiagnostic('ARTIFACT_DIGEST_MISMATCH', 'retrieve a matching generated application and projection');
  }

  const projection = parseProjection(bound.projectionJson);
  requireRecord(projection, ['schemaVersion', 'profile', 'kind', 'pipeline', 'requiredCapabilities',
    'types', 'steps', 'contextKeys', 'policy', 'provenance'], 'UNSUPPORTED_PROJECTION', 'unexpected projection fields');
  if (projection.schemaVersion !== 1) throw new NodeLocalDiagnostic('UNSUPPORTED_SCHEMA', 'expected projection schema 1');
  if (projection.profile !== profile || projection.kind !== 'compiler-node-local') {
    throw new NodeLocalDiagnostic('UNSUPPORTED_PROFILE', `expected compiler-node-local and ${profile}`);
  }
  requireList(projection.requiredCapabilities, capabilities, 'UNSUPPORTED_CAPABILITY', 'capabilities differ from Node LOCAL');
  requireList(projection.contextKeys, contextKeys, 'UNSUPPORTED_CONTEXT', 'expected executionId and traceId');
  requireConstants(projection.policy, policy, 'UNSUPPORTED_POLICY', 'policy differs from Node LOCAL');
  const pipeline = projection.pipeline;
  requireRecord(pipeline, ['id', 'inputType', 'outputType', 'transport', 'platform', 'execution'],
    'UNSUPPORTED_PIPELINE', 'unexpected pipeline fields');
  if (!matches(pipeline.id, pipelineId)) throw new NodeLocalDiagnostic('INVALID_PIPELINE_ID');
  if (pipeline.transport !== 'LOCAL' || pipeline.platform !== 'COMPUTE' || pipeline.execution !== 'DIRECT') {
    throw new NodeLocalDiagnostic('UNSUPPORTED_EXECUTION', 'expected LOCAL / COMPUTE / DIRECT');
  }
  requireRecord(projection.provenance, ['dslSha256', 'bindingsSha256'], 'INVALID_PROVENANCE', 'expected input digests');
  if (!Object.values(projection.provenance).every(value => matches(value, digest))) {
    throw new NodeLocalDiagnostic('INVALID_PROVENANCE', 'input digests must be lowercase SHA-256');
  }

  const names = admitSteps(projection.steps, pipeline, admitTypes(projection.types));
  const handlers = snapshot(bound.handlers, 'INVALID_HANDLER_REGISTRY', 'expected own handler functions');
  for (const name of names) {
    if (!Object.hasOwn(handlers, name) || typeof handlers[name] !== 'function') {
      throw new NodeLocalDiagnostic('MISSING_HANDLER', `register handler ${name}`);
    }
  }
  if (Object.keys(handlers).length !== names.size) {
    throw new NodeLocalDiagnostic('EXTRA_HANDLER', 'register only handlers declared by the compiler');
  }

  return {
    pipelineId: pipeline.id,
    steps: projection.steps.map(step => ({...step, invoke: handlers[step.handler]})),
  };
}
