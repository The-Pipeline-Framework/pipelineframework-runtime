import { createHash, createHmac, timingSafeEqual } from 'node:crypto';
import { createServer } from 'node:https';
import { createNodeLocalRuntime, NodeLocalDiagnostic, PolyglotInt32PayloadBinding } from './index.mjs';

const executePath = '/pipeline/worker/transitions/execute';
const capabilitiesPath = '/pipeline/worker/capabilities';
const profiles = {
  outerEncoding: 'application/tpf-transition-envelope+json',
  payloadEncoding: 'tpf-polyglot-int32-json/1',
  executionProfile: 'tpf-polyglot-unary-policy/1',
  resultProfile: 'tpf-polyglot-failed-result/1',
};
const diagnostics = new Map([
  ['TRANSIENT', ['TRANSIENT_EXHAUSTED', 'Local retry budget exhausted']],
  ['BUSINESS_REJECTION', ['BUSINESS_REJECTION', 'Business function rejected input']],
  ['UNCLASSIFIED', ['UNCLASSIFIED', 'Unclassified business function failure']],
]);
const range = {currentStepIndex: 0n, stopBeforeStepIndex: -1n, resultShape: 'SINGLE',
  redriveIntent: 'REPLAY', redriveStepIndex: -1n};
const invocationFields = ['executionId', 'traceId', 'transitionKey', 'attempt', 'executionVersion',
  'payloadTypeId', 'payloadEncoding', 'payload'];
const emptyFields = new Set(['redriveCommandId', 'redriveReason', 'pageContext', 'awaitOccurrenceCause']);
const bodyLimit = 16 * 1024;
const intakeTimeout = 10_000;
const tolerance = 120_000;
const nonceLimit = 10_000;
const utf8 = new TextDecoder('utf-8', {fatal: true, ignoreBOM: true});
const javaNonBlank = /[^\u0009-\u000d\u001c-\u0020\u1680\u2000-\u2006\u2008-\u200a\u2028\u2029\u205f\u3000]/u;

class RejectedRequest extends Error {
  constructor(status) {
    super();
    this.status = status;
  }
}

function readCommand(body, pins, payloads, inputType) {
  const source = utf8.decode(body);
  // Scan JSON tokens for duplicates before JSON.parse can discard them. The
  // envelope is a single flat object; the application payload stays a string.
  const keys = new Set();
  let objects = 0;
  for (const [token, string, colon] of source.matchAll(/("(?:\\.|[^"\\])*")([ \t\r\n]*:)?|[\[\]{]/g)) {
    if (token === '[' || token === ']' || (token === '{' && ++objects > 1)) throw new RejectedRequest(400);
    if (!colon) continue;
    const key = JSON.parse(string);
    if (keys.has(key)) throw new RejectedRequest(400);
    keys.add(key);
  }
  // Keep Java's signed 64-bit executionVersion exact. Numeric fields in the
  // coordinator's wire envelope are decimal integers, never exponent/fraction forms.
  const command = JSON.parse(source, (_key, value, context) => {
    if (typeof value === 'string' && !value.isWellFormed()) throw new RejectedRequest(400);
    if (typeof value !== 'number') return value;
    if (!/^(?:0|-?[1-9][0-9]{0,18})$/.test(context.source)) throw new RejectedRequest(400);
    return BigInt(context.source);
  });
  if (command === null || typeof command !== 'object' || Array.isArray(command)) throw new RejectedRequest(400);
  const {executionId, traceId, transitionKey, attempt, executionVersion, payloadTypeId, payloadEncoding, payload} = command;
  const fixed = {...pins, ...range};
  if (Object.entries(fixed).some(([key, value]) => !Object.hasOwn(command, key) || command[key] !== value)
      || invocationFields.some(key => !Object.hasOwn(command, key))
      || Object.keys(command).some(key => !Object.hasOwn(fixed, key) && !invocationFields.includes(key)
        && (!emptyFields.has(key) || command[key] !== null))
      || [executionId, traceId].some(value => typeof value !== 'string' || !/[^ \t\r\n]/.test(value))
      || typeof transitionKey !== 'string' || !javaNonBlank.test(transitionKey)
      || typeof attempt !== 'bigint' || attempt < 0n || attempt > 2147483647n
      || typeof executionVersion !== 'bigint' || executionVersion < 0n || executionVersion > 9223372036854775807n) {
    throw new RejectedRequest(400);
  }
  const value = payloads.decode(inputType, {payloadTypeId, payloadEncoding, payload});
  return {input: {value}, context: {executionId, traceId}};
}

function parseTimestamp(value) {
  const match = /^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?Z$/.exec(value ?? '');
  if (!match) return NaN;
  const epoch = Date.parse(`${match[1]}.${(match[2] ?? '').padEnd(3, '0').slice(0, 3)}Z`);
  // Date.parse can normalise impossible calendar dates.
  return Number.isFinite(epoch) && new Date(epoch).toISOString().startsWith(`${match[1]}.`) ? epoch : NaN;
}

function authenticateWith(sharedSecret) {
  const key = Buffer.from(sharedSecret, 'utf8');
  const accepted = new Map();
  let oldest = -Infinity;
  let earliestNonce = Infinity;
  return (method, path, headers, body) => {
    const timestamp = headers.get('x-tpf-worker-timestamp');
    const nonce = headers.get('x-tpf-worker-nonce');
    const signature = headers.get('x-tpf-worker-signature');
    const issued = parseTimestamp(timestamp);
    const now = Date.now();
    if (!Number.isFinite(issued) || Math.abs(now - issued) > tolerance
        || !/^[!-~]{1,128}$/.test(nonce ?? '') || !/^[a-f0-9]{64}$/.test(signature ?? '')) {
      throw new RejectedRequest(401);
    }
    const digest = createHash('sha256').update(body).digest('hex');
    const canonical = `${method}\n${path}\n${timestamp}\n${nonce}\n${digest}`;
    const expected = createHmac('sha256', key).update(canonical, 'utf8').digest();
    if (!timingSafeEqual(expected, Buffer.from(signature, 'hex'))) throw new RejectedRequest(401);

    // Preserve live nonces at capacity and never revive an expired timestamp on clock rollback.
    oldest = Math.max(oldest, now - tolerance);
    if (earliestNonce < oldest) {
      earliestNonce = Infinity;
      for (const [seen, at] of accepted) {
        if (at < oldest) accepted.delete(seen);
        else earliestNonce = Math.min(earliestNonce, at);
      }
    }
    if (issued < oldest || accepted.has(nonce) || accepted.size === nonceLimit) throw new RejectedRequest(401);
    accepted.set(nonce, issued);
    earliestNonce = Math.min(earliestNonce, issued);
  };
}

function requestHeaders(request) {
  const headers = new Map();
  for (let index = 0; index < request.rawHeaders.length; index += 2) {
    const name = request.rawHeaders[index].toLowerCase();
    if (headers.has(name)) throw new RejectedRequest(400);
    headers.set(name, request.rawHeaders[index + 1]);
  }
  if (['content-encoding', 'transfer-encoding', 'expect'].some(name => headers.has(name))) {
    throw new RejectedRequest(400);
  }
  return headers;
}

async function readBody(request, headers, execute) {
  const length = headers.get('content-length') ?? (execute ? '' : '0');
  if (!/^(?:0|[1-9][0-9]{0,19})$/.test(length)) throw new RejectedRequest(400);
  const count = Number(length);
  if (count > bodyLimit) throw new RejectedRequest(413);
  if (!execute && count !== 0) throw new RejectedRequest(400);
  if (execute && headers.get('content-type')?.toLowerCase() !== 'application/json') throw new RejectedRequest(415);
  const chunks = [];
  let size = 0;
  for await (const chunk of request) {
    size += chunk.length;
    if (size > count) throw new RejectedRequest(400);
    chunks.push(chunk);
  }
  if (!request.complete || size !== count) throw new RejectedRequest(400);
  return Buffer.concat(chunks, count);
}

function wireResult(result, payloads, outputType, steps) {
  if (result.kind === 'COMPLETED') {
    return {outcome: 'COMPLETED', outputPayloads: [payloads.encode(outputType, result.outcome.value.value)],
      terminalOutputPublished: false, terminalInputPassthrough: false};
  }
  if (result.kind === 'FAILED' && steps.has(result.failure.stepId)) {
    const diagnostic = diagnostics.get(result.failure.code);
    if (diagnostic) return {outcome: 'FAILED', resultProfile: profiles.resultProfile,
      failure: {code: diagnostic[0], diagnostic: diagnostic[1], stepId: result.failure.stepId}};
  }
  throw new Error('Unsupported native terminal result');
}

function respond(response, status, value) {
  if (response.destroyed) return;
  const body = value === undefined ? Buffer.alloc(0) : Buffer.from(JSON.stringify(value), 'utf8');
  response.writeHead(status, {'Content-Type': 'application/json', 'Content-Length': body.length, Connection: 'close'});
  response.end(body);
}

/** Create an HTTPS listener from deployment-verified code and Compiled Truth. */
export function createNativeTransitionWorker({bind, expectedProjection, tenantId, pipelineId, contractVersion,
  releaseVersion, artifactId, artifactDigest, profiles: selectedProfiles, sharedSecret, key, cert, onObservation}) {
  const pins = {tenantId, pipelineId, contractVersion, releaseVersion};
  const text = [...Object.values(pins), artifactId, sharedSecret];
  const digest = /^sha256:[0-9a-f]{64}$/;
  if (text.some(value => typeof value !== 'string' || !value.isWellFormed() || !javaNonBlank.test(value))
      || !digest.test(contractVersion) || typeof artifactDigest !== 'string' || !digest.test(artifactDigest)
      || !Buffer.isBuffer(expectedProjection) || typeof bind !== 'function' || !key || !cert
      || selectedProfiles === null || typeof selectedProfiles !== 'object'
      || Object.keys(selectedProfiles).length !== Object.keys(profiles).length
      || Object.entries(profiles).some(([field, value]) => !Object.hasOwn(selectedProfiles, field)
        || selectedProfiles[field] !== value)) {
    throw new TypeError('Invalid native worker deployment binding');
  }
  const expectedProjectionJson = utf8.decode(expectedProjection);
  const runtime = createNodeLocalRuntime({expectedProjectionJson, onObservation});
  let application;
  bind(Object.freeze({load(binding) {
    if (application) throw new TypeError('Expected one prepared native application');
    application = runtime.load(binding);
    return application;
  }}));
  if (!application) throw new TypeError('Expected one prepared native application');
  // The existing runtime already admitted these exact bytes and all their nominal boundaries.
  const projection = JSON.parse(expectedProjectionJson);
  if (projection.pipeline.id !== pipelineId || projection.profile !== profiles.executionProfile) {
    throw new TypeError('Prepared application does not match the native worker deployment binding');
  }
  const {inputType, outputType} = projection.pipeline;
  const payloads = new PolyglotInt32PayloadBinding(...projection.types.map(type => type.id));
  const steps = new Set(projection.steps.map(step => step.id));
  const capabilities = {protocolVersion: '1', providerName: 'rest', pipelineId, contractVersion, releaseVersion,
    artifactId, artifactDigest, payloadEncodings: [profiles.outerEncoding], workerProtocols: ['local', 'rest']};
  const authenticate = authenticateWith(sharedSecret);

  const server = createServer({key, cert, minVersion: 'TLSv1.2', maxHeaderSize: 8192,
    requestTimeout: intakeTimeout, headersTimeout: intakeTimeout, handshakeTimeout: intakeTimeout,
    connectionsCheckingInterval: 1000}, async (request, response) => {
    const execute = request.method === 'POST' && request.url === executePath;
    if (!execute && !(request.method === 'GET' && request.url === capabilitiesPath)) {
      respond(response, 404);
      return;
    }

    let invocation;
    try {
      const headers = requestHeaders(request);
      const body = await readBody(request, headers, execute);
      authenticate(request.method, request.url, headers, body);
      if (execute) invocation = readCommand(body, pins, payloads, inputType);
    } catch (error) {
      const malformed = error instanceof NodeLocalDiagnostic || error instanceof SyntaxError || error instanceof TypeError;
      respond(response, error instanceof RejectedRequest ? error.status : (malformed ? 400 : 500));
      return;
    }
    if (!execute) {
      respond(response, 200, capabilities);
      return;
    }
    try {
      // Once admitted, losing the HTTP client does not cancel or retry business execution.
      const result = await application.run(invocation.input, invocation.context);
      respond(response, 200, wireResult(result, payloads, outputType, steps));
    } catch {
      respond(response, 500);
    }
  });
  server.maxHeadersCount = 0; // The byte limit applies to every header, including duplicates.
  server.maxConnections = 16;
  server.maxRequestsPerSocket = 1;
  server.on('checkContinue', (_request, response) => respond(response, 400));
  server.on('checkExpectation', (_request, response) => respond(response, 400));
  return server;
}
