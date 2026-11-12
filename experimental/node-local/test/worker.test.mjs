import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { createHash, createHmac, randomUUID } from 'node:crypto';
import { once } from 'node:events';
import { mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { request } from 'node:https';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { after, before, test } from 'node:test';
import { StructuredFailure } from '@tpf/experimental-node-local';
import { createNativeTransitionWorker } from '@tpf/experimental-node-local/worker';

const executePath = '/pipeline/worker/transitions/execute';
const capabilitiesPath = '/pipeline/worker/capabilities';
const profiles = {outerEncoding: 'application/tpf-transition-envelope+json',
  payloadEncoding: 'tpf-polyglot-int32-json/1', executionProfile: 'tpf-polyglot-unary-policy/1',
  resultProfile: 'tpf-polyglot-failed-result/1'};
const pins = {tenantId: 'tenant', pipelineId: 'worker-test', contractVersion: `sha256:${'1'.repeat(64)}`,
  releaseVersion: 'release', artifactId: 'runnable', artifactDigest: `sha256:${'2'.repeat(64)}`};
const sharedSecret = 'worker-secret';
const typeIds = ['Input', 'First', 'Second', 'Output'];
const projectionJson = JSON.stringify({
  schemaVersion: 1, profile: profiles.executionProfile, kind: 'compiler-node-local',
  pipeline: {id: pins.pipelineId, inputType: 'Input', outputType: 'Output', transport: 'LOCAL',
    platform: 'COMPUTE', execution: 'DIRECT'},
  requiredCapabilities: ['pure', 'linear', 'unary', 'local', 'retry', 'cancellation', 'context'],
  types: typeIds.map(id => ({id, fields: [{name: 'value', type: 'int32', presence: 'REQUIRED', nullability: 'NON_NULL'}]})),
  steps: ['A', 'B', 'C'].map((id, index) => ({id, handler: `run${id}`, inputType: typeIds[index],
    outputType: typeIds[index + 1], retryLimit: index === 1 ? 1 : 0})),
  contextKeys: ['executionId', 'traceId'],
  policy: {failureMapping: 'structured-failure/1', retryOwner: 'LOCAL_SHELL', resultMapping: 'explicit-value/1',
    cancellation: 'first-accepted-terminal/1', context: 'explicit-snapshot/1', recovery: 'REJECT', legacyPolicy: 'REJECT'},
  provenance: {dslSha256: '0'.repeat(64), bindingsSha256: '1'.repeat(64)},
});
const hash = bytes => createHash('sha256').update(bytes).digest('hex');
const command = (changes = {}) => Buffer.from(JSON.stringify({tenantId: pins.tenantId, pipelineId: pins.pipelineId,
  contractVersion: pins.contractVersion, releaseVersion: pins.releaseVersion, executionId: 'execution', traceId: 'tracé',
  transitionKey: 'transition', attempt: 0, executionVersion: 2, currentStepIndex: 0, stopBeforeStepIndex: -1,
  resultShape: 'SINGLE', redriveIntent: 'REPLAY', redriveStepIndex: -1, redriveCommandId: null,
  redriveReason: null, pageContext: null, awaitOccurrenceCause: null, payloadTypeId: 'Input',
  payloadEncoding: profiles.payloadEncoding, payload: '{"value":3}', ...changes}));
const value = number => ({kind: 'VALUE', value: {value: number}});
const empty = Buffer.alloc(0);
let pemDirectory;
let key;
let cert;

before(() => {
  pemDirectory = mkdtempSync(join(tmpdir(), 'tpf-node-worker-'));
  const keyFile = join(pemDirectory, 'key.pem');
  const certFile = join(pemDirectory, 'cert.pem');
  execFileSync('openssl', ['req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '1',
    '-subj', '/CN=localhost', '-keyout', keyFile, '-out', certFile], {stdio: 'ignore'});
  key = readFileSync(keyFile);
  cert = readFileSync(certFile);
});
after(() => rmSync(pemDirectory, {recursive: true, force: true}));

function options(handlers = {}) {
  return {...pins, profiles, sharedSecret, key, cert, expectedProjection: Buffer.from(projectionJson),
    bind: runtime => runtime.load({schemaVersion: 1, projectionJson, projectionSha256: hash(projectionJson),
      handlers: {runA: input => value(input.value + 1), runB: input => value(input.value * 2),
        runC: input => value(input.value - 3), ...handlers}})};
}

async function listening(t, config) {
  const server = createNativeTransitionWorker(config);
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  t.after(() => new Promise((resolve, reject) => {
    server.close(error => error ? reject(error) : resolve());
    server.closeAllConnections();
  }));
  return server;
}

function signed(method, path, body, {timestamp = new Date().toISOString().replace('Z', '000001Z'),
  nonce = randomUUID(), signature} = {}) {
  const canonical = `${method}\n${path}\n${timestamp}\n${nonce}\n${hash(body)}`;
  return {'X-TPF-Worker-Timestamp': timestamp, 'X-TPF-Worker-Nonce': nonce,
    'X-TPF-Worker-Signature': signature ?? createHmac('sha256', sharedSecret).update(canonical).digest('hex')};
}

function send(server, method, path, body = empty, headers = signed(method, path, body)) {
  const fields = method === 'POST' ? {'Content-Type': 'application/json', 'Content-Length': body.length, ...headers} : headers;
  return new Promise((resolve, reject) => {
    const outgoing = request({host: '127.0.0.1', servername: 'localhost', port: server.address().port,
      method, path, ca: cert, headers: fields, agent: false}, async incoming => {
      try {
        const chunks = [];
        for await (const chunk of incoming) chunks.push(chunk);
        const source = Buffer.concat(chunks).toString('utf8');
        resolve({status: incoming.statusCode, body: source === '' ? undefined : JSON.parse(source)});
      } catch (error) { reject(error); }
    });
    outgoing.on('error', reject).end(body);
  });
}

test('startup verifies projection bytes, the pinned pipeline and each selected profile', () => {
  assert.throws(() => createNativeTransitionWorker({...options(), expectedProjection: Buffer.from(projectionJson + '\n')}),
    {code: 'TRUSTED_PROJECTION_MISMATCH'});
  assert.throws(() => createNativeTransitionWorker({...options(), pipelineId: 'elsewhere'}), /does not match/);
  for (const field of Object.keys(profiles)) {
    assert.throws(() => createNativeTransitionWorker({...options(), profiles: {...profiles, [field]: 'future/2'}}),
      /Invalid native worker deployment binding/);
  }
});

test('uses the Java golden raw-body signature and a bounded fail-closed nonce guard', async t => {
  let now = Date.parse('2026-09-14T10:15:30.000Z');
  t.mock.method(Date, 'now', () => now);
  const server = await listening(t, options());
  const golden = Buffer.from('{"outcome":"COMPLETED"}');
  const headers = signed('POST', executePath, golden, {timestamp: '2026-09-14T10:15:30Z', nonce: 'nonce-1',
    signature: 'e22366b38e2b071ccc7e31df9d7c4f7ad13d0b6ac17d2ef0b0f43e89cfb53523'});
  assert.equal((await send(server, 'POST', executePath, golden, headers)).status, 400);
  assert.equal((await send(server, 'POST', executePath, golden, headers)).status, 401);

  // Fill through the listener without opening ten thousand TLS connections.
  const attempt = (nonce, timestamp = '2026-09-14T10:15:30.000999999Z') => new Promise(resolve => {
    const rawHeaders = Object.entries(signed('GET', capabilitiesPath, empty,
      {timestamp, nonce})).flat();
    const incoming = {method: 'GET', url: capabilitiesPath, rawHeaders, complete: true, async *[Symbol.asyncIterator]() {}};
    const outgoing = {status: 0, writeHead(status) { this.status = status; }, end() { resolve(this.status); }};
    server.emit('request', incoming, outgoing);
  });
  for (let index = 0; index < 9999; index++) assert.equal(await attempt(`capacity-${index}`), 200);
  assert.equal(await attempt('one-too-many'), 401);
  assert.equal(await attempt('capacity-0'), 401);
  now += 120_000;
  assert.equal(await attempt('one-too-many'), 401);
  now++;
  assert.equal(await attempt('capacity-0'), 401);
  assert.equal(await attempt('new', new Date(now).toISOString()), 200);
  now -= 120_001;
  assert.equal(await attempt('capacity-0'), 401);
});

test('authenticates capabilities and rejects invalid commands before any business invocation', async t => {
  const calls = [];
  const server = await listening(t, {...options(), onObservation: event => {
    if (event.kind === 'ATTEMPT_STARTED') calls.push([event.stepId, event.context]);
  }});
  const {status, body} = await send(server, 'GET', capabilitiesPath);
  assert.equal(status, 200);
  assert.deepEqual(body, {protocolVersion: '1', providerName: 'rest', pipelineId: pins.pipelineId,
    contractVersion: pins.contractVersion, releaseVersion: pins.releaseVersion, artifactId: pins.artifactId,
    artifactDigest: pins.artifactDigest, payloadEncodings: [profiles.outerEncoding], workerProtocols: ['local', 'rest']});
  assert.equal((await send(server, 'GET', capabilitiesPath + '?query=1')).status, 404);
  assert.equal((await send(server, 'POST', executePath, command(), signed('POST', executePath, command({traceId: 'changed'})))).status, 401);
  assert.equal((await send(server, 'POST', executePath, command(), signed('POST', executePath, command(),
    {timestamp: new Date(Date.now() - 121_000).toISOString()}))).status, 401);
  const duplicates = {...signed('POST', executePath, command()), 'x-tpf-worker-nonce': ['first', 'second']};
  assert.equal((await send(server, 'POST', executePath, command(), duplicates)).status, 400);
  assert.equal((await send(server, 'POST', executePath, Buffer.alloc(16385))).status, 413);
  assert.equal((await send(server, 'POST', executePath, command(), {...signed('POST', executePath, command()),
    'Content-Encoding': 'gzip'})).status, 400);

  const source = command().toString();
  for (const invalid of [command({tenantId: 'other'}), command({pipelineId: 'other'}), command({contractVersion: 'other'}),
    command({releaseVersion: 'other'}), command({currentStepIndex: 1}), command({stopBeforeStepIndex: 2}),
    command({resultShape: 'MATERIALIZED_MULTI'}), command({redriveIntent: 'REISSUE_COMMAND'}), command({traceId: ' \t'}),
    command({awaitOccurrenceCause: 'cause'}), command({pageContext: {page: 2}}), command({resultProfile: 'spoofed'}),
    command({payloadTypeId: 'Second'}), command({payloadEncoding: 'json'}), command({payload: '{"value":1,"value":2}'}),
    command({payload: '{"value":2147483648}'}), command({attempt: true}), command({attempt: 2147483648}),
    Buffer.from(source.replace('"executionVersion":2', '"executionVersion":9223372036854775808')),
    Buffer.from(source.replace('"executionVersion":2', '"executionVersion":2.0000000000000001')),
    Buffer.from(source.replace('"tenantId":', '"tenantI\\u0064":"other","tenantId":')),
    Buffer.from(source.replace('"traceId":"tracé"', '"traceId":"\\ud800"')),
    Buffer.concat([Buffer.from(source.slice(0, -1) + ',"unknown":"'), Buffer.from([0xc0, 0xaf]), Buffer.from('"}')])]) {
    assert.equal((await send(server, 'POST', executePath, invalid)).status, 400, invalid.toString());
  }
  assert.deepEqual(calls, []);

  const exactLong = Buffer.from(source.replace('"executionVersion":2', '"executionVersion":9223372036854775807'));
  const headers = signed('POST', executePath, exactLong);
  assert.deepEqual(await send(server, 'POST', executePath, exactLong, headers), {status: 200, body: {
    outcome: 'COMPLETED', outputPayloads: [{payloadTypeId: 'Output', payloadEncoding: profiles.payloadEncoding, payload: '{"value":5}'}],
    terminalOutputPublished: false, terminalInputPassthrough: false}});
  assert.equal((await send(server, 'POST', executePath, exactLong, headers)).status, 401);
  assert.deepEqual(calls, ['A', 'B', 'C'].map(step => [step, {executionId: 'execution', traceId: 'tracé'}]));
});

test('serializes only the three safe portable failure outcomes from the existing local policy', async t => {
  const cases = [['TRANSIENT', 'TRANSIENT_EXHAUSTED', 'Local retry budget exhausted', 2],
    ['BUSINESS_REJECTION', 'BUSINESS_REJECTION', 'Business function rejected input', 1],
    ['raw', 'UNCLASSIFIED', 'Unclassified business function failure', 1]];
  for (const [original, code, diagnostic, attempts] of cases) {
    let calls = 0;
    const server = await listening(t, options({runB() {
      calls++;
      if (original === 'raw') throw new Error('private business detail');
      throw new StructuredFailure({code: original, message: 'private business detail', stepId: 'B',
        owner: 'LOCAL_SHELL', retryable: original === 'TRANSIENT'});
    }, runC() { assert.fail('business function after failed step'); }}));
    assert.deepEqual(await send(server, 'POST', executePath, command()), {status: 200, body: {
      outcome: 'FAILED', resultProfile: profiles.resultProfile, failure: {code, diagnostic, stepId: 'B'}}});
    assert.equal(calls, attempts);
  }
  const rejected = await listening(t, options({runB: () => ({kind: 'EMPTY'}),
    runC() { assert.fail('business function after invalid output'); }}));
  assert.deepEqual(await send(rejected, 'POST', executePath, command()), {status: 500, body: undefined});
});
