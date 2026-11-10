import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import test from 'node:test';
import { createNodeLocalRuntime, NodeLocalDiagnostic, StructuredFailure } from '@tpf/experimental-node-local';

const context = {executionId: 'execution', traceId: 'trace'};
const value = n => ({kind: 'VALUE', value: {value: n}});
const completed = n => ({kind: 'COMPLETED', outcome: value(n)});
const envelope = (stepId, message = 'try again') => ({
  code: 'TRANSIENT', retryable: true, owner: 'LOCAL_SHELL', stepId, message,
});
const digest = text => createHash('sha256').update(text, 'utf8').digest('hex');

function projection() {
  return {
    schemaVersion: 1,
    profile: 'tpf-polyglot-unary-policy/1',
    kind: 'compiler-node-local',
    pipeline: {id: 'test-app', inputType: 'Input', outputType: 'Output',
      transport: 'LOCAL', platform: 'COMPUTE', execution: 'DIRECT'},
    requiredCapabilities: ['pure', 'linear', 'unary', 'local', 'retry', 'cancellation', 'context'],
    types: ['Input', 'AfterA', 'AfterB', 'Output'].map(id => ({id, fields: [
      {name: 'value', type: 'int32', presence: 'REQUIRED', nullability: 'NON_NULL'},
    ]})),
    steps: [
      {id: 'A', inputType: 'Input', outputType: 'AfterA', handler: 'add', retryLimit: 0},
      {id: 'B', inputType: 'AfterA', outputType: 'AfterB', handler: 'scale', retryLimit: 1},
      {id: 'C', inputType: 'AfterB', outputType: 'Output', handler: 'subtract', retryLimit: 0},
    ],
    contextKeys: ['executionId', 'traceId'],
    policy: {failureMapping: 'structured-failure/1', retryOwner: 'LOCAL_SHELL',
      resultMapping: 'explicit-value/1', cancellation: 'first-accepted-terminal/1',
      context: 'explicit-snapshot/1', recovery: 'REJECT', legacyPolicy: 'REJECT'},
    provenance: {dslSha256: 'a'.repeat(64), bindingsSha256: 'b'.repeat(64)},
  };
}

function binding(handlers = {}, generated = projection()) {
  const projectionJson = typeof generated === 'string' ? generated : JSON.stringify(generated);
  return {
    schemaVersion: 1,
    projectionJson,
    projectionSha256: digest(projectionJson),
    handlers: {
      add: ({value: n}) => value(n + 1),
      scale: ({value: n}) => value(n * 2),
      subtract: ({value: n}) => value(n - 3),
      ...handlers,
    },
  };
}

function assertDiagnostic(action, code) {
  assert.throws(action, error => error instanceof NodeLocalDiagnostic && error.code === code);
}

test('load pins exact projection bytes, exact numbers and unique JSON fields before admitting handlers', () => {
  const runtime = createNodeLocalRuntime();
  const original = binding();
  assertDiagnostic(() => runtime.load({...original, projectionJson: original.projectionJson + '\n'}),
    'ARTIFACT_DIGEST_MISMATCH');
  assertDiagnostic(() => runtime.load({...original, schemaVersion: 2}), 'UNSUPPORTED_SCHEMA');
  assertDiagnostic(() => runtime.load(binding({}, '{')), 'INVALID_JSON');
  const duplicate = original.projectionJson.replace('"schemaVersion":1', '"schemaVersion":2,"schemaVers\\u0069on":1');
  assertDiagnostic(() => runtime.load(binding({}, duplicate)), 'INVALID_JSON');
  for (const token of ['1.0000000000000001', '0.99999999999999999', 'true', '"1"']) {
    const json = original.projectionJson.replace('"retryLimit":1', `"retryLimit":${token}`);
    assertDiagnostic(() => runtime.load(binding({}, json)),
      /^[10]\./.test(token) ? 'INVALID_NUMBER' : 'UNSUPPORTED_RETRY');
  }
  const exact = original.projectionJson.replace('"retryLimit":1', '"retryLimit":10e-1');
  assert.equal(typeof runtime.load(binding({}, exact)).run, 'function');
});

test('load rejects incompatible features and mismatched nominal identity even with a matching digest', () => {
  const runtime = createNodeLocalRuntime();
  const mutations = [
    ['UNSUPPORTED_PROFILE', p => { p.kind = 'compiler-projection-probe'; }],
    ['UNSUPPORTED_PROFILE', p => { p.profile = 'future/2'; }],
    ['UNSUPPORTED_CAPABILITY', p => p.requiredCapabilities.push('blocking')],
    ['UNSUPPORTED_CONTEXT', p => p.contextKeys.push('credentials')],
    ['UNSUPPORTED_POLICY', p => { p.policy.retryOwner = 'REMOTE'; }],
    ['UNSUPPORTED_POLICY', p => { p.policy.recovery = 'ALLOW'; }],
    ['UNSUPPORTED_EXECUTION', p => { p.pipeline.transport = 'GRPC'; }],
    ['UNSUPPORTED_EXECUTION', p => { p.pipeline.execution = 'QUEUED'; }],
    ['UNSUPPORTED_STEP', p => { p.steps[0].blocking = true; }],
    ['UNSUPPORTED_STEP_SHAPE', p => p.steps.pop()],
    ['UNSUPPORTED_RETRY', p => { p.steps[1].retryLimit = 3; }],
    ['UNSUPPORTED_TYPE', p => { p.types[0].fields[0].nullability = 'NULLABLE'; }],
    ['TYPE_MISMATCH', p => { p.steps[1].inputType = 'Input'; }],
    ['TYPE_MISMATCH', p => { p.steps[2].outputType = 'AfterA'; }],
    ['TYPE_MISMATCH', p => { p.pipeline.outputType = 'AfterB'; }],
    ['AMBIGUOUS_HANDLER', p => { p.steps[1].handler = p.steps[0].handler; }],
    ['INVALID_STEP_ID', p => { p.steps[1].id = p.steps[0].id; }],
    ['INVALID_PROVENANCE', p => { p.provenance.dslSha256 = 'x'; }],
  ];
  for (const [code, mutate] of mutations) {
    const changed = projection();
    mutate(changed);
    assertDiagnostic(() => runtime.load(binding({}, changed)), code);
  }
  assertDiagnostic(() => createNodeLocalRuntime({legacyRetry: true}), 'UNSUPPORTED_RUNTIME_OPTIONS');
  assertDiagnostic(() => createNodeLocalRuntime(null), 'UNSUPPORTED_RUNTIME_OPTIONS');
});

test('the registry must contain exactly the declared own data properties, and is captured at load', async () => {
  const runtime = createNodeLocalRuntime();
  assertDiagnostic(() => runtime.load(binding({scale: undefined})), 'MISSING_HANDLER');
  assertDiagnostic(() => runtime.load(binding({extra: () => {}})), 'EXTRA_HANDLER');
  const original = binding();
  assertDiagnostic(() => runtime.load({...original, handlers: Object.create(original.handlers)}), 'MISSING_HANDLER');
  const accessor = binding();
  Object.defineProperty(accessor.handlers, 'scale', {get: () => assert.fail('loader invoked getter')});
  assertDiagnostic(() => runtime.load(accessor), 'INVALID_HANDLER_REGISTRY');
  const app = runtime.load(original);
  original.handlers.scale = () => assert.fail('runtime invoked replacement');
  assert.deepEqual(await app.run({value: 3}, context), completed(5));
});

test('the compiler order and boundary IDs are observed, and B retries with exactly its original input', async () => {
  const calls = [];
  const events = [];
  let attempt = 0;
  const handlers = {
    add: input => {
      calls.push(['A', input.value]);
      return value(input.value + 1);
    },
    scale: async input => {
      calls.push(['B', input.value]);
      assert(Object.isFrozen(input));
      if (++attempt === 1) throw new StructuredFailure(envelope('B'));
      return value(input.value * 2);
    },
    subtract: input => {
      calls.push(['C', input.value]);
      return value(input.value - 3);
    },
  };
  const runtime = createNodeLocalRuntime({onObservation: event => events.push(event)});
  const app = runtime.load(binding(handlers));
  assert.deepEqual(await app.run({value: 3}, context), completed(5));
  assert.deepEqual(calls, [['A', 3], ['B', 4], ['B', 4], ['C', 8]]);
  assert.deepEqual(events.filter(event => event.kind === 'ATTEMPT_STARTED')
    .map(({stepId, inputType, outputType, attempt: n}) => [stepId, inputType, outputType, n]),
  [['A', 'Input', 'AfterA', 1], ['B', 'AfterA', 'AfterB', 1], ['B', 'AfterA', 'AfterB', 2],
    ['C', 'AfterB', 'Output', 1]]);
  assert.equal(events.filter(event => event.kind === 'RETRY_SCHEDULED').length, 1);
  assert.equal(events.filter(event => event.kind === 'TERMINAL').length, 1);
  assert.deepEqual(events.at(-1).record, completed(5));
  assert(events.every(Object.isFrozen));

  calls.length = 0;
  const reordered = projection();
  const [a, b, c] = reordered.steps;
  reordered.steps = [{...b, inputType: 'Input', outputType: 'AfterA'},
    {...a, inputType: 'AfterA', outputType: 'AfterB'}, c];
  assert.deepEqual(await runtime.load(binding(handlers, reordered)).run({value: 3}, context), completed(4));
  assert.deepEqual(calls, [['B', 3], ['A', 6], ['C', 7]]);
});

test('exhaustion retains the last bound envelope; no failure starts the next step', async () => {
  let attempts = 0;
  const app = createNodeLocalRuntime().load(binding({
    scale: () => { throw new StructuredFailure(envelope('B', `attempt ${++attempts}`)); },
    subtract: () => assert.fail('C ran after terminal failure'),
  }));
  const record = await app.run({value: 3}, context);
  assert.deepEqual(record, {kind: 'FAILED', failure: envelope('B', 'attempt 2')});
  assert(Object.isFrozen(record.failure));
  assert.equal(attempts, 2);
});

test('only a validated branded structured failure is trusted', async () => {
  const rejection = {...envelope('A'), code: 'BUSINESS_REJECTION', retryable: false, message: 'declined'};
  const unclassified = {...rejection, code: 'UNCLASSIFIED', message: 'Unclassified business function failure'};
  const throwingGetter = {...envelope('A'), get message() { assert.fail('runtime invoked getter'); }};
  const revoked = Proxy.revocable({}, {});
  revoked.revoke();
  for (const [failure, expected] of [
    [new StructuredFailure(rejection), {kind: 'FAILED', failure: rejection}],
    [new StructuredFailure(unclassified), {kind: 'FAILED', failure: unclassified}],
    [new Error('sensitive'), {kind: 'FAILED', failure: unclassified}],
    [envelope('A'), {kind: 'FAILED', failure: unclassified}],
    [revoked.proxy, {kind: 'FAILED', failure: unclassified}],
    [new StructuredFailure({code: 'TRANSIENT'}), {kind: 'REJECTED', diagnostic: 'MALFORMED_FAILURE'}],
    [new StructuredFailure(throwingGetter), {kind: 'REJECTED', diagnostic: 'MALFORMED_FAILURE'}],
    [new StructuredFailure({...envelope('A'), code: 'UNKNOWN'}), {kind: 'REJECTED', diagnostic: 'UNKNOWN_FAILURE_CODE'}],
    [new StructuredFailure({...envelope('A'), retryable: false}), {kind: 'REJECTED', diagnostic: 'FAILURE_BINDING_MISMATCH'}],
    [new StructuredFailure({...envelope('A'), owner: 'REMOTE'}), {kind: 'REJECTED', diagnostic: 'FAILURE_BINDING_MISMATCH'}],
    [new StructuredFailure(envelope('B')), {kind: 'REJECTED', diagnostic: 'FAILURE_BINDING_MISMATCH'}],
  ]) {
    let downstream = 0;
    const app = createNodeLocalRuntime().load(binding({
      add: () => { throw failure; },
      scale: () => { downstream += 1; return value(0); },
    }));
    assert.deepEqual(await app.run({value: 3}, context), expected);
    assert.equal(downstream, 0);
  }
});

test('only explicit non-null int32 records are accepted as input and return values', async () => {
  const runtime = createNodeLocalRuntime();
  const direct = runtime.load(binding({add: input => ({kind: 'VALUE', value: input}),
    scale: input => ({kind: 'VALUE', value: input}), subtract: input => ({kind: 'VALUE', value: input})}));
  for (const n of [-2147483648, 2147483647, 0]) {
    assert.deepEqual(await direct.run({value: n}, context), completed(n));
  }
  const invalid = {kind: 'REJECTED', diagnostic: 'INVALID_VALUE'};
  for (const input of [null, {}, {value: -0}, {value: 0.5}, {value: 2147483648}, {value: -2147483649},
    {value: Infinity}, {value: true}, {value: 3, extra: true}, {get value() { assert.fail('getter called'); }}]) {
    assert.deepEqual(await direct.run(input, context), invalid);
  }
  const thenable = {kind: 'VALUE', value: {value: 1}, get then() { assert.fail('then getter called'); }};
  for (const returned of [null, undefined, {value: 3}, {kind: 'VALUE'}, {kind: 'VALUE', value: null}, thenable]) {
    const app = runtime.load(binding({add: () => returned}));
    assert.deepEqual(await app.run({value: 3}, context), invalid);
  }
  const empty = runtime.load(binding({add: () => ({kind: 'EMPTY'})}));
  assert.deepEqual(await empty.run({value: 3}, context), {kind: 'REJECTED', diagnostic: 'UNSUPPORTED_EMPTY'});
});

test('cancellation records the winner while an in-flight handler drains, even if that handler fails', async () => {
  const controller = new AbortController();
  const gate = Promise.withResolvers();
  const events = [];
  const calls = [];
  const app = createNodeLocalRuntime({onObservation: event => events.push(event)}).load(binding({
    add: () => { calls.push('A'); return gate.promise; },
    scale: () => { calls.push('B'); return value(0); },
  }));
  let delivered = false;
  const pending = app.run({value: 3}, context, controller.signal).then(record => {
    delivered = true;
    return record;
  });
  // An unrelated listener cannot hide an accepted AbortSignal event from the runtime.
  controller.signal.addEventListener('abort', event => event.stopImmediatePropagation(), {capture: true});
  controller.abort();
  const cancelled = {kind: 'CANCELLED'};
  assert.deepEqual(events.at(-1).record, cancelled);
  await Promise.resolve();
  assert.equal(delivered, false);
  gate.reject(new StructuredFailure(envelope('A')));
  assert.deepEqual(await pending, cancelled);
  assert.deepEqual(calls, ['A']);
  assert.deepEqual(events.map(event => event.kind), ['ATTEMPT_STARTED', 'TERMINAL']);
});

test('a synthetic abort event neither cancels execution nor consumes genuine cancellation', async () => {
  for (const cancel of [false, true]) {
    const controller = new AbortController();
    const gate = Promise.withResolvers();
    const events = [];
    const runtime = createNodeLocalRuntime({onObservation: event => events.push(event)});
    const app = runtime.load(binding({add: () => gate.promise}));
    const pending = app.run({value: 3}, context, controller.signal);

    controller.signal.dispatchEvent(new Event('abort'));
    assert.equal(controller.signal.aborted, false);
    assert.deepEqual(events.map(event => event.kind), ['ATTEMPT_STARTED']);
    if (cancel) controller.abort();
    gate.resolve(value(4));

    const expected = cancel ? {kind: 'CANCELLED'} : completed(5);
    assert.deepEqual(await pending, expected);
    assert.deepEqual(events.filter(event => event.kind === 'ATTEMPT_STARTED')
      .map(event => event.stepId), cancel ? ['A'] : ['A', 'B', 'C']);
    assert.deepEqual(events.filter(event => event.kind === 'TERMINAL').map(event => event.record), [expected]);
  }
});

test('cancellation before handoff or retry prevents dispatch, while a committed completion wins', async () => {
  for (const [cancelOn, stepId, expectedCalls, expected] of [
    ['RETURNED', 'A', ['A'], {kind: 'CANCELLED'}],
    ['RETRY_SCHEDULED', 'B', ['A', 'B'], {kind: 'CANCELLED'}],
    ['RETURNED', 'C', ['A', 'B', 'C'], completed(5)],
  ]) {
    const controller = new AbortController();
    const calls = [];
    const terminals = [];
    const runtime = createNodeLocalRuntime({onObservation: event => {
      if (event.kind === cancelOn && event.stepId === stepId) controller.abort();
      if (event.kind === 'TERMINAL') terminals.push(event.record);
    }});
    const app = runtime.load(binding({
      add: ({value: n}) => { calls.push('A'); return value(n + 1); },
      scale: ({value: n}) => {
        calls.push('B');
        if (cancelOn === 'RETRY_SCHEDULED') throw new StructuredFailure(envelope('B'));
        return value(n * 2);
      },
      subtract: ({value: n}) => { calls.push('C'); return value(n - 3); },
    }));
    assert.deepEqual(await app.run({value: 3}, context, controller.signal), expected);
    assert.deepEqual(calls, expectedCalls);
    assert.deepEqual(terminals, [expected]);
  }
});

test('pre-aborted and invalid invocations dispatch no handler', async () => {
  let calls = 0;
  const app = createNodeLocalRuntime().load(binding({
    add: () => { calls += 1; return value(0); },
  }));
  assert.deepEqual(await app.run({value: 3}, context, AbortSignal.abort()), {kind: 'CANCELLED'});
  assert.deepEqual(await app.run({value: 3}, context, {}), {kind: 'REJECTED', diagnostic: 'INVALID_SIGNAL'});
  for (const caller of [null, {executionId: 'one'}, {executionId: ' \t\r\n', traceId: 'trace'},
    {...context, credentials: true}]) {
    assert.deepEqual(await app.run({value: 3}, caller), {kind: 'REJECTED', diagnostic: 'INVALID_CONTEXT'});
  }
  assert.equal(calls, 0);
});

test('overlapping invocations keep immutable input and context snapshots across async work and retry', async () => {
  const left = Promise.withResolvers();
  const right = Promise.withResolvers();
  const attempted = [];
  const contexts = [];
  const leftInput = {value: 3};
  const leftContext = {executionId: ' left ', traceId: '\u00a0'};
  const rightContext = {executionId: 'right', traceId: ' trace-right '};
  let leftRetries = 0;
  const runtime = createNodeLocalRuntime({onObservation: event => {
    if (event.kind === 'TERMINAL') contexts.push(event.context);
  }});
  const app = runtime.load(binding({
    add: async (input, ctx) => {
      await (ctx.executionId === ' left ' ? left.promise : right.promise);
      assert(Object.isFrozen(input));
      assert(Object.isFrozen(ctx));
      return value(input.value + 1);
    },
    scale: async (input, ctx) => {
      attempted.push([ctx.executionId, ctx.traceId, input.value]);
      if (ctx.executionId === ' left ' && leftRetries++ === 0) throw new StructuredFailure(envelope('B'));
      return value(input.value * 2);
    },
  }));
  const pendingLeft = app.run(leftInput, leftContext);
  const pendingRight = app.run({value: 9}, rightContext);
  leftInput.value = 100;
  leftContext.executionId = 'changed';
  rightContext.traceId = 'changed';
  right.resolve();
  assert.deepEqual(await pendingRight, completed(17));
  left.resolve();
  assert.deepEqual(await pendingLeft, completed(5));
  assert.deepEqual(attempted, [['right', ' trace-right ', 10], [' left ', '\u00a0', 4], [' left ', '\u00a0', 4]]);
  assert.deepEqual(contexts, [{executionId: 'right', traceId: ' trace-right '}, {executionId: ' left ', traceId: '\u00a0'}]);
});

test('observer failures and rejected promises do not affect the result', async () => {
  const runtime = createNodeLocalRuntime({onObservation: event => {
    if (event.kind === 'ATTEMPT_STARTED') throw new Error('observer failed');
    return Promise.reject(new Error('observer rejected'));
  }});
  assert.deepEqual(await runtime.load(binding()).run({value: 3}, context), completed(5));
});
