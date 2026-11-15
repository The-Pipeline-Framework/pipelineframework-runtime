import assert from 'node:assert/strict';
import test from 'node:test';
import { NodeLocalDiagnostic, PolyglotInt32PayloadBinding } from '@tpf/experimental-node-local';

const typeIds = ['InputValue', 'FirstValue', 'SecondValue', 'OutputValue'];
const binding = new PolyglotInt32PayloadBinding(...typeIds);
const input = binding.encode('InputValue', 1);

function rejects(code, run) {
  assert.throws(run, {name: NodeLocalDiagnostic.name, code});
}

test('round trips every nominal type as a compact int32 JSON record', () => {
  for (const typeId of typeIds) {
    for (const value of [-2147483648, -1, 0, 1, 2147483647]) {
      const encoded = binding.encode(typeId, value);
      assert.deepEqual(encoded, {
        payloadTypeId: typeId,
        payloadEncoding: 'tpf-polyglot-int32-json/1',
        payload: '{"value":' + value + '}',
      });
      assert.equal(binding.decode(typeId, encoded), value);
    }
  }
  assert.equal(binding.encode('InputValue', -0).payload, '{"value":0}');
  assert.equal(binding.decode('InputValue', {...input, payload: ' \r{\t"\\u0076a\\u006Cue":-0\n} '}), 0);
});

test('rejects decimal or exponent tokens, escaped duplicates, and non-JSON whitespace', () => {
  for (const payload of ['{"value":1.0}', '{"value":1e0}', '{"value":2147483648}',
    '{"value":-2147483649}', '{"value":null}', '{"value":1,"\\u0076alue":2}',
    '{"value":1}\u00a0', '\uFEFF{"value":1}']) {
    rejects('INVALID_PAYLOAD_VALUE', () => binding.decode('InputValue', {...input, payload}));
  }
  for (const value of [2147483648, -2147483649, false, '1', 1.1, NaN, Infinity, 1n, null]) {
    rejects('INVALID_PAYLOAD_VALUE', () => binding.encode('InputValue', value));
  }
});

test('checks the catalog, expected type and encoding before the body', () => {
  rejects('INVALID_PAYLOAD_CATALOG', () => new PolyglotInt32PayloadBinding('A', 'A', 'B', 'C'));
  rejects('INVALID_PAYLOAD_CATALOG', () => new PolyglotInt32PayloadBinding('A', 'B', 'C'));
  rejects('INVALID_PAYLOAD_CATALOG', () => new PolyglotInt32PayloadBinding('A', 'B', 'C', 'java.lang.Integer'));
  rejects('UNKNOWN_PAYLOAD_TYPE', () => binding.encode('String', 1));
  const malformed = {...input, payloadEncoding: 'application/tpf-transition-envelope+json', payload: 'invalid'};
  rejects('UNKNOWN_PAYLOAD_TYPE', () => binding.decode('String', malformed));
  rejects('PAYLOAD_TYPE_MISMATCH', () => binding.decode('FirstValue', malformed));
  rejects('PAYLOAD_ENCODING_MISMATCH', () => binding.decode('InputValue', malformed));
});

test('reads only own data fields of the envelope without invoking accessors', () => {
  let reads = 0;
  const accessor = {
    ...input,
    get payload() {
      reads++;
      return input.payload;
    },
  };
  for (const malformed of [accessor, Object.create(input), {...input, unexpected: true},
    {...input, payload: 1}, {...input, [Symbol('extra')]: true}]) {
    rejects('INVALID_PAYLOAD_ENVELOPE', () => binding.decode('InputValue', malformed));
  }
  assert.equal(reads, 0);
});
